"""Command line entry point."""
from __future__ import annotations

import argparse
import logging
import os
import sys

from . import __version__

DESCRIPTION = """\
Read-only MongoDB health-data collector.

  check    preflight only: topology, Atlas detection, version, privileges,
           namespace count, per-member targeting plan. Writes nothing.
  collect  full capture: node stats for every member, catalog, sharding
           metadata, collection/index stats and $indexStats from EVERY member,
           then cache/heat sampling (default 60s; --cache-duration 0 to skip).
  cache    cache/heat sampling only (plus start/end $indexStats).
  report   summarize one or more capture files as Markdown.
"""


def _add_connection_args(p: argparse.ArgumentParser) -> None:
    g = p.add_argument_group("connection")
    g.add_argument("--uri", help="MongoDB connection string (prefer --uri-file or MONGODB_URI so the "
                                 "password stays out of shell history)")
    g.add_argument("--uri-file", help="file containing the connection string")
    g.add_argument("--timeout-ms", type=int, default=15000, help="server selection / connect timeout")


def _add_scope_args(p: argparse.ArgumentParser) -> None:
    g = p.add_argument_group("scope")
    g.add_argument("--db", action="append", default=[], metavar="NAME",
                   help="only these databases (repeatable)")
    g.add_argument("--exclude-db", action="append", default=[], metavar="NAME",
                   help="skip these databases (repeatable; admin, config, local and "
                        "__mdb_internal_atlas are always skipped unless named with --db)")
    g.add_argument("--max-namespaces", type=int, default=20000,
                   help="refuse to run above this many collections (default 20000)")
    g.add_argument("--allow-many-namespaces", action="store_true",
                   help="run even above --max-namespaces")
    g.add_argument("--workers", type=int, default=4, help="concurrent commands (default 4)")
    g.add_argument("--batch-size", type=int, default=500,
                   help="namespaces per $_internalAllCollectionStats call (default 500)")
    g.add_argument("--shard-access", choices=["auto", "direct", "mongos"], default="auto",
                   help="sharded clusters: how to reach each member for collection-level stats "
                        "(auto probes whether direct shard access is allowed)")


def _add_output_args(p: argparse.ArgumentParser) -> None:
    g = p.add_argument_group("output")
    g.add_argument("--out", default=".", help="output directory (default: current directory)")
    g.add_argument("--no-compress", action="store_true", help="write plain .jsonl instead of .jsonl.gz")
    g.add_argument("--include-auth", action="store_true",
                   help="also capture usersInfo/rolesInfo (needs viewUser/viewRole)")


def _add_cache_args(p: argparse.ArgumentParser, default_duration: float) -> None:
    g = p.add_argument_group("cache sampling")
    g.add_argument("--cache-duration", type=float, default=default_duration,
                   help=f"seconds to sample (default {default_duration:g}; 0 = one snapshot)")
    g.add_argument("--cache-interval", type=float, default=15,
                   help="seconds between samples (default 15)")
    g.add_argument("--cache-light", action="store_true",
                   help="sample op counts only (latencyStats/count), no storageStats: no "
                        "WiredTiger data handles are opened; no cache residency/churn")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="mongo-survey", description=DESCRIPTION,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--version", action="version", version=f"mongo-survey {__version__}")
    parser.add_argument("-v", "--verbose", action="store_true")
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("check", help="preflight only")
    _add_connection_args(p)
    _add_scope_args(p)
    p.add_argument("--include-auth", action="store_true")
    p.add_argument("--no-query-stats", action="store_true")
    p.add_argument("--query-stats-hmac", action="store_true")

    p = sub.add_parser("collect", help="full capture")
    _add_connection_args(p)
    _add_scope_args(p)
    _add_output_args(p)
    _add_cache_args(p, default_duration=60)

    g = p.add_argument_group("query stats")
    g.add_argument("--no-query-stats", action="store_true",
                   help="skip $queryStats (per-query-shape workload statistics)")
    g.add_argument("--query-stats-hmac", action="store_true",
                   help="have the server HMAC-hash field/collection names in $queryStats output "
                        "(random per-run key, never stored)")

    p = sub.add_parser("cache", help="cache/heat sampling only")
    _add_connection_args(p)
    _add_scope_args(p)
    _add_output_args(p)
    _add_cache_args(p, default_duration=60)

    p = sub.add_parser("report", help="Markdown summary of capture file(s)")
    p.add_argument("files", nargs="+")
    p.add_argument("--top", type=int, default=20, help="rows per ranking table (default 20)")
    p.add_argument("--full-lists", action="store_true",
                   help="list every unused / secondary-only index (default: largest 5 x --top)")
    return parser


def _keep_awake() -> None:
    """macOS: stop idle sleep while this process runs (a sleeping laptop stalls
    sampling and resets connections). `caffeinate -i -w PID` exits with us."""
    import shutil
    import subprocess

    if sys.platform != "darwin" or not shutil.which("caffeinate"):
        return
    subprocess.Popen(["caffeinate", "-i", "-w", str(os.getpid())],
                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    logging.getLogger("mongo-survey").info("holding a caffeinate -i assertion for this run (macOS)")


def main(argv=None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(level=logging.DEBUG if args.verbose else logging.INFO,
                        format="%(asctime)s %(levelname)-7s %(message)s", datefmt="%H:%M:%S",
                        stream=sys.stderr)
    if not args.verbose:
        logging.getLogger("pymongo").setLevel(logging.WARNING)

    if args.command == "report":
        from .report import render
        sys.stdout.write(render(args.files, top=args.top, full_lists=args.full_lists))
        return 0

    from .connection import Connector, read_uri
    from .survey import Options, run

    if args.cache_interval <= 0 if hasattr(args, "cache_interval") else False:
        raise SystemExit("error: --cache-interval must be > 0")
    opts = Options(
        command=args.command,
        out_dir=getattr(args, "out", "."),
        compress=not getattr(args, "no_compress", False),
        include_dbs=args.db,
        exclude_dbs=args.exclude_db,
        max_namespaces=args.max_namespaces,
        allow_many_namespaces=args.allow_many_namespaces,
        workers=args.workers,
        cache_duration=getattr(args, "cache_duration", 0),
        cache_interval=getattr(args, "cache_interval", 15),
        cache_light=getattr(args, "cache_light", False),
        shard_access=args.shard_access,
        include_auth=args.include_auth,
        batch_size=args.batch_size,
        query_stats=not getattr(args, "no_query_stats", True),
        query_stats_hmac=getattr(args, "query_stats_hmac", False),
    )
    if opts.command != "check":
        os.makedirs(opts.out_dir, exist_ok=True)
    from pymongo.errors import ConnectionFailure, PyMongoError

    _keep_awake()
    conn = Connector(read_uri(args.uri, args.uri_file), timeout_ms=args.timeout_ms)
    try:
        return run(conn, opts)
    except ConnectionFailure as exc:
        if args.verbose:
            raise
        msg = str(exc).split(", Timeout:")[0]
        raise SystemExit(f"error: cannot connect: {msg}")
    except PyMongoError as exc:
        if args.verbose:
            raise
        raise SystemExit(f"error: {type(exc).__name__}: {str(exc)[:500]} (rerun with -v for a traceback)")
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
