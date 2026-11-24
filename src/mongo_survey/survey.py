"""Orchestration for `check`, `collect` and `cache`."""
from __future__ import annotations

import logging
import os
import platform
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Set

from pymongo.errors import PyMongoError

from . import SCHEMA_VERSION, __version__, atlas, privileges
from .collectors import metadata, node, querystats, stats
from .collectors.targets import (DIRECT, MONGOS, Coverage, HostAliases, build_targets,
                                 make_plan, probe_direct_collection_access)
from .connection import Connector
from .output import CaptureWriter, utcnow
from .topology import SHARDED, STANDALONE, Topology, discover, identify

log = logging.getLogger("mongo-survey")

DEFAULT_EXCLUDED_DBS = ("admin", "config", "local", "__mdb_internal_atlas")

EXIT_OK = 0
EXIT_FATAL = 1
EXIT_INCOMPLETE = 2


@dataclass
class Options:
    command: str                     # check | collect | cache
    out_dir: str
    compress: bool
    include_dbs: List[str]
    exclude_dbs: List[str]
    max_namespaces: int
    allow_many_namespaces: bool
    workers: int
    cache_duration: float
    cache_interval: float
    cache_light: bool
    shard_access: str                # auto | direct | mongos
    include_auth: bool
    batch_size: int = 500            # namespaces per $_internalAllCollectionStats call
    query_stats: bool = True         # collect $queryStats (collect command only)
    query_stats_hmac: bool = False   # hash identifiers in $queryStats output


class _NullWriter:
    """Writer used by `check`: keeps errors, discards records."""

    def __init__(self):
        self.errors: List[Dict[str, Any]] = []

    def record(self, section, subsection, output=None, *, error=None, **kw):
        if error is not None:
            self.errors.append({"section": section, "subsection": subsection, "error": error, **kw})

    def run(self, section, subsection, fn, **meta):
        try:
            return fn()
        except PyMongoError as exc:
            self.record(section, subsection, error={"message": str(exc)}, **meta)
            return None


def select_databases(all_dbs: List[str], include: List[str], exclude: List[str]) -> List[str]:
    if include:
        unknown = sorted(set(include) - set(all_dbs))
        if unknown:
            raise SystemExit(f"error: --db names not found on the cluster: {', '.join(unknown)}")
        return [d for d in all_dbs if d in include]
    skip = set(DEFAULT_EXCLUDED_DBS) | set(exclude)
    return [d for d in all_dbs if d not in skip]


def _safe_name(s: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]+", "_", s).strip("_") or "cluster"


def _check_privileges(conn: Connector, kind: str, dbs: List[str], include_auth: bool,
                      query_stats: bool, query_stats_hmac: bool) -> Dict[str, Any]:
    privs, who = privileges.fetch(conn.seed)
    if not who["users"]:
        # No authenticated user: either auth is disabled or the login failed. Prove
        # which by running a command that requires auth when auth is on.
        try:
            conn.seed.admin.command("listDatabases", nameOnly=True)
        except PyMongoError as exc:
            raise SystemExit(f"error: not authenticated and listDatabases failed: {exc}")
        log.info("no authenticated user and listDatabases works: authorization is disabled")
        return {"authDisabled": True}
    needs = privileges.required(kind, dbs, include_auth, query_stats=query_stats,
                                query_stats_hmac=query_stats_hmac)
    miss = privileges.missing(privs, needs)
    if miss:
        raise SystemExit("error: the connected user lacks privileges this run needs:\n"
                         + privileges.format_missing(miss))
    return {"authDisabled": False, **who}


def _resolve_access(conn: Connector, topo: Topology, requested: str) -> Dict[str, Any]:
    if topo.kind != SHARDED:
        return {"mode": DIRECT, "reason": f"{topo.kind}: direct member connections"}
    if requested == DIRECT:
        return {"mode": DIRECT, "reason": "--shard-access direct"}
    if requested == MONGOS:
        return {"mode": MONGOS, "reason": "--shard-access mongos"}
    probe = probe_direct_collection_access(conn, topo)
    if probe["allowed"]:
        return {"mode": DIRECT, "reason": "direct shard collection access allowed", "probe": probe}
    return {"mode": MONGOS, "reason": "direct shard collection access refused; using mongos tag targeting",
            "probe": probe}


def _plan_doc(topo: Topology) -> Dict[str, Any]:
    plan = make_plan(topo)
    return {
        "rounds": [{"label": r.label(), "readPreferenceTags": r.read_preference_tags(),
                    "expected": r.expected} for r in plan.rounds],
        "plannedHosts": sorted(plan.expected_hosts),
        "untargetable": [{"shard": c.shard, "host": c.host, "tags": dict(c.tags)} for c in plan.untargetable],
    }


def _one_per_shard(targets):
    """Enough targets to see every shard once: round 1 through mongos reaches every
    shard; in direct mode take the first member of each shard."""
    if targets and targets[0].host is None:
        return targets[:1]
    seen, out = set(), []
    for t in targets:
        if t.shard not in seen:
            seen.add(t.shard)
            out.append(t)
    return out


def _inventory(recorder, targets, topo: Topology, aliases: HostAliases, member_shard: Dict[str, str],
               dbs: List[str], workers: int, locations: Dict[str, Set[str]]) -> List[str]:
    """Every in-scope namespace, from one unfiltered light pass per shard
    (latencyStats/count only: no storageStats, no data handles opened).
    Fills `locations` with ns -> shards where the collection exists."""
    seen = stats.run_collection_stats(recorder, _one_per_shard(targets), stats.LIGHT, Coverage(topo),
                                      aliases, member_shard, workers, sample=-1, databases=set(dbs),
                                      locations=locations)
    return sorted(seen)


def run(conn: Connector, opts: Options) -> int:
    started = utcnow()
    writer: Any
    if opts.command == "check":
        writer = _NullWriter()
    else:
        writer = None  # opened once the cluster name is known

    probe_writer = _NullWriter()
    kind, hello, build, db_names = identify(conn)
    dbs = select_databases(db_names, opts.include_dbs, opts.exclude_dbs)
    log.info("%s, MongoDB %s: checking privileges", kind, build["version"])
    auth = _check_privileges(conn, kind, dbs, opts.include_auth,
                             query_stats=opts.query_stats and opts.command in ("check", "collect"),
                             query_stats_hmac=opts.query_stats_hmac)
    log.info("discovering members")
    topo = discover(conn, probe_writer, kind, hello, build, db_names)
    seed_hosts = [f"{h}:{p}" for h, p in conn.seed.topology_description.server_descriptions()]
    atlas_info = atlas.detect(topo, seed_hosts)
    access = _resolve_access(conn, topo, opts.shard_access)
    plan = _plan_doc(topo) if access["mode"] == MONGOS else None
    targets = build_targets(conn, topo, access["mode"])
    aliases = HostAliases(topo.all_members)
    member_shard = {m.host: (m.shard or m.replset) for m in topo.all_members}
    log.info("namespace inventory")
    locations: Dict[str, Set[str]] = {}
    all_ns = _inventory(probe_writer, targets, topo, aliases, member_shard, dbs, opts.workers, locations)
    ns_count = len(all_ns)

    summary = {
        "topology": topo.kind,
        "version": topo.version,
        "atlas": atlas_info,
        "access": access,
        "databases": len(dbs),
        "namespaces": ns_count,
        "members": len([m for m in topo.all_members if m.data_bearing]),
        "targets": [t.label for t in targets],
        "auth": {"authDisabled": auth.get("authDisabled"), "roles": auth.get("roles")},
    }
    _print_overview(topo, summary, plan)
    if probe_writer.errors:
        for e in probe_writer.errors:
            log.error("topology error: %s", e)

    if ns_count > opts.max_namespaces and not opts.allow_many_namespaces:
        log.error("%d namespaces exceeds --max-namespaces %d. Collecting storage stats opens a "
                  "WiredTiger data handle per collection and index on every member, which can "
                  "evict useful handles and stall the node. Narrow with --db/--exclude-db, use "
                  "`cache --cache-light`, or pass --allow-many-namespaces.", ns_count, opts.max_namespaces)
        return EXIT_FATAL
    if plan and plan["untargetable"]:
        log.warning("%d member(s) cannot be targeted through mongos and will be coverage gaps: %s",
                    len(plan["untargetable"]), ", ".join(u["host"] for u in plan["untargetable"]))

    if opts.command == "check":
        log.info("check passed%s", " (with untargetable members)" if plan and plan["untargetable"] else "")
        return EXIT_INCOMPLETE if (probe_writer.errors or (plan and plan["untargetable"])) else EXIT_OK

    name = conn.display_name if topo.kind == SHARDED else (topo.replset_name or conn.display_name)
    stamp = started.strftime("%Y%m%dT%H%M%SZ")
    path = os.path.join(opts.out_dir, f"mongo-survey-{_safe_name(name)}-{stamp}.jsonl"
                        + (".gz" if opts.compress else ""))
    writer = CaptureWriter(path, opts.compress)
    log.info("writing %s", path)
    writer.write({
        "type": "header",
        "tool": "mongo-survey",
        "toolVersion": __version__,
        "schemaVersion": SCHEMA_VERSION,
        "command": opts.command,
        "startedAt": started,
        "uri": conn.redacted_uri,
        "options": {k: v for k, v in vars(opts).items()},
        "client": {"python": platform.python_version(), "platform": platform.platform(),
                   "argv": [a if "mongodb" not in a else conn.redacted_uri for a in sys.argv[1:]]},
        "summary": summary,
        "topology": topo.to_doc(),
        "targetingPlan": plan,
        "seedHosts": seed_hosts,
        "databases": dbs,
    })
    for e in probe_writer.errors:
        writer.record("topology", e.get("subsection", "error"), None, error=e.get("error"), host=e.get("host"))

    coverage = Coverage(topo)
    coverage.relevant_shards = {sh for shards in locations.values() for sh in shards}
    writer.record("inventory", "namespaces", [{"ns": ns, "shards": sorted(locations.get(ns, ()))}
                                              for ns in all_ns])
    try:
        if opts.command == "collect":
            _collect_static(writer, conn, topo, dbs, opts, aliases, seed_hosts)
        else:
            # cache mode skips the node snapshot, but host aliasing needs each
            # member's OS hostname ($indexStats reports it instead of the config name)
            for m in topo.all_members:
                if m.data_bearing:
                    out = writer.run("node", "hostInfo", lambda h=m.host: conn.direct(h).admin.command("hostInfo"),
                                     host=m.host, command={"hostInfo": 1})
                    if out:
                        aliases.learn(m.host, out["system"]["hostname"])

        mode = stats.LIGHT if opts.cache_light else stats.FULL
        log.info("index usage snapshot (start): %d target(s)", len(targets))
        ns_list = stats.index_stats_namespaces(set(all_ns))
        t_idx = time.monotonic()
        stats.run_index_stats(writer, targets, ns_list, coverage, aliases, member_shard, opts.workers, "start",
                              locations=locations)
        log.info("  $indexStats: %d namespace(s) x %d target(s) in %.1fs", len(ns_list), len(targets),
                 time.monotonic() - t_idx)
        log.info("collection/cache sampling: mode=%s duration=%ss interval=%ss",
                 mode, opts.cache_duration, opts.cache_interval)
        stats.run_sampling(writer, targets, set(dbs), all_ns, mode, opts.cache_duration,
                           opts.cache_interval, coverage, aliases, member_shard, opts.workers, opts.batch_size)
        if opts.cache_duration > 0:
            log.info("index usage snapshot (end)")
            stats.run_index_stats(writer, targets, ns_list, coverage, aliases, member_shard,
                                  opts.workers, "end", locations=locations)
    finally:
        cov = coverage.summary()
        writer.write({
            "type": "footer",
            "finishedAt": utcnow(),
            "records": writer.records,
            "coverage": cov,
            "errors": writer.errors,
        })
        writer.close()

    return _report_outcome(writer, cov, path)


def _collect_static(writer, conn, topo: Topology, dbs: List[str], opts: Options, aliases: HostAliases,
                    seed_hosts: List[str]) -> None:
    is_rs = topo.kind != STANDALONE
    members = [m for m in topo.all_members if m.data_bearing]
    log.info("node-level commands on %d member(s)", len(members))
    with ThreadPoolExecutor(max_workers=opts.workers) as pool:
        hosts = list(pool.map(lambda m: (m, node.collect_member(writer, conn, m, is_rs)), members))
    for m, status_host in hosts:
        aliases.learn(m.host, status_host)
    if topo.kind == SHARDED:
        log.info("router commands on %d mongos", len(seed_hosts))
        with ThreadPoolExecutor(max_workers=opts.workers) as pool:
            list(pool.map(lambda h: node.collect_router(writer, conn, h), seed_hosts))
        log.info("sharding metadata")
        metadata.collect_sharding(writer, conn)
    metadata.collect_cluster_settings(writer, conn)
    log.info("catalog for %d database(s)", len(dbs))
    metadata.collect_catalog(writer, conn, dbs)
    if opts.include_auth:
        metadata.collect_auth(writer, conn)
    if opts.query_stats:
        if topo.kind == SHARDED:
            hosts, role = seed_hosts, "mongos"
        else:
            hosts, role = [m.host for m in members], "mongod"
        log.info("$queryStats from %d %s%s", len(hosts), role,
                 " (identifiers HMAC-hashed)" if opts.query_stats_hmac else "")
        t = time.monotonic()
        querystats.collect(writer, conn, hosts, querystats.new_hmac_key() if opts.query_stats_hmac else None,
                           opts.workers, role)
        log.info("  $queryStats in %.1fs", time.monotonic() - t)


def _report_outcome(writer: CaptureWriter, cov: Dict[str, Any], path: str) -> int:
    incomplete = False
    for kind, k in cov["byKind"].items():
        msg = f"coverage {kind}: {k['covered']}/{k['expected']} members"
        if k["gaps"]:
            incomplete = True
            log.warning(msg + " — missing: " + ", ".join(g["host"] for g in k["gaps"]))
        else:
            log.info(msg)
    if cov["indexStatsNamespaceGaps"]:
        incomplete = True
        log.warning("$indexStats incomplete for %d namespace(s) (see footer.coverage)",
                    len(cov["indexStatsNamespaceGaps"]))
    if writer.errors:
        incomplete = True
        log.warning("%d command error(s) recorded (see footer.errors). First: %s",
                    len(writer.errors), writer.errors[0]["error"])
    log.info("wrote %s (%d records, %.1f MB)", path, writer.records, os.path.getsize(path) / 1e6)
    return EXIT_INCOMPLETE if incomplete else EXIT_OK


def _print_overview(topo: Topology, summary: Dict[str, Any], plan: Optional[Dict[str, Any]]) -> None:
    log.info("topology: %s, MongoDB %s, %d data-bearing member(s), %d database(s), %d namespace(s)",
             topo.kind, topo.version, summary["members"], summary["databases"], summary["namespaces"])
    a = summary["atlas"]
    log.info("atlas: %s%s", "yes" if a["isAtlas"] else "no",
             (" — " + "; ".join(a["evidence"])) if a["evidence"] else "")
    log.info("collection-level access: %s (%s)", summary["access"]["mode"], summary["access"]["reason"])
    if plan:
        for r in plan["rounds"]:
            log.info("  %s -> %s", r["label"], ", ".join(f"{s}:{h or '?'}" for s, h in r["expected"].items()))
        log.info("  planned coverage: %d member(s)", len(plan["plannedHosts"]))
