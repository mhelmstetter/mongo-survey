"""Node-level commands, run over a direct connection to each member.

These commands are allowed on direct shard connections even on Atlas 8.0+.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple

from pymongo import DESCENDING, ASCENDING

from ..output import CaptureWriter
from ..topology import Member

SECTION = "node"

NODE_COMMANDS: List[Tuple[str, Dict[str, Any]]] = [
    ("hello", {"hello": 1}),
    ("buildInfo", {"buildInfo": 1}),
    ("serverStatus", {"serverStatus": 1}),
    ("hostInfo", {"hostInfo": 1}),
    ("getCmdLineOpts", {"getCmdLineOpts": 1}),
    ("getParameter", {"getParameter": "*"}),
    ("startupWarnings", {"getLog": "startupWarnings"}),
    ("top", {"top": 1}),
]
REPLSET_COMMANDS: List[Tuple[str, Dict[str, Any]]] = [
    ("replSetGetStatus", {"replSetGetStatus": 1}),
]

OPLOG_STAT_FIELDS = ("ns", "size", "count", "storageSize", "maxSize", "capped")


def oplog_window(client) -> Dict[str, Any]:
    oplog = client.local["oplog.rs"]
    proj = {"ts": 1, "wall": 1, "_id": 0}
    first = oplog.find_one({}, proj, sort=[("$natural", ASCENDING)])
    last = oplog.find_one({}, proj, sort=[("$natural", DESCENDING)])
    stats = client.local.command("collStats", "oplog.rs")
    out = {k: stats.get(k) for k in OPLOG_STAT_FIELDS}
    out.update({"first": first, "last": last})
    if first and last:
        out["windowSeconds"] = last["ts"].time - first["ts"].time
    return out


def collect_member(w: CaptureWriter, conn, member: Member, is_replset: bool) -> Optional[str]:
    """Run every node command on one member. Returns serverStatus.host (for aliasing)."""
    client = conn.direct(member.host)
    meta = {"host": member.host, "shard": member.shard or member.replset}
    status_host = None
    for name, cmd in NODE_COMMANDS + (REPLSET_COMMANDS if is_replset else []):
        out = w.run(SECTION, name, lambda c=cmd: client.admin.command(c), command=cmd, **meta)
        if name == "serverStatus" and out:
            status_host = out.get("host")
    if is_replset:
        w.run(SECTION, "oplogWindow", lambda: oplog_window(client),
              command={"find": "local.oplog.rs", "first/last": "ts"}, **meta)
    return status_host


ROUTER_COMMANDS: List[Tuple[str, Dict[str, Any]]] = [
    ("hello", {"hello": 1}),
    ("buildInfo", {"buildInfo": 1}),
    ("serverStatus", {"serverStatus": 1}),
    ("getCmdLineOpts", {"getCmdLineOpts": 1}),
    ("getParameter", {"getParameter": "*"}),
]


def collect_router(w: CaptureWriter, conn, host: str) -> None:
    """One mongos from the connection string, over a direct connection."""
    admin = conn.direct(host).admin
    for name, cmd in ROUTER_COMMANDS:
        w.run("router", name, lambda c=cmd: admin.command(c), command=cmd, host=host)
