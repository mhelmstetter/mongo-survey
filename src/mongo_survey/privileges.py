"""Privilege preflight.

Before collecting anything we ask the server which privileges the user holds
(connectionStatus with showPrivileges) and compare them with what the run
needs. A missing privilege stops the run with a list of what is missing and
the built-in roles that grant it.

Matching follows the server's resource rules:
  {anyResource: true}           matches everything
  {cluster: true}               matches cluster actions
  {db: "", collection: ""}      any "normal" collection in any db, but NOT the
                                config/local dbs and NOT system.* collections
  {db: "X", collection: ""}     any normal collection in db X
  {db: "", collection: "C"}     collection C in any db
  {db: "X", collection: "C"}    exactly X.C
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

CLUSTER = "<cluster>"

SUGGESTED_ROLES = (
    "clusterMonitor@admin (node/cluster stats, $indexStats, top, config metadata), "
    "readAnyDatabase@admin or read@<each app db> (listCollections), "
    "read@local (oplog window)"
)


@dataclass(frozen=True)
class Need:
    db: str          # CLUSTER for cluster actions
    collection: str  # "" for database-wide
    action: str
    why: str

    def describe(self) -> str:
        where = "cluster" if self.db == CLUSTER else (f"{self.db}.{self.collection}" if self.collection else f"db {self.db}")
        return f"{self.action} on {where} ({self.why})"


def _is_special_db(db: str) -> bool:
    return db in ("config", "local")


def resource_matches(resource: Dict[str, Any], db: str, coll: str) -> bool:
    if resource.get("anyResource"):
        return True
    if db == CLUSTER:
        return bool(resource.get("cluster"))
    if resource.get("cluster"):
        return False
    rdb = resource.get("db")
    rcoll = resource.get("collection")
    if rdb is None or rcoll is None:
        return False
    db_ok = rdb == db or (rdb == "" and not _is_special_db(db))
    if rcoll == "":
        coll_ok = not coll.startswith("system.")
    else:
        coll_ok = rcoll == coll
    # A database-wide need ("" collection) is only met by a database-wide grant.
    if coll == "" and rcoll != "":
        return False
    return db_ok and coll_ok


def has(privileges: Sequence[Dict[str, Any]], need: Need) -> bool:
    for p in privileges:
        if need.action in p.get("actions", []) and resource_matches(p.get("resource", {}), need.db, need.collection):
            return True
    return False


def required(kind: str, databases: Iterable[str], include_auth: bool, query_stats: bool = False,
             query_stats_hmac: bool = False) -> List[Need]:
    needs = [
        Need(CLUSTER, "", "serverStatus", "per-node server stats"),
        Need(CLUSTER, "", "hostInfo", "per-node host info"),
        Need(CLUSTER, "", "getCmdLineOpts", "per-node startup options"),
        Need(CLUSTER, "", "getParameter", "per-node server parameters"),
        Need(CLUSTER, "", "getLog", "startup warnings"),
        Need(CLUSTER, "", "top", "per-collection op counters"),
        Need(CLUSTER, "", "listDatabases", "database list"),
        Need(CLUSTER, "", "allCollectionStats", "$_internalAllCollectionStats"),
    ]
    if kind != "standalone":
        needs += [
            Need(CLUSTER, "", "replSetGetConfig", "member list and tags"),
            Need(CLUSTER, "", "replSetGetStatus", "member states"),
            Need("local", "oplog.rs", "find", "oplog window"),
        ]
    if kind == "sharded":
        needs.append(Need(CLUSTER, "", "getShardMap", "config server hosts"))
        for c in ("shards", "databases", "collections", "chunks", "tags", "settings", "mongos"):
            needs.append(Need("config", c, "find", "sharding metadata"))
    for db in databases:
        for action, why in (("listCollections", "collection options"), ("dbStats", "database stats"),
                            ("collStats", "collection stats"), ("indexStats", "index usage")):
            needs.append(Need(db, "", action, why))
    if query_stats:
        needs.append(Need(CLUSTER, "", "queryStatsReadTransformed" if query_stats_hmac else "queryStatsRead",
                          "$queryStats (skip with --no-query-stats)"))
    if include_auth:
        needs += [Need("admin", "", "viewUser", "--include-auth"), Need("admin", "", "viewRole", "--include-auth")]
    return needs


def missing(privileges: Sequence[Dict[str, Any]], needs: Sequence[Need]) -> List[Need]:
    return [n for n in needs if not has(privileges, n)]


def fetch(client) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    status = client.admin.command("connectionStatus", showPrivileges=True)
    info = status["authInfo"]
    return info.get("authenticatedUserPrivileges", []), {
        "users": info.get("authenticatedUsers", []),
        "roles": info.get("authenticatedUserRoles", []),
    }


def format_missing(miss: Sequence[Need], limit: Optional[int] = 25) -> str:
    lines = [f"  - {n.describe()}" for n in miss[:limit]]
    if limit and len(miss) > limit:
        lines.append(f"  ... and {len(miss) - limit} more")
    return "\n".join(lines) + f"\nSuggested roles: {SUGGESTED_ROLES}"
