"""Targets: the ways we can run a collection-scoped command against specific members.

direct  one target per data-bearing member, over a direct connection.
        Used for replica sets, standalones, and sharded clusters where direct
        shard access is allowed (pre-8.0 self-managed, or directShardOperations).
mongos  one target per targeting round (see targeting.py); each round reaches
        one member per shard through mongos via read preference tags.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Set

from pymongo import MongoClient
from pymongo.errors import OperationFailure
from pymongo.read_preferences import Nearest

from ..connection import Connector
from ..targeting import Plan, Round, candidates_from_topology, plan_rounds
from ..topology import SHARDED, Topology

DIRECT = "direct"
MONGOS = "mongos"


@dataclass
class Target:
    label: str
    client: MongoClient
    read_preference: Optional[Nearest] = None
    host: Optional[str] = None      # direct targets: the member
    shard: Optional[str] = None     # direct targets: its shard/replset
    round: Optional[Round] = None   # mongos targets: the plan round

    def db(self, name: str):
        if self.read_preference is not None:
            return self.client.get_database(name, read_preference=self.read_preference)
        return self.client[name]


def probe_direct_collection_access(conn: Connector, topo: Topology) -> Dict[str, object]:
    """Can we run collection-level commands on a shard member directly?"""
    member = next(m for m in topo.data_members if not m.is_config_server)
    app_dbs = [d for d in topo.database_names if d not in ("admin", "config", "local")]
    probe_db = app_dbs[0] if app_dbs else "test"
    try:
        conn.direct(member.host)[probe_db].command("listCollections", nameOnly=True)
        return {"allowed": True, "host": member.host, "db": probe_db}
    except OperationFailure as exc:
        if exc.code == 13 and "directly to a shard" in str(exc):
            return {"allowed": False, "host": member.host, "db": probe_db, "error": str(exc)}
        raise


def build_targets(conn: Connector, topo: Topology, mode: str) -> List[Target]:
    if mode == DIRECT:
        out = []
        for shard, members in topo.shards.items():
            for m in members:
                if m.data_bearing:
                    out.append(Target(label=m.host, client=conn.direct(m.host), host=m.host, shard=shard))
        return out
    assert topo.kind == SHARDED
    return [
        Target(label=f"round{i + 1} {r.label()}", client=conn.seed,
               read_preference=Nearest(tag_sets=r.read_preference_tags()), round=r)
        for i, r in enumerate(make_plan(topo).rounds)
    ]


def make_plan(topo: Topology) -> Plan:
    return plan_rounds(candidates_from_topology(topo.data_members))


class HostAliases:
    """Maps host names reported in results (e.g. $indexStats `host`, which uses
    the OS hostname) to the replica set config names we use everywhere else."""

    def __init__(self, members):
        self._map: Dict[str, str] = {m.host: m.host for m in members}

    def learn(self, member_host: str, server_status_host: Optional[str]) -> None:
        if not server_status_host:
            return
        self._map.setdefault(server_status_host, member_host)
        if ":" not in server_status_host:
            port = member_host.rsplit(":", 1)[-1]
            self._map.setdefault(f"{server_status_host}:{port}", member_host)

    def resolve(self, host: Optional[str]) -> Optional[str]:
        if host is None:
            return None
        return self._map.get(host, host)


class Coverage:
    """Tracks which members actually answered, per kind of data."""

    def __init__(self, topo: Topology):
        self.expected: Dict[str, Set[str]] = {
            shard: {m.host for m in ms if m.data_bearing} for shard, ms in topo.shards.items()
        }
        self.observed: Dict[str, Dict[str, Set[str]]] = {}
        # ns -> shards holding it / hosts that returned its $indexStats
        self.ns_shards: Dict[str, Set[str]] = {}
        self.ns_index_hosts: Dict[str, Set[str]] = {}
        # Shards holding at least one in-scope collection. Members of other shards
        # (e.g. --db limited to a database that lives on one shard) have nothing
        # to report, so they are not expected. None = every shard.
        self.relevant_shards: Optional[Set[str]] = None

    def _expected(self) -> Dict[str, Set[str]]:
        if self.relevant_shards is None:
            return self.expected
        return {s: h for s, h in self.expected.items() if s in self.relevant_shards}

    def saw(self, kind: str, shard: Optional[str], host: Optional[str]) -> None:
        if shard is None or host is None:
            return
        self.observed.setdefault(kind, {}).setdefault(shard, set()).add(host)

    def gaps(self, kind: str) -> List[Dict[str, str]]:
        seen = self.observed.get(kind, {})
        return [{"shard": s, "host": h} for s, hosts in sorted(self._expected().items())
                for h in sorted(hosts - seen.get(s, set()))]

    def index_gaps(self) -> List[Dict[str, object]]:
        out = []
        for ns, shards in sorted(self.ns_shards.items()):
            want = set().union(*(self.expected.get(s, set()) for s in shards))
            missing = sorted(want - self.ns_index_hosts.get(ns, set()))
            if missing:
                out.append({"ns": ns, "missingHosts": missing})
        return out

    def summary(self) -> Dict[str, object]:
        kinds = sorted(self.observed)
        expected = self._expected()
        return {
            "expectedMembers": {s: sorted(h) for s, h in sorted(expected.items())},
            "shardsWithoutInScopeCollections": sorted(set(self.expected) - set(expected)),
            "byKind": {
                k: {"covered": sum(len(self.observed[k].get(s, set()) & h) for s, h in expected.items()),
                    "expected": sum(len(v) for v in expected.values()),
                    "gaps": self.gaps(k)}
                for k in kinds
            },
            "indexStatsNamespaceGaps": self.index_gaps(),
        }
