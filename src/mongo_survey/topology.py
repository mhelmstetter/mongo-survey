"""Topology discovery: what are we connected to, and which members exist.

Member lists and tags come from replSetGetConfig on a direct connection to one
member of each replica set. That command is allowed on Atlas shard members even
on 8.0+ where collection-level commands are not.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional, Tuple

from concurrent.futures import ThreadPoolExecutor

from pymongo.errors import PyMongoError

from . import MIN_SERVER_VERSION
from .connection import Connector
from .output import describe_error

STANDALONE = "standalone"
REPLSET = "replset"
SHARDED = "sharded"


@dataclass
class Member:
    host: str
    replset: str
    shard: Optional[str]           # shard id, None for a plain replica set / CSRS
    tags: Dict[str, str] = field(default_factory=dict)
    hidden: bool = False
    arbiter: bool = False
    priority: float = 1.0
    votes: int = 1
    secondary_delay_secs: int = 0
    state: Optional[str] = None    # from replSetGetStatus when available
    is_config_server: bool = False

    @property
    def data_bearing(self) -> bool:
        return not self.arbiter

    def to_doc(self) -> Dict[str, Any]:
        return asdict(self)


@dataclass
class Topology:
    kind: str
    seed_hello: Dict[str, Any]
    version: str
    shards: Dict[str, List[Member]] = field(default_factory=dict)  # shard/replset name -> members
    config_servers: List[Member] = field(default_factory=list)
    config_shard: bool = False     # 8.0 "config shard": CSRS is also a data shard
    replset_name: Optional[str] = None
    database_names: List[str] = field(default_factory=list)

    @property
    def data_members(self) -> List[Member]:
        return [m for ms in self.shards.values() for m in ms if m.data_bearing]

    @property
    def all_members(self) -> List[Member]:
        seen = {}
        for m in [*(m for ms in self.shards.values() for m in ms), *self.config_servers]:
            seen.setdefault(m.host, m)
        return list(seen.values())

    def to_doc(self) -> Dict[str, Any]:
        return {
            "kind": self.kind,
            "version": self.version,
            "replsetName": self.replset_name,
            "configShard": self.config_shard,
            "shards": {k: [m.to_doc() for m in v] for k, v in self.shards.items()},
            "configServers": [m.to_doc() for m in self.config_servers],
            "databases": self.database_names,
        }


def parse_version(v: str) -> Tuple[int, ...]:
    parts = []
    for p in v.split("-")[0].split("."):
        if not p.isdigit():
            break
        parts.append(int(p))
    return tuple(parts)


def check_version(version: str, where: str) -> None:
    if parse_version(version)[:2] < MIN_SERVER_VERSION:
        need = ".".join(map(str, MIN_SERVER_VERSION))
        raise SystemExit(f"error: {where} runs MongoDB {version}; mongo-survey requires {need} or newer")


def _members_from_config(cfg: Dict[str, Any], shard: Optional[str], is_config: bool) -> List[Member]:
    out = []
    for m in cfg["members"]:
        out.append(
            Member(
                host=m["host"],
                replset=cfg["_id"],
                shard=shard,
                tags=dict(m.get("tags") or {}),
                hidden=bool(m.get("hidden", False)),
                arbiter=bool(m.get("arbiterOnly", False)),
                priority=float(m.get("priority", 1)),
                votes=int(m.get("votes", 1)),
                secondary_delay_secs=int(m.get("secondaryDelaySecs", m.get("slaveDelay", 0)) or 0),
                is_config_server=is_config,
            )
        )
    return out


def _apply_states(members: List[Member], status: Dict[str, Any]) -> None:
    by_host = {m["name"]: m.get("stateStr") for m in status.get("members", [])}
    for m in members:
        m.state = by_host.get(m.host)


def _replset_members(conn: Connector, hosts: List[str], shard: Optional[str], is_config: bool) -> Tuple[List[Member], Dict[str, Any], Dict[str, Any]]:
    """Ask the first reachable host for replSetGetConfig + replSetGetStatus."""
    errors = []
    for h in hosts:
        try:
            admin = conn.direct(h).admin
            cfg = admin.command("replSetGetConfig")["config"]
            status = admin.command("replSetGetStatus")
        except PyMongoError as exc:
            errors.append(f"{h}: {exc}")
            continue
        members = _members_from_config(cfg, shard, is_config)
        _apply_states(members, status)
        return members, cfg, status
    raise SystemExit(
        "error: could not read replSetGetConfig from any member of "
        f"{shard or 'replica set'} via a direct connection:\n  " + "\n  ".join(errors)
        + "\nmongo-survey needs direct network access to every member for node-level commands."
    )


def _split_shard_host(host_field: str) -> Tuple[str, List[str]]:
    rs, _, hosts = host_field.partition("/")
    return rs, hosts.split(",") if hosts else []


def identify(conn: Connector) -> Tuple[str, Dict[str, Any], Dict[str, Any], List[str]]:
    """Cheap first look, safe before the privilege check: kind, hello, buildInfo, db names.

    hello and buildInfo need no privileges. listDatabases needs the
    listDatabases action; without it the server lists only databases the user
    has privileges on, which is what the privilege check then evaluates.
    """
    admin = conn.seed.admin
    hello = admin.command("hello")
    build = admin.command("buildInfo")
    check_version(build["version"], "the seed connection")
    if hello.get("msg") == "isdbgrid":
        kind = SHARDED
    elif hello.get("setName"):
        kind = REPLSET
    else:
        kind = STANDALONE
    dbs = admin.command("listDatabases", nameOnly=True, authorizedDatabases=True)["databases"]
    return kind, hello, build, [d["name"] for d in dbs]


def discover(conn: Connector, recorder, kind: str, hello: Dict[str, Any], build: Dict[str, Any],
             db_names: List[str]) -> Topology:
    admin = conn.seed.admin
    version = build["version"]
    recorder.record("topology", "seed_hello", hello)
    recorder.record("topology", "seed_buildInfo", build)

    if kind == SHARDED:
        topo = Topology(kind=SHARDED, seed_hello=hello, version=version, database_names=db_names)
        shard_docs = list(conn.seed.config.shards.find().sort("_id", 1))
        recorder.record("topology", "config.shards", shard_docs)
        with ThreadPoolExecutor(max_workers=8) as pool:
            found = list(pool.map(
                lambda sd: _replset_members(conn, _split_shard_host(sd["host"])[1], sd["_id"],
                                            is_config=(sd["_id"] == "config")),
                shard_docs))
        for sd, (members, cfg, _status) in zip(shard_docs, found):
            recorder.record("topology", "replSetGetConfig", cfg, shard=sd["_id"])
            topo.shards[sd["_id"]] = members
            if sd["_id"] == "config":
                topo.config_shard = True
        shard_map = admin.command("getShardMap")
        recorder.record("topology", "getShardMap", shard_map)
        cfg_conn = shard_map["map"]["config"]
        _, cfg_hosts = _split_shard_host(cfg_conn)
        if topo.config_shard:
            topo.config_servers = topo.shards["config"]
            for m in topo.config_servers:
                m.is_config_server = True
        else:
            members, cfg, _ = _replset_members(conn, cfg_hosts, None, is_config=True)
            recorder.record("topology", "replSetGetConfig", cfg, shard="config")
            topo.config_servers = members
    elif kind == REPLSET:
        topo = Topology(kind=REPLSET, seed_hello=hello, version=version,
                        replset_name=hello["setName"], database_names=db_names)
        cfg = admin.command("replSetGetConfig")["config"]
        status = admin.command("replSetGetStatus")
        recorder.record("topology", "replSetGetConfig", cfg)
        members = _members_from_config(cfg, None, False)
        _apply_states(members, status)
        topo.shards[hello["setName"]] = members
    else:
        topo = Topology(kind=STANDALONE, seed_hello=hello, version=version, database_names=db_names)
        me = hello.get("me") or "{}:{}".format(*conn.seed.address)
        topo.shards["standalone"] = [Member(host=me, replset="", shard=None)]

    # Every member must meet the version floor, not just the router.
    def member_version(m: Member):
        try:
            return m, conn.direct(m.host).admin.command("buildInfo")["version"], None
        except PyMongoError as exc:
            return m, None, describe_error(exc)

    with ThreadPoolExecutor(max_workers=8) as pool:
        results = list(pool.map(member_version, [m for m in topo.all_members if not m.arbiter]))
    for m, v, err in results:
        if err is not None:
            recorder.record("topology", "member_buildInfo", None, host=m.host, error=err)
            continue
        check_version(v, m.host)
    return topo
