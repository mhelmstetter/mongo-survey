"""Cluster-wide metadata, read once through the seed connection (mongos or replica set)."""
from __future__ import annotations

from typing import List

from ..output import CaptureWriter


def collect_catalog(w: CaptureWriter, conn, databases: List[str]) -> None:
    client = conn.seed
    w.run("catalog", "listDatabases", lambda: client.admin.command("listDatabases"),
          command={"listDatabases": 1})
    for db in databases:
        w.run("catalog", "dbStats", lambda d=db: client[d].command("dbStats"),
              command={"dbStats": 1}, db=db)
        w.run("catalog", "listCollections", lambda d=db: list(client[d].list_collections()),
              command={"listCollections": 1}, db=db)


def collect_sharding(w: CaptureWriter, conn) -> None:
    client = conn.seed
    cfg = client.config
    for coll in ("shards", "databases", "collections", "tags", "settings", "mongos"):
        w.run("sharding", f"config.{coll}", lambda c=coll: list(cfg[c].find()),
              command={"find": f"config.{coll}"})
    # Chunk counts per (collection uuid, shard), plus jumbo chunks. config.chunks
    # is keyed by collection uuid since 5.0; config.collections maps uuid -> ns.
    chunk_pipeline = [
        {"$group": {"_id": {"uuid": "$uuid", "shard": "$shard"},
                    "chunks": {"$sum": 1},
                    "jumbo": {"$sum": {"$cond": [{"$eq": ["$jumbo", True]}, 1, 0]}}}},
        {"$lookup": {"from": "collections", "localField": "_id.uuid", "foreignField": "uuid", "as": "c"}},
        {"$project": {"_id": 0, "ns": {"$first": "$c._id"}, "shard": "$_id.shard",
                      "chunks": 1, "jumbo": 1}},
        {"$sort": {"ns": 1, "shard": 1}},
    ]
    w.run("sharding", "chunkCounts", lambda: list(cfg.chunks.aggregate(chunk_pipeline)),
          command={"aggregate": "config.chunks", "pipeline": chunk_pipeline})
    w.run("sharding", "balancerStatus", lambda: client.admin.command("balancerStatus"),
          command={"balancerStatus": 1})
    w.run("sharding", "shardedDataDistribution",
          lambda: list(client.admin.aggregate([{"$shardedDataDistribution": {}}])),
          command={"aggregate": 1, "pipeline": [{"$shardedDataDistribution": {}}]})


def collect_cluster_settings(w: CaptureWriter, conn) -> None:
    admin = conn.seed.admin
    w.run("cluster", "getDefaultRWConcern", lambda: admin.command("getDefaultRWConcern"),
          command={"getDefaultRWConcern": 1})
    # featureCompatibilityVersion is not readable on mongos; every member's
    # getParameter:'*' (node section) already includes it.


def collect_auth(w: CaptureWriter, conn) -> None:
    admin = conn.seed.admin
    w.run("auth", "usersInfo", lambda: admin.command("usersInfo", {"forAllDBs": True}),
          command={"usersInfo": {"forAllDBs": True}})
    w.run("auth", "rolesInfo", lambda: admin.command("rolesInfo", 1, showPrivileges=True),
          command={"rolesInfo": 1, "showPrivileges": True})
