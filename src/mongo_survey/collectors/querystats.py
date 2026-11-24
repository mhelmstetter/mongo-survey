"""$queryStats: per-query-shape workload statistics (MongoDB 7.1+, Atlas 7.0+).

Each process keeps its own query stats store: in a sharded cluster the
mongos routers record the queries they receive, in a replica set each member
records the queries it runs. So we read every mongos (sharded) or every data
member (replica set / standalone) over a direct connection.

The store holds one entry per (query shape x client metadata x comment x
batch size x read preference ...). Grouping on `key.queryShape` server-side
collapses those variants: on a real cluster 98k entries became 695 shapes.
Literal values are never present in query shapes (they are replaced by type
placeholders); field and collection names are, unless --query-stats-hmac is
used, which asks the server to HMAC-hash every identifier with a random
per-run key that is never written anywhere.
"""
from __future__ import annotations

import logging
import os
from typing import Any, Dict, List, Optional

from bson.binary import Binary
from pymongo.errors import PyMongoError

from ..output import CaptureWriter, describe_error, utcnow

log = logging.getLogger("mongo-survey")

SECTION = "querystats"
RECORD_CHUNK = 500

# Metrics with {sum, max, min, sumOfSquares}. Not every version reports every one
# (keysExamined, docsExamined, bytesRead, readTimeMicros, workingTimeMillis and cpuNanos arrived in 8.0; cpuNanos is Linux-only), so each
# gets a presence count and the report shows "—" where a metric was never reported.
DIST_METRICS = ["totalExecMicros", "firstResponseExecMicros", "docsReturned",
                "keysExamined", "docsExamined", "bytesRead", "readTimeMicros", "workingTimeMillis", "cpuNanos"]
# Per-execution counters (number of executions where the condition held).
COUNT_METRICS = ["hasSortStage", "usedDisk", "fromMultiPlanner", "fromPlanCache"]


def new_hmac_key() -> Binary:
    return Binary(os.urandom(32), subtype=8)


def pipeline(hmac_key: Optional[Binary]) -> List[Dict[str, Any]]:
    stage: Dict[str, Any] = {}
    if hmac_key is not None:
        stage["transformIdentifiers"] = {"algorithm": "hmac-sha-256", "hmacKey": hmac_key}
    m = "$metrics"
    present = lambda path: {"$sum": {"$cond": [{"$eq": [{"$type": path}, "missing"]}, 0, 1]}}  # noqa: E731
    group: Dict[str, Any] = {
        "_id": "$key.queryShape",
        "entries": {"$sum": 1},
        "queryShapeHash": {"$first": "$queryShapeHash"},  # 8.0+
        "execCount": {"$sum": f"{m}.execCount"},
        "firstSeen": {"$min": f"{m}.firstSeenTimestamp"},
        "latestSeen": {"$max": f"{m}.latestSeenTimestamp"},
        "apps": {"$addToSet": "$key.client.application.name"},
        "drivers": {"$addToSet": {"name": "$key.client.driver.name", "version": "$key.client.driver.version"}},
        "readPreferences": {"$addToSet": {"$getField": {
            "field": "mode", "input": {"$getField": {"field": {"$literal": "$readPreference"}, "input": "$key"}}}}},
        "readConcerns": {"$addToSet": "$key.readConcern.level"},
    }
    for name in DIST_METRICS:
        group[f"{name}_sum"] = {"$sum": f"{m}.{name}.sum"}
        group[f"{name}_max"] = {"$max": f"{m}.{name}.max"}
        group[f"{name}_n"] = present(f"{m}.{name}")
    for name in COUNT_METRICS:
        group[name] = {"$sum": f"{m}.{name}"}
        group[f"{name}_n"] = present(f"{m}.{name}")
    return [{"$queryStats": stage}, {"$group": group}]


def collect(w: CaptureWriter, conn, hosts: List[str], hmac_key: Optional[Binary], workers: int,
            role: str) -> None:
    """Read the grouped query stats from each host (mongos or mongod)."""
    from concurrent.futures import ThreadPoolExecutor

    pl = pipeline(hmac_key)
    # Never record the key itself; record only that identifiers were transformed.
    shown = pipeline(None)
    if hmac_key is not None:
        shown[0]["$queryStats"] = {"transformIdentifiers": {"algorithm": "hmac-sha-256", "hmacKey": "<random, not stored>"}}

    def one(host: str) -> None:
        start = utcnow()
        # 0 means the server records nothing (the default on self-managed deployments).
        rate_limit = w.run(SECTION, "rateLimit",
                           lambda: conn.direct(host).admin.command(
                               "getParameter", 1, internalQueryStatsRateLimit=1)["internalQueryStatsRateLimit"],
                           host=host, command={"getParameter": 1, "internalQueryStatsRateLimit": 1})
        if rate_limit == 0:
            log.warning("%s: internalQueryStatsRateLimit is 0, so query stats are not being recorded", host)
        try:
            # $queryStats must run entirely on the process that owns the store, so the
            # $group must not be allowed to spill to disk (Atlas enables
            # allowDiskUseByDefault, hence the explicit False). The grouped result is
            # small: one document per query shape.
            docs = list(conn.direct(host).admin.aggregate(pl, allowDiskUse=False))
        except PyMongoError as exc:
            w.record(SECTION, "shapes", None, error=describe_error(exc), host=host,
                     command={"aggregate": 1, "pipeline": shown}, start=start, end=utcnow(), role=role)
            return
        end = utcnow()
        for i in range(0, max(len(docs), 1), RECORD_CHUNK):
            w.record(SECTION, "shapes", docs[i:i + RECORD_CHUNK], host=host,
                     command={"aggregate": 1, "pipeline": shown} if i == 0 else None,
                     start=start, end=end, role=role, shapes=len(docs), rateLimit=rate_limit,
                     identifiersHashed=hmac_key is not None)

    with ThreadPoolExecutor(max_workers=workers) as pool:
        list(pool.map(one, hosts))
