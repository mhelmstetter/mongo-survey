"""Aggregation pipelines. Every field mongo-survey reads per collection is listed here.

$_internalAllCollectionStats (6.0+) returns collection stats for every
collection on a node in one aggregation, instead of one collStats command per
collection. It is an internal, undocumented stage; its options:

  stats.storageStats   collStats-style storage stats. Opens each collection's
                       WiredTiger data handle (dhandle), so it is the expensive part.
  stats.latencyStats   per-collection reads/writes/commands op counts and latency
                       (from the same counters as the `top` command). Cheap.
  stats.count          document count (fast count). Cheap.
  stats.queryExecStats collection scan counters. Cheap.

A `$match` on `ns` directly after the stage is absorbed into it (explain shows
it inside the stage), so `{ns: {$in: [...]}}` limits which collections are
opened at all. Full stats are requested in batches of explicitly listed
namespaces; the namespace list comes from one cheap unfiltered light pass.

Shaping: storageStats is ~180 KB per collection (hundreds of WiredTiger
counters). We keep a curated subset server-side. NOTE: `$project`/`$addFields`
after the stage return storageStats subfields as empty (observed on 8.0), but a
`$group` keyed on {ns, host} with `$first` works, so the shaper uses `$group`.
Each (ns, host) pair is unique, so the group is a 1:1 reshaping.
"""
from __future__ import annotations

from typing import Any, Dict, List

# WiredTiger cache counters, per collection and per index.
WT_CACHE_FIELDS = [
    "bytes currently in the cache",
    "bytes read into cache",
    "bytes written from cache",
    "bytes dirty in the cache cumulative",
    "tracked dirty bytes in the cache",
    "pages requested from the cache",
    "pages read into cache",
    "pages written from cache",
    "modified pages evicted",
    "unmodified pages evicted",
]
# WiredTiger cursor counters: how much the btree is actually being touched.
WT_CURSOR_FIELDS = [
    "search calls",
    "search near calls",
    "next calls",
    "prev calls",
    "insert calls",
    "update calls",
    "modify",
    "remove calls",
]
WT_BLOCK_FIELDS = [
    "file size in bytes",
    "file bytes available for reuse",
]
WT_BTREE_FIELDS = [
    "maximum tree depth",
    "row-store internal pages",
    "row-store leaf pages",
    "overflow pages",
]
STORAGE_TOP_FIELDS = [
    "count",
    "size",
    "avgObjSize",
    "storageSize",
    "freeStorageSize",
    "capped",
    "nindexes",
    "totalIndexSize",
    "totalSize",
    "indexSizes",
    "numOrphanDocs",
]


def _get(field: str, input_expr: Any) -> Dict[str, Any]:
    # $getField handles WiredTiger's field names, which contain spaces.
    return {"$getField": {"field": {"$literal": field}, "input": input_expr}}


def _wt_section(input_expr: str, section: str, fields: List[str]) -> Dict[str, Any]:
    sec = _get(section, input_expr)
    return {f: _get(f, sec) for f in fields}


def _wt_subset(wt_expr: str) -> Dict[str, Any]:
    return {
        "cache": _wt_section(wt_expr, "cache", WT_CACHE_FIELDS),
        "cursor": _wt_section(wt_expr, "cursor", WT_CURSOR_FIELDS),
        "block-manager": _wt_section(wt_expr, "block-manager", WT_BLOCK_FIELDS),
        "btree": _wt_section(wt_expr, "btree", WT_BTREE_FIELDS),
    }


def ns_match(namespaces: List[str]) -> Dict[str, Any]:
    return {"$match": {"ns": {"$in": list(namespaces)}}}


def full_stats_pipeline(namespaces: List[str]) -> List[Dict[str, Any]]:
    """Storage + cache + latency + scan stats for exactly these namespaces."""
    ss = "$storageStats"
    shaped = {
        "shard": "$shard",
        "localTime": "$localTime",
        "latencyStats": "$latencyStats",
        "queryExecStats": "$queryExecStats",
        "storage": {f: f"{ss}.{f}" for f in STORAGE_TOP_FIELDS},
        "blockCompressor": {
            "$arrayElemAt": [
                {"$getField": {"field": "captures", "input": {
                    "$regexFind": {"input": f"{ss}.wiredTiger.creationString",
                                   "regex": "block_compressor=([a-z0-9]*)"}}}},
                0,
            ]
        },
        "wiredTiger": _wt_subset(f"{ss}.wiredTiger"),
        "indexes": {
            "$map": {
                "input": {"$objectToArray": {"$ifNull": [f"{ss}.indexDetails", {}]}},
                "in": {"name": "$$this.k", **_wt_subset("$$this.v")},
            }
        },
    }
    return [
        {"$_internalAllCollectionStats": {"stats": {
            "storageStats": {"verbose": False, "waitForLock": True, "numericOnly": False},
            "latencyStats": {"histograms": False},
            "count": {},
            "queryExecStats": {},
        }}},
        ns_match(namespaces),
        {"$group": {"_id": {"ns": "$ns", "host": "$host"}, "d": {"$first": shaped}}},
    ]


def light_stats_pipeline() -> List[Dict[str, Any]]:
    """Op counts and document counts for every collection on the node: no
    storageStats, so no dhandle opens. Unfiltered on purpose (one call per
    node); callers keep only the databases in scope."""
    return [
        {"$_internalAllCollectionStats": {"stats": {
            "latencyStats": {"histograms": False},
            "count": {},
            "queryExecStats": {},
        }}},
    ]


def flatten_full(doc: Dict[str, Any]) -> Dict[str, Any]:
    """Undo the $group wrapper: {_id:{ns,host}, d:{...}} -> {ns, host, ...}."""
    out = {"ns": doc["_id"]["ns"], "host": doc["_id"].get("host")}
    out.update(doc["d"])
    return out


INDEX_STATS_PIPELINE: List[Dict[str, Any]] = [
    {"$indexStats": {}},
    {"$project": {"_id": 0, "name": 1, "key": 1, "host": 1, "shard": 1,
                  "accesses": 1, "spec": 1, "building": 1}},
]
