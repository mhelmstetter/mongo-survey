"""Collection stats, $indexStats, and the cache/heat sampling loop."""
from __future__ import annotations

import logging
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Set, Tuple

from pymongo.errors import PyMongoError

from .. import pipelines
from ..output import CaptureWriter, describe_error, utcnow
from .targets import Coverage, HostAliases, Target

log = logging.getLogger("mongo-survey")

FULL = "full"
LIGHT = "light"
RECORD_CHUNK = 500  # result docs per capture record


def _stamp(doc: Dict, target: Target, aliases: HostAliases, member_shard: Dict[str, str]) -> Dict:
    """Normalize host/shard on a result doc. Direct targets know exactly who answered."""
    reported = doc.get("host")
    if target.host is not None:
        doc["host"] = target.host
        if reported and reported != target.host:
            doc["reportedHost"] = reported
    else:
        doc["host"] = aliases.resolve(reported)
        if reported != doc["host"]:
            doc["reportedHost"] = reported
    if target.shard is not None and not doc.get("shard"):
        doc["shard"] = target.shard
    if not doc.get("shard") and doc.get("host") in member_shard:
        doc["shard"] = member_shard[doc["host"]]
    return doc


def run_collection_stats(
    w: CaptureWriter, targets: List[Target], mode: str, coverage: Coverage, aliases: HostAliases,
    member_shard: Dict[str, str], workers: int, sample: int, *,
    databases: Set[str], namespaces: Optional[List[str]] = None, batch_size: int = 500,
    locations: Optional[Dict[str, Set[str]]] = None,
) -> Set[str]:
    """One pass of $_internalAllCollectionStats over every target.

    FULL: storage stats for `namespaces`, in batches of `batch_size` listed names.
    LIGHT: one unfiltered call per target; results outside `databases` are dropped.
    Returns the set of in-scope namespaces seen.
    """
    kind = f"collstats_{mode}"
    if mode == FULL:
        if namespaces is None:
            raise ValueError("full collection stats need an explicit namespace list")
        batches = [namespaces[i:i + batch_size] for i in range(0, len(namespaces), batch_size)]
    else:
        batches = [None]
    seen: Set[str] = set()

    def one(target: Target, batch_no: int, batch: Optional[List[str]]) -> List[Dict]:
        pipeline = pipelines.full_stats_pipeline(batch) if mode == FULL else pipelines.light_stats_pipeline()
        start = utcnow()
        try:
            raw = list(target.db("admin").aggregate(pipeline, allowDiskUse=True))
        except PyMongoError as exc:
            w.record("collstats", mode, None, error=describe_error(exc), target=target.label,
                     host=target.host, shard=target.shard, command={"aggregate": 1, "pipeline": pipeline},
                     start=start, end=utcnow(), batch=batch_no, sample=sample)
            return []
        end = utcnow()
        docs = []
        for d in raw:
            d = pipelines.flatten_full(d) if mode == FULL else d
            if d["ns"].split(".", 1)[0] not in databases:
                continue
            docs.append(_stamp(d, target, aliases, member_shard))
        # The pipeline is recorded in the first chunk only (it can hold 500 names).
        for i in range(0, max(len(docs), 1), RECORD_CHUNK):
            w.record("collstats", mode, docs[i:i + RECORD_CHUNK], target=target.label,
                     host=target.host, shard=target.shard,
                     command={"aggregate": 1, "pipeline": pipeline} if i == 0 else None,
                     start=start, end=end, batch=batch_no, sample=sample)
        return docs

    jobs = [(t, i, b) for t in targets for i, b in enumerate(batches)]
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for docs in pool.map(lambda j: one(*j), jobs):
            for d in docs:
                seen.add(d["ns"])
                if locations is not None and d.get("shard"):
                    locations.setdefault(d["ns"], set()).add(d["shard"])
                coverage.saw(kind, d.get("shard"), d.get("host"))
                # A shard is expected to answer $indexStats for ns if it holds documents.
                # (The database primary shard keeps an empty local copy of collections
                # whose chunks live elsewhere; mongos rightly never routes there.)
                if d.get("shard") and wants_index_stats(d["ns"]) and _doc_count(d) > 0:
                    coverage.ns_shards.setdefault(d["ns"], set()).add(d["shard"])
    return seen


def _doc_count(d: Dict) -> float:
    """Document count on this shard. Unknown counts as non-empty, so a missing
    value can only add an expected shard (reported as a gap), never hide one."""
    if d.get("count") is not None:
        return d["count"]
    storage_count = (d.get("storage") or {}).get("count")
    return float("inf") if storage_count is None else storage_count


def wants_index_stats(ns: str) -> bool:
    """$indexStats candidates: skip system.* except time-series buckets."""
    coll = ns.split(".", 1)[1]
    return not coll.startswith("system.") or coll.startswith("system.buckets.")


def index_stats_namespaces(namespaces: Set[str]) -> List[str]:
    return [ns for ns in sorted(namespaces) if wants_index_stats(ns)]


def run_index_stats(
    w: CaptureWriter, targets: List[Target], namespaces: List[str], coverage: Coverage,
    aliases: HostAliases, member_shard: Dict[str, str], workers: int, phase: str,
    locations: Optional[Dict[str, Set[str]]] = None,
) -> None:
    def one(target: Target, ns: str) -> Tuple[str, List[Dict]]:
        db, coll = ns.split(".", 1)
        start = utcnow()
        cmd = {"aggregate": coll, "pipeline": pipelines.INDEX_STATS_PIPELINE}
        try:
            docs = list(target.db(db)[coll].aggregate(pipelines.INDEX_STATS_PIPELINE))
        except PyMongoError as exc:
            w.record("indexstats", phase, None, error=describe_error(exc), target=target.label,
                     host=target.host, shard=target.shard, command=cmd, start=start, end=utcnow(), ns=ns)
            return ns, []
        docs = [_stamp(d, target, aliases, member_shard) for d in docs]
        w.record("indexstats", phase, docs, target=target.label, host=target.host, shard=target.shard,
                 command=cmd, start=start, end=utcnow(), ns=ns)
        return ns, docs

    # Direct targets only need the collections that exist on their shard;
    # mongos targets route by themselves.
    jobs = [(t, ns) for t in targets for ns in namespaces
            if t.shard is None or locations is None or t.shard in locations.get(ns, ())]
    done = 0
    with ThreadPoolExecutor(max_workers=workers) as pool:
        for ns, docs in pool.map(lambda j: one(*j), jobs):
            done += 1
            if done % 500 == 0:
                log.info("  $indexStats %s: %d/%d", phase, done, len(jobs))
            for d in docs:
                coverage.saw(f"indexstats_{phase}", d.get("shard"), d.get("host"))
                # A shard that answered is expected to answer from every member.
                if d.get("shard"):
                    coverage.ns_shards.setdefault(ns, set()).add(d["shard"])
                coverage.ns_index_hosts.setdefault(ns, set()).add(d.get("host"))


def run_sampling(
    w: CaptureWriter, targets: List[Target], databases: Set[str], namespaces: List[str],
    mode: str, duration: float, interval: float, coverage: Coverage, aliases: HostAliases,
    member_shard: Dict[str, str], workers: int, batch_size: int,
) -> Set[str]:
    """Sample per-collection and per-index stats every `interval` seconds for
    `duration` seconds (sample 0 at t=0, so duration 0 means one sample).

    Node-level time series (serverStatus) are deliberately not sampled: FTDC
    already records them every second on every node.
    """
    seen: Set[str] = set()
    t0 = time.monotonic()
    wall0 = time.time()
    sample = 0
    while True:
        log.info("sample %d (t=%.0fs)", sample, time.monotonic() - t0)
        # If this machine was suspended, wall-clock time runs ahead of the monotonic
        # clock (which stops during sleep on macOS). Record it: sample spacing and
        # any connection resets around it are explained by the client, not the cluster.
        skew = (time.time() - wall0) - (time.monotonic() - t0)
        if skew > 5:
            log.warning("this machine appears to have been suspended for ~%.0fs during sampling", skew)
            w.record("client", "suspended", {"seconds": round(skew, 1), "beforeSample": sample})
            wall0 += skew
        t_stats = time.monotonic()
        seen |= run_collection_stats(w, targets, mode, coverage, aliases, member_shard, workers, sample,
                                     databases=databases, namespaces=namespaces, batch_size=batch_size)
        log.info("  collection stats: %d target(s) in %.1fs", len(targets), time.monotonic() - t_stats)
        sample += 1
        next_at = sample * interval
        elapsed = time.monotonic() - t0
        if next_at > duration + 1e-6:
            break
        if elapsed > next_at:
            log.warning("sample %d took longer than --cache-interval (%.1fs elapsed); "
                        "sampling back-to-back", sample - 1, elapsed - (sample - 1) * interval)
        else:
            time.sleep(next_at - elapsed)
    return seen
