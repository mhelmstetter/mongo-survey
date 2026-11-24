"""Markdown summary of one or more capture files.

Combining captures:
  - snapshot data (sizes, $indexStats counters) uses the newest observation per
    member across all captures;
  - rates (cache churn, ops) are total counter delta / total sampled seconds per
    (namespace, member), then summed across members. Counter decreases (a
    restart between samples) are excluded and counted in the notes.
"""
from __future__ import annotations

import datetime as dt
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

from .output import read_capture

CACHE = "cache"
C_IN = "bytes currently in the cache"
C_READ = "bytes read into cache"
C_WRITE = "bytes written from cache"
C_PAGES = "pages requested from the cache"


# ----------------------------------------------------------------------------- formatting

def human_bytes(n: Optional[float]) -> str:
    if n is None:
        return "—"
    n = float(n)
    for unit in ("B", "KB", "MB", "GB", "TB", "PB"):
        if abs(n) < 1024 or unit == "PB":
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def human_rate(n: Optional[float]) -> str:
    return "—" if n is None else human_bytes(n) + "/s"


def num(n: Optional[float], digits: int = 0) -> str:
    if n is None:
        return "—"
    return f"{n:,.{digits}f}"


def table(headers: List[str], rows: Iterable[List[Any]]) -> str:
    rows = list(rows)
    if not rows:
        return "_none_\n"
    out = ["| " + " | ".join(headers) + " |", "|" + "|".join("---" for _ in headers) + "|"]
    for r in rows:
        out.append("| " + " | ".join(str(c).replace("|", "\\|") for c in r) + " |")
    return "\n".join(out) + "\n"


def short_host(h: str) -> str:
    return h


# ----------------------------------------------------------------------------- model

@dataclass
class Capture:
    path: str
    header: Dict[str, Any]
    footer: Optional[Dict[str, Any]]
    # (ns, host) -> list of (localTime, doc) ordered by time
    collstats: Dict[Tuple[str, str], List[Tuple[dt.datetime, Dict[str, Any]]]] = field(default_factory=lambda: defaultdict(list))
    # (ns, host) -> phase -> list of index docs
    indexstats: Dict[Tuple[str, str], Dict[str, Tuple[dt.datetime, List[Dict[str, Any]]]]] = field(default_factory=lambda: defaultdict(dict))
    node: Dict[str, Dict[str, Any]] = field(default_factory=lambda: defaultdict(dict))
    error_records: int = 0
    # host -> grouped $queryStats docs (one per query shape)
    query_stats: Dict[str, List[Dict[str, Any]]] = field(default_factory=lambda: defaultdict(list))
    query_stats_hashed: bool = False


def load(path: str) -> Capture:
    header = None
    footer = None
    cap: Optional[Capture] = None
    for rec in read_capture(path):
        t = rec.get("type")
        if t == "header":
            header = rec
            cap = Capture(path=path, header=rec, footer=None)
            continue
        if cap is None:
            raise ValueError(f"{path}: first line is not a mongo-survey header")
        if t == "footer":
            footer = rec
            continue
        if rec.get("error"):
            cap.error_records += 1
            continue
        sec, sub, out = rec["section"], rec["subsection"], rec["output"]
        if sec == "collstats" and rec.get("sample", -1) >= 0:
            for d in out:
                cap.collstats[(d["ns"], d["host"])].append((d.get("localTime") or rec["ts"]["end"], d))
        elif sec == "indexstats":
            for d in out:
                cap.indexstats[(rec["ns"], d["host"])].setdefault(sub, (rec["ts"]["end"], []))[1].append(d)
        elif sec == "querystats" and sub == "shapes":
            cap.query_stats[rec["host"]].extend(out)
            cap.query_stats_hashed = cap.query_stats_hashed or bool(rec.get("identifiersHashed"))
        elif sec == "node":
            cap.node[rec["host"]][sub] = out
    if cap is None:
        raise ValueError(f"{path}: empty capture")
    cap.footer = footer
    for series in cap.collstats.values():
        series.sort(key=lambda x: x[0])
    _ = header
    return cap


# ----------------------------------------------------------------------------- derived metrics

def _get(d: Dict[str, Any], *path: str) -> Optional[float]:
    for p in path:
        if not isinstance(d, dict) or p not in d:
            return None
        d = d[p]
    return d  # type: ignore[return-value]


@dataclass
class Rate:
    delta: float = 0.0
    seconds: float = 0.0
    resets: int = 0

    def add_series(self, points: List[Tuple[dt.datetime, Optional[float]]]) -> None:
        pts = [(t, v) for t, v in points if v is not None]
        for (t0, v0), (t1, v1) in zip(pts, pts[1:]):
            secs = (t1 - t0).total_seconds()
            if secs <= 0:
                continue
            if v1 < v0:
                self.resets += 1
                continue
            self.delta += v1 - v0
            self.seconds += secs

    @property
    def per_sec(self) -> Optional[float]:
        return self.delta / self.seconds if self.seconds > 0 else None


class HostRates:
    """One Rate per member; the cluster rate is the sum of member rates."""

    def __init__(self):
        self.by_host: Dict[str, Rate] = defaultdict(Rate)

    def add_series(self, host: str, points) -> None:
        self.by_host[host].add_series(points)

    @property
    def per_sec(self) -> Optional[float]:
        rates = [r.per_sec for r in self.by_host.values() if r.per_sec is not None]
        return sum(rates) if rates else None

    @property
    def delta(self) -> float:
        return sum(r.delta for r in self.by_host.values())

    @property
    def resets(self) -> int:
        return sum(r.resets for r in self.by_host.values())


class Analysis:
    def __init__(self, caps: List[Capture]):
        self.caps = caps
        self.member_state: Dict[str, str] = {}
        self.member_shard: Dict[str, str] = {}
        self.cache_max: Dict[str, float] = {}
        for c in caps:
            topo = c.header["topology"]
            groups = list(topo["shards"].items()) + [("config", topo["configServers"])]
            for shard, members in groups:
                for m in members:
                    self.member_shard.setdefault(m["host"], shard)
                    if m.get("state"):
                        self.member_state[m["host"]] = m["state"]
        self.resets = 0

    # --- latest full collstats per (ns, host) across captures
    def latest_collstats(self) -> Dict[Tuple[str, str], Dict[str, Any]]:
        best: Dict[Tuple[str, str], Tuple[dt.datetime, Dict[str, Any]]] = {}
        for c in self.caps:
            for key, series in c.collstats.items():
                t, d = series[-1]
                if "storage" not in d:
                    continue
                if key not in best or t > best[key][0]:
                    best[key] = (t, d)
        return {k: v[1] for k, v in best.items()}

    def collection_sizes(self) -> List[Dict[str, Any]]:
        """Per ns: per shard take the max over its members, then sum shards."""
        per_ns_shard: Dict[Tuple[str, str], Dict[str, float]] = {}
        for (ns, host), d in self.latest_collstats().items():
            shard = d.get("shard") or self.member_shard.get(host, host)
            s = d["storage"]
            cur = per_ns_shard.setdefault((ns, shard), {})
            for k in ("count", "size", "storageSize", "totalIndexSize", "freeStorageSize"):
                v = s.get(k)
                if v is not None:
                    cur[k] = max(cur.get(k, 0), v)
            cur["compressor"] = d.get("blockCompressor")
        out: Dict[str, Dict[str, Any]] = {}
        for (ns, shard), v in per_ns_shard.items():
            o = out.setdefault(ns, {"ns": ns, "shards": set(), "count": 0, "size": 0, "storageSize": 0,
                                    "totalIndexSize": 0, "freeStorageSize": 0, "compressor": v.get("compressor")})
            if v.get("count", 0) > 0:
                o["shards"].add(shard)
            for k in ("count", "size", "storageSize", "totalIndexSize", "freeStorageSize"):
                o[k] += v.get(k, 0)
        return sorted(out.values(), key=lambda o: -(o["storageSize"] + o["totalIndexSize"]))

    def index_usage(self) -> List[Dict[str, Any]]:
        """Newest $indexStats per (ns, index, host) across captures."""
        newest: Dict[Tuple[str, str, str], Tuple[dt.datetime, Dict[str, Any]]] = {}
        window: Dict[Tuple[str, str, str], int] = {}
        for c in self.caps:
            for (ns, host), phases in c.indexstats.items():
                for phase, (t, docs) in phases.items():
                    for d in docs:
                        k = (ns, d["name"], host)
                        if k not in newest or t > newest[k][0]:
                            newest[k] = (t, d)
                if "start" in phases and "end" in phases:
                    start = {d["name"]: d for d in phases["start"][1]}
                    for d in phases["end"][1]:
                        s = start.get(d["name"])
                        if s and _get(s, "accesses", "since") == _get(d, "accesses", "since"):
                            k = (ns, d["name"], host)
                            window[k] = window.get(k, 0) + (d["accesses"]["ops"] - s["accesses"]["ops"])
        gaps = self._index_gap_ns()
        sizes = self._index_sizes()
        rows: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for (ns, name, host), (_, d) in newest.items():
            r = rows.setdefault((ns, name), {"ns": ns, "name": name, "key": d.get("key"), "ops": 0,
                                             "hosts": 0, "usedOn": [], "since": None,
                                             "primaryOps": 0, "secondaryOps": 0, "windowOps": 0,
                                             "incomplete": ns in gaps, "spec": d.get("spec") or {},
                                             "size": sizes.get((ns, name))})
            ops = d["accesses"]["ops"]
            since = d["accesses"]["since"]
            r["ops"] += ops
            r["hosts"] += 1
            r["windowOps"] += window.get((ns, name, host), 0)
            if ops:
                r["usedOn"].append(host)
            if r["since"] is None or since > r["since"]:
                r["since"] = since  # newest "since" = shortest observation window
            if self.member_state.get(host) == "PRIMARY":
                r["primaryOps"] += ops
            else:
                r["secondaryOps"] += ops
        return sorted(rows.values(), key=lambda r: (r["ns"], r["name"]))

    def _index_gap_ns(self) -> Set[str]:
        out: Set[str] = set()
        for c in self.caps:
            if c.footer is None:
                # truncated capture: we cannot tell whether coverage was complete
                out |= {ns for ns, _ in c.indexstats}
                continue
            out |= {g["ns"] for g in c.footer["coverage"]["indexStatsNamespaceGaps"]}
        return out

    def _index_sizes(self) -> Dict[Tuple[str, str], float]:
        per: Dict[Tuple[str, str, str], float] = {}
        for (ns, host), d in self.latest_collstats().items():
            shard = d.get("shard") or self.member_shard.get(host, host)
            for name, sz in (d["storage"].get("indexSizes") or {}).items():
                k = (ns, name, shard)
                per[k] = max(per.get(k, 0), sz)
        out: Dict[Tuple[str, str], float] = defaultdict(float)
        for (ns, name, _), sz in per.items():
            out[(ns, name)] += sz
        return out

    # --- cache / ops rates
    def collection_rates(self) -> List[Dict[str, Any]]:
        acc: Dict[str, Dict[str, Any]] = {}
        for c in self.caps:
            for (ns, host), series in c.collstats.items():
                a = acc.setdefault(ns, {"ns": ns, "read": HostRates(), "write": HostRates(),
                                        "pages": HostRates(), "reads": HostRates(), "writes": HostRates(),
                                        "readLat": HostRates(), "scans": HostRates(), "inCache": 0.0,
                                        "hosts": set()})
                full = [(t, d) for t, d in series if "wiredTiger" in d]
                for key, path in (("read", (CACHE, C_READ)), ("write", (CACHE, C_WRITE)),
                                  ("pages", (CACHE, C_PAGES))):
                    a[key].add_series(host, [(t, _get(d, "wiredTiger", *path)) for t, d in full])
                for key, path in (("reads", ("latencyStats", "reads", "ops")),
                                  ("writes", ("latencyStats", "writes", "ops")),
                                  ("readLat", ("latencyStats", "reads", "latency")),
                                  ("scans", ("queryExecStats", "collectionScans", "total"))):
                    a[key].add_series(host, [(t, _get(d, *path)) for t, d in series])
                if full:
                    a["inCache"] += _get(full[-1][1], "wiredTiger", CACHE, C_IN) or 0
                    a["hosts"].add(host)
        for a in acc.values():
            self.resets += sum(a[k].resets for k in ("read", "write", "pages", "reads", "writes"))
        return list(acc.values())

    def index_cache_rates(self) -> List[Dict[str, Any]]:
        acc: Dict[Tuple[str, str], Dict[str, Any]] = {}
        for c in self.caps:
            for (ns, host), series in c.collstats.items():
                full = [(t, d) for t, d in series if "indexes" in d]
                names = {i["name"] for _, d in full for i in d["indexes"]}
                for name in names:
                    a = acc.setdefault((ns, name), {"ns": ns, "name": name, "read": HostRates(),
                                                    "write": HostRates(), "pages": HostRates(), "inCache": 0.0})
                    pts = [(t, next((i for i in d["indexes"] if i["name"] == name), None)) for t, d in full]
                    for key, field_ in (("read", C_READ), ("write", C_WRITE), ("pages", C_PAGES)):
                        a[key].add_series(host, [(t, _get(i, CACHE, field_) if i else None) for t, i in pts])
                    last = pts[-1][1] if pts else None
                    a["inCache"] += (_get(last, CACHE, C_IN) or 0) if last else 0
        return list(acc.values())

    def node_rows(self) -> List[List[Any]]:
        """Point-in-time node state from each member's serverStatus in the node
        snapshot (newest capture wins). Node time series belong to FTDC."""
        newest: Dict[str, Tuple[dt.datetime, Dict[str, Any]]] = {}
        for c in self.caps:
            for host, cmds in c.node.items():
                ss = cmds.get("serverStatus")
                if ss is None:
                    continue
                t = ss["localTime"]
                if host not in newest or t > newest[host][0]:
                    newest[host] = (t, ss)
        rows = []
        for host, (_, ss) in sorted(newest.items()):
            cache = ss["wiredTiger"]["cache"]
            mx = cache["maximum bytes configured"]
            used = cache["bytes currently in the cache"]
            dirty = cache["tracked dirty bytes in the cache"]
            rows.append([
                host, self.member_shard.get(host, ""), self.member_state.get(host, ""), ss["version"],
                f"{ss['uptime'] / 86400:.1f} d", human_bytes(mx), f"{100 * used / mx:.1f}%",
                f"{100 * dirty / mx:.2f}%",
                num(ss["wiredTiger"]["data-handle"]["connection data handles currently active"]),
                num(ss["connections"]["current"]),
            ])
        return rows


# ----------------------------------------------------------------------------- query stats

QS_DIST = ("totalExecMicros", "firstResponseExecMicros", "docsReturned", "keysExamined",
           "docsExamined", "bytesRead", "readTimeMicros", "workingTimeMillis", "cpuNanos")
QS_COUNT = ("hasSortStage", "usedDisk", "fromMultiPlanner", "fromPlanCache")


def _shape_key(doc: Dict[str, Any]) -> str:
    """Identity of a query shape across hosts: the server's queryShapeHash (8.0+),
    else the shape itself in server field order (order matters for sort and
    projection specs, so keys are not re-sorted)."""
    from bson import json_util
    return doc.get("queryShapeHash") or json_util.dumps(doc["_id"])


def query_shapes(caps: List[Capture]) -> List[Dict[str, Any]]:
    """Merge grouped $queryStats across hosts. Each host's store is independent
    (each mongos or member counts only what it executed), so sums add up. Across
    captures only the newest capture per host is used, since counters are cumulative."""
    newest: Dict[str, Tuple[Any, List[Dict[str, Any]]]] = {}
    for c in caps:
        for host, docs in c.query_stats.items():
            t = c.header["startedAt"]
            if host not in newest or t > newest[host][0]:
                newest[host] = (t, docs)
    merged: Dict[str, Dict[str, Any]] = {}
    for host, (_, docs) in newest.items():
        for d in docs:
            k = _shape_key(d)
            m = merged.setdefault(k, {"shape": d["_id"], "hosts": set(), "apps": set(), "readPreferences": set(),
                                      "execCount": 0, "entries": 0, "firstSeen": None, "latestSeen": None,
                                      **{f"{n}_sum": 0 for n in QS_DIST}, **{f"{n}_max": None for n in QS_DIST},
                                      **{f"{n}_n": 0 for n in QS_DIST + QS_COUNT}, **{n: 0 for n in QS_COUNT}})
            m["hosts"].add(host)
            m["apps"].update(a for a in d.get("apps", []) if a)
            m["readPreferences"].update(r for r in d.get("readPreferences", []) if r)
            m["execCount"] += d["execCount"]
            m["entries"] += d["entries"]
            for n in QS_DIST:
                m[f"{n}_sum"] += d[f"{n}_sum"]
                m[f"{n}_n"] += d[f"{n}_n"]
                if d.get(f"{n}_max") is not None:
                    m[f"{n}_max"] = d[f"{n}_max"] if m[f"{n}_max"] is None else max(m[f"{n}_max"], d[f"{n}_max"])
            for n in QS_COUNT:
                m[n] += d[n]
                m[f"{n}_n"] += d[f"{n}_n"]
            for f, fn in (("firstSeen", min), ("latestSeen", max)):
                if d.get(f) is not None:
                    m[f] = d[f] if m[f] is None else fn(m[f], d[f])
    return list(merged.values())


def _metric(m: Dict[str, Any], name: str) -> Optional[float]:
    """Sum of a metric, or None if no entry ever reported it (older server)."""
    return m[f"{name}_sum"] if m[f"{name}_n"] else None


def _shape_text(shape: Dict[str, Any], width: int = 110) -> str:
    from bson import json_util
    body = {k: v for k, v in shape.items() if k not in ("cmdNs", "command")}
    txt = json_util.dumps(body)
    return txt if len(txt) <= width else txt[:width - 1] + "…"


def _ratio(a: Optional[float], b: Optional[float]) -> Optional[float]:
    return None if a is None or not b else a / b


def render_query_stats(caps: List[Capture], top: int) -> List[str]:
    all_shapes = query_shapes(caps)
    # Leave out queries issued by mongo-survey itself (they carry its appName).
    shapes = [s for s in all_shapes if not (s["apps"] and all(a.startswith("mongo-survey/") for a in s["apps"]))]
    own = len(all_shapes) - len(shapes)
    out: List[str] = []
    hosts = {h for c in caps for h in c.query_stats}
    out.append("\n## Query shapes ($queryStats)\n")
    if not hosts:
        out.append("_Not collected (`--no-query-stats`, `cache` command, or an older capture)._\n")
        return out
    if not shapes:
        out.append(f"_{len(hosts)} host(s) returned no query stats. Collection may be disabled "
                   "(`internalQueryStatsRateLimit` 0; see the node/router getParameter records)._\n")
        return out
    hashed = any(c.query_stats_hashed for c in caps)
    out.append(f"{len(shapes)} distinct query shapes from {len(hosts)} host(s). Counters are cumulative since "
               "each shape entered the store (see first/latest seen); the store is a bounded cache, so "
               "rarely-run shapes may have been evicted."
               + (" Identifiers are HMAC-hashed." if hashed else "")
               + (f" {own} shape(s) issued by mongo-survey itself are not shown." if own else "") + "\n\n")
    total_us = sum(s["totalExecMicros_sum"] for s in shapes) or 1

    def row(s):
        ns = s["shape"].get("cmdNs", {})
        execs = s["execCount"]
        t = s["totalExecMicros_sum"]
        return [f"{ns.get('db', '?')}.{ns.get('coll', '?')}", s["shape"].get("command", "?"),
                "`" + _shape_text(s["shape"]).replace("`", "'") + "`",
                num(execs), num(t / 1e6, 1), f"{100 * t / total_us:.1f}%",
                num(_ratio(t / 1000, execs), 2), num((s["totalExecMicros_max"] or 0) / 1000, 1),
                num(_ratio(s["docsReturned_sum"], execs), 1),
                num(_ratio(_metric(s, "docsExamined"), s["docsReturned_sum"] or None), 1),
                num(_ratio(_metric(s, "keysExamined"), s["docsReturned_sum"] or None), 1),
                ", ".join(sorted(s["apps"]))[:40] or "—"]

    headers = ["namespace", "cmd", "shape", "execs", "total s", "share", "avg ms", "max ms",
               "docs ret/exec", "docs exam/ret", "keys exam/ret", "apps"]
    out.append(f"### By total execution time (top {top})\n")
    out.append(table(headers, [row(s) for s in sorted(shapes, key=lambda s: -s["totalExecMicros_sum"])[:top]]))
    out.append(f"\n### By execution count (top {top})\n")
    out.append(table(headers, [row(s) for s in sorted(shapes, key=lambda s: -s["execCount"])[:top]]))
    examined = [s for s in shapes if _metric(s, "docsExamined") is not None]
    if examined:
        out.append(f"\n### Most documents examined (top {top}) — candidates for better indexes\n")
        out.append(table(headers, [row(s) for s in sorted(examined, key=lambda s: -s["docsExamined_sum"])[:top]]))
    else:
        out.append("\n_docsExamined / keysExamined are not reported by this server version (added in 8.0)._\n")
    return out


# ----------------------------------------------------------------------------- render

def _capped(rows: List[List[Any]], limit: Optional[int]) -> Tuple[List[List[Any]], str]:
    if limit is None or len(rows) <= limit:
        return rows, ""
    return rows[:limit], f"\n_Showing the largest {limit} of {len(rows)}; rerun with --full-lists for all._\n"


def render(paths: List[str], top: int = 20, full_lists: bool = False) -> str:
    list_limit = None if full_lists else top * 5
    caps = [load(p) for p in paths]
    a = Analysis(caps)
    out: List[str] = []
    h0 = caps[-1].header
    s0 = h0["summary"]
    out.append(f"# mongo-survey report\n")
    out.append(f"MongoDB {s0['version']} {s0['topology']}, "
               f"{'Atlas' if s0['atlas']['isAtlas'] else 'not Atlas'}, "
               f"{s0['members']} data-bearing members, {s0['namespaces']} namespaces.\n")

    out.append("## Captures\n")
    out.append(table(
        ["file", "command", "started", "cluster access", "sample window", "errors"],
        [[c.path.rsplit("/", 1)[-1], c.header["command"], c.header["startedAt"],
          c.header["summary"]["access"]["mode"],
          f"{c.header['options']['cache_duration']:g}s / {c.header['options']['cache_interval']:g}s",
          len(c.footer["errors"]) if c.footer else "TRUNCATED (no footer)"] for c in caps]))

    out.append("## Coverage\n")
    out.append("Which members actually returned data. An index is never called unused "
               "unless every member that should have answered did.\n\n")
    rows = []
    for c in caps:
        if not c.footer:
            rows.append([c.path.rsplit("/", 1)[-1], "—", "capture truncated; coverage unknown", ""])
            continue
        for kind, k in c.footer["coverage"]["byKind"].items():
            rows.append([c.path.rsplit("/", 1)[-1], kind, f"{k['covered']}/{k['expected']}",
                         ", ".join(g["host"] for g in k["gaps"]) or "—"])
        g = c.footer["coverage"]["indexStatsNamespaceGaps"]
        rows.append([c.path.rsplit("/", 1)[-1], "$indexStats per namespace",
                     f"{len(g)} namespace(s) incomplete", ", ".join(x["ns"] for x in g[:5]) + (" …" if len(g) > 5 else "")])
    out.append(table(["capture", "data", "covered", "missing"], rows))
    errs = [e for c in caps if c.footer for e in c.footer["errors"]]
    if errs:
        out.append("\n### Command errors\n")
        grouped: Dict[Tuple[str, str, Any], int] = defaultdict(int)
        for e in errs:
            grouped[(e["section"] + "/" + e["subsection"], (e.get("error") or {}).get("codeName") or (e.get("error") or {}).get("type"),
                     (e.get("error") or {}).get("message", "")[:120])] += 1
        out.append(table(["command", "error", "message", "count"],
                         [[k[0], k[1], k[2], n] for k, n in sorted(grouped.items(), key=lambda x: -x[1])]))

    out.append("\n## Nodes (point in time, from the node snapshot)\n")
    out.append("One serverStatus per member at capture time (`collect` only). For node-level time "
               "series (cache pressure, eviction, tickets, replication) use FTDC.\n\n")
    out.append(table(["member", "shard", "state", "version", "uptime", "cache max", "cache used", "dirty",
                      "dhandles", "connections"], a.node_rows()))

    sizes = a.collection_sizes()
    out.append(f"\n## Largest collections (top {top})\n")
    out.append(table(["namespace", "docs", "data size", "storage", "indexes", "free (reusable)",
                      "compressor", "shards with data"],
                     [[o["ns"], num(o["count"]), human_bytes(o["size"]), human_bytes(o["storageSize"]),
                       human_bytes(o["totalIndexSize"]), human_bytes(o["freeStorageSize"]), o["compressor"] or "—",
                       len(o["shards"])] for o in sizes[:top]]))

    rates = a.collection_rates()
    out.append(f"\n## Cache churn by collection (top {top} by bytes read into cache)\n")
    out.append("Summed across all members. High read-into-cache with low residency = the working "
               "set does not fit and pages are being evicted and re-read.\n\n")
    out.append(table(["namespace", "in cache now", "read into cache", "written from cache", "pages requested/s"],
                     [[r["ns"], human_bytes(r["inCache"]), human_rate(r["read"].per_sec),
                       human_rate(r["write"].per_sec), num(r["pages"].per_sec, 1)]
                      for r in sorted(rates, key=lambda r: -(r["read"].per_sec or 0))[:top]]))
    out.append(f"\n## Cache residency by collection (top {top})\n")
    out.append(table(["namespace", "in cache now (all members)", "members"],
                     [[r["ns"], human_bytes(r["inCache"]), len(r["hosts"])]
                      for r in sorted(rates, key=lambda r: -r["inCache"])[:top]]))

    out.append(f"\n## Operations by collection (top {top} by reads/s)\n")
    out.append("From per-collection latencyStats. Secondaries count reads served to clients; "
               "replicated writes are not counted as writes there.\n\n")
    def avg_ms(r):
        ops = r["reads"].delta
        return None if not ops else r["readLat"].delta / ops / 1000
    out.append(table(["namespace", "reads/s", "writes/s", "avg read latency (ms)", "collection scans/s"],
                     [[r["ns"], num(r["reads"].per_sec, 1), num(r["writes"].per_sec, 1), num(avg_ms(r), 2),
                       num(r["scans"].per_sec, 2)]
                      for r in sorted(rates, key=lambda r: -(r["reads"].per_sec or 0))[:top]]))
    out.append(f"\n### By writes/s (top {top})\n")
    out.append(table(["namespace", "writes/s", "reads/s"],
                     [[r["ns"], num(r["writes"].per_sec, 1), num(r["reads"].per_sec, 1)]
                      for r in sorted(rates, key=lambda r: -(r["writes"].per_sec or 0))[:top]]))

    irates = a.index_cache_rates()
    out.append(f"\n## Cache churn by index (top {top})\n")
    out.append(table(["namespace", "index", "in cache now", "read into cache", "pages requested/s"],
                     [[r["ns"], r["name"], human_bytes(r["inCache"]), human_rate(r["read"].per_sec),
                       num(r["pages"].per_sec, 1)]
                      for r in sorted(irates, key=lambda r: -(r["read"].per_sec or 0))[:top]]))

    out.extend(render_query_stats(caps, top))

    usage = a.index_usage()
    unused = [u for u in usage if u["ops"] == 0 and not u["incomplete"] and u["name"] != "_id_"]
    unknown = [u for u in usage if u["ops"] == 0 and u["incomplete"] and u["name"] != "_id_"]
    sec_only = [u for u in usage if u["primaryOps"] == 0 and u["secondaryOps"] > 0]
    out.append("\n## Index usage\n")
    out.append(f"{len(usage)} indexes observed. Ops are $indexStats counters since each member's "
               "restart or the index's creation (the `since` column is the newest such start, i.e. the "
               "shortest observation window). $indexStats counts query use only: an index can show cache "
               "activity from writes (index maintenance) and still have zero ops. Unique and TTL indexes "
               "do work that $indexStats does not count; check the flags column before dropping.\n\n")
    out.append(f"### Unused on every member ({len(unused)})\n")
    rows, note = _capped([[u["ns"], u["name"], human_bytes(u["size"]), u["hosts"], u["since"], _flags(u["spec"])]
                          for u in sorted(unused, key=lambda u: -(u["size"] or 0))], list_limit)
    out.append(table(["namespace", "index", "size", "members", "observed since", "flags"], rows) + note)
    if unknown:
        out.append(f"\n### Zero ops but coverage incomplete — not safe to judge ({len(unknown)})\n")
        rows, note = _capped([[u["ns"], u["name"], u["hosts"]] for u in unknown], list_limit)
        out.append(table(["namespace", "index", "members seen"], rows) + note)
    out.append(f"\n### Used only on secondaries ({len(sec_only)}) — dropping these would break secondary reads\n")
    out.append("Member roles are as of capture time; an election since the counters started "
               "can make a former primary's usage appear here.\n\n")
    rows, note = _capped([[u["ns"], u["name"], num(u["secondaryOps"]), ", ".join(u["usedOn"])]
                          for u in sorted(sec_only, key=lambda u: -u["secondaryOps"])], list_limit)
    out.append(table(["namespace", "index", "secondary ops", "used on"], rows) + note)
    out.append(f"\n### Busiest indexes during the sample window (top {top})\n")
    out.append(table(["namespace", "index", "ops in window", "ops total", "size"],
                     [[u["ns"], u["name"], num(u["windowOps"]), num(u["ops"]), human_bytes(u["size"])]
                      for u in sorted(usage, key=lambda u: -u["windowOps"])[:top] if u["windowOps"]]))

    if a.resets:
        out.append(f"\n_Note: {a.resets} counter decrease(s) (member restart between samples) were excluded "
                   "from rate calculations._\n")
    return "\n".join(out)


def _flags(spec: Dict[str, Any]) -> str:
    f = []
    for k in ("unique", "sparse", "hidden"):
        if spec.get(k):
            f.append(k)
    if "partialFilterExpression" in spec:
        f.append("partial")
    if "expireAfterSeconds" in spec:
        f.append(f"TTL {spec['expireAfterSeconds']}s")
    return ", ".join(f)
