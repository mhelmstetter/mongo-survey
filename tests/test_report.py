import datetime as dt

from mongo_survey.output import CaptureWriter
from mongo_survey.report import Rate, render

T0 = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)


def at(s):
    return T0 + dt.timedelta(seconds=s)


def test_rate_excludes_resets():
    r = Rate()
    r.add_series([(at(0), 100), (at(10), 200), (at(20), 50), (at(30), 150)])
    assert r.delta == 200 and r.seconds == 20 and r.resets == 1
    assert r.per_sec == 10


def test_rate_ignores_missing_points():
    r = Rate()
    r.add_series([(at(0), None), (at(10), 0), (at(20), 40)])
    assert r.per_sec == 4


def _cs(ns, host, t, read, inc, ops, idx_read):
    return {
        "ns": ns, "host": host, "shard": "rs0", "localTime": t,
        "latencyStats": {"reads": {"ops": ops, "latency": ops * 1000}, "writes": {"ops": 0, "latency": 0}},
        "queryExecStats": {"collectionScans": {"total": 0}},
        "storage": {"count": 10, "size": 1000, "storageSize": 500, "totalIndexSize": 100,
                    "freeStorageSize": 0, "indexSizes": {"_id_": 50, "a_1": 50}},
        "blockCompressor": "snappy",
        "wiredTiger": {"cache": {"bytes read into cache": read, "bytes currently in the cache": inc,
                                 "bytes written from cache": 0, "pages requested from the cache": 0}},
        "indexes": [{"name": "a_1", "cache": {"bytes read into cache": idx_read,
                                              "bytes currently in the cache": 1,
                                              "bytes written from cache": 0,
                                              "pages requested from the cache": 0}}],
    }


def _idx(host, name, ops):
    return {"name": name, "key": {}, "host": host, "shard": "rs0", "accesses": {"ops": ops, "since": T0}, "spec": {}}


def test_render_end_to_end(tmp_path):
    path = str(tmp_path / "cap.jsonl.gz")
    w = CaptureWriter(path, compress=True)
    members = [{"host": h, "state": s} for h, s in (("p:1", "PRIMARY"), ("s:1", "SECONDARY"))]
    w.write({"type": "header", "command": "collect", "startedAt": T0,
             "options": {"cache_duration": 20, "cache_interval": 10},
             "summary": {"version": "8.0.0", "topology": "replset", "atlas": {"isAtlas": False},
                         "members": 2, "namespaces": 1, "access": {"mode": "direct"}},
             "topology": {"shards": {"rs0": members}, "configServers": []}})
    for sample, secs in enumerate((0, 10, 20)):
        for host in ("p:1", "s:1"):
            w.record("collstats", "full", [_cs("app.c", host, at(secs), read=1000 * secs, inc=5,
                                               ops=10 * secs, idx_read=100 * secs)], sample=sample)
    for phase, ops in (("start", 0), ("end", 0)):
        w.record("indexstats", phase, [_idx("p:1", "_id_", 5), _idx("p:1", "a_1", 0)], ns="app.c")
        w.record("indexstats", phase, [_idx("s:1", "_id_", 0), _idx("s:1", "a_1", ops)], ns="app.c")
    w.write({"type": "footer", "coverage": {"byKind": {}, "indexStatsNamespaceGaps": []}, "errors": []})
    w.close()

    md = render([path], top=5)
    # 2 members x 1000 B/s each
    assert "2.0 KB/s" in md
    # reads: 2 members x 10 ops/s, avg latency 1 ms
    assert "| app.c | 20.0 | 0.0 | 1.00 |" in md
    # a_1 has zero ops on both members and full coverage -> unused
    unused = md.split("### Unused on every member")[1].split("###")[0]
    assert "a_1" in unused


def test_secondary_only_index(tmp_path):
    path = str(tmp_path / "cap.jsonl")
    w = CaptureWriter(path, compress=False)
    members = [{"host": "p:1", "state": "PRIMARY"}, {"host": "s:1", "state": "SECONDARY"}]
    w.write({"type": "header", "command": "collect", "startedAt": T0,
             "options": {"cache_duration": 0, "cache_interval": 10},
             "summary": {"version": "8.0.0", "topology": "replset", "atlas": {"isAtlas": False},
                         "members": 2, "namespaces": 1, "access": {"mode": "direct"}},
             "topology": {"shards": {"rs0": members}, "configServers": []}})
    w.record("indexstats", "start", [_idx("p:1", "b_1", 0)], ns="app.c")
    w.record("indexstats", "start", [_idx("s:1", "b_1", 7)], ns="app.c")
    w.write({"type": "footer", "coverage": {"byKind": {}, "indexStatsNamespaceGaps": []}, "errors": []})
    w.close()
    md = render([path])
    sec = md.split("### Used only on secondaries")[1].split("###")[0]
    assert "b_1" in sec and "s:1" in sec
    assert "b_1" not in md.split("### Unused on every member")[1].split("###")[0]


def _qs(shape, execs, us, hash_=None):
    d = {"_id": shape, "entries": 1, "execCount": execs, "apps": ["app"], "readPreferences": ["primary"],
         "firstSeen": T0, "latestSeen": T0, "queryShapeHash": hash_}
    for n in ("totalExecMicros", "firstResponseExecMicros", "docsReturned", "keysExamined",
              "docsExamined", "bytesRead", "readTimeMicros", "workingTimeMillis", "cpuNanos"):
        d[f"{n}_sum"] = us if n == "totalExecMicros" else (execs if n == "docsReturned" else 0)
        d[f"{n}_max"] = us
        d[f"{n}_n"] = 0 if n in ("keysExamined", "docsExamined", "bytesRead", "readTimeMicros",
                                 "workingTimeMillis", "cpuNanos") else 1
    for n in ("hasSortStage", "usedDisk", "fromMultiPlanner", "fromPlanCache"):
        d[n] = 0
        d[f"{n}_n"] = 0
    return d


def test_query_shapes_merge_across_hosts_and_keep_field_order(tmp_path):
    from mongo_survey.report import query_shapes, load
    path = str(tmp_path / "cap.jsonl")
    w = CaptureWriter(path, compress=False)
    w.write({"type": "header", "command": "collect", "startedAt": T0,
             "options": {"cache_duration": 0, "cache_interval": 10},
             "summary": {"version": "7.0.0", "topology": "sharded", "atlas": {"isAtlas": True},
                         "members": 0, "namespaces": 0, "access": {"mode": "direct"}},
             "topology": {"shards": {}, "configServers": []}})
    a = {"cmdNs": {"db": "d", "coll": "c"}, "command": "find", "sort": {"x": 1, "y": 1}}
    b = {"cmdNs": {"db": "d", "coll": "c"}, "command": "find", "sort": {"y": 1, "x": 1}}
    w.record("querystats", "shapes", [_qs(a, 10, 1000), _qs(b, 1, 50)], host="m1:1")
    w.record("querystats", "shapes", [_qs(a, 5, 500)], host="m2:1")
    w.write({"type": "footer", "coverage": {"byKind": {}, "indexStatsNamespaceGaps": []}, "errors": []})
    w.close()
    shapes = {str(list(s["shape"]["sort"])): s for s in query_shapes([load(path)])}
    assert len(shapes) == 2                      # different sort order = different shape
    assert shapes["['x', 'y']"]["execCount"] == 15  # summed across mongos
    assert shapes["['x', 'y']"]["hosts"] == {"m1:1", "m2:1"}
    md = render([path])
    assert "not reported by this server version" in md  # docsExamined missing -> not shown as 0
