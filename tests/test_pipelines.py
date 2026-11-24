from mongo_survey import pipelines


def test_full_pipeline_lists_namespaces_and_groups():
    p = pipelines.full_stats_pipeline(["a.x", "b.y"])
    assert p[1] == {"$match": {"ns": {"$in": ["a.x", "b.y"]}}}
    # $project after the stage empties storageStats; the shaper must be a $group
    assert list(p[2]) == ["$group"]
    assert p[2]["$group"]["_id"] == {"ns": "$ns", "host": "$host"}


def test_light_pipeline_has_no_storage_stats():
    (stage,) = pipelines.light_stats_pipeline()
    assert "storageStats" not in stage["$_internalAllCollectionStats"]["stats"]


def test_flatten_full():
    d = pipelines.flatten_full({"_id": {"ns": "a.x", "host": "h:1"}, "d": {"storage": {"count": 1}}})
    assert d == {"ns": "a.x", "host": "h:1", "storage": {"count": 1}}
