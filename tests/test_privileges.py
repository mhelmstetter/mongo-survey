from mongo_survey.privileges import CLUSTER, Need, has, missing, required, resource_matches


def priv(resource, *actions):
    return {"resource": resource, "actions": list(actions)}


def test_cluster_resource():
    assert resource_matches({"cluster": True}, CLUSTER, "")
    assert not resource_matches({"db": "", "collection": ""}, CLUSTER, "")
    assert resource_matches({"anyResource": True}, CLUSTER, "")


def test_any_normal_excludes_config_and_local():
    any_normal = {"db": "", "collection": ""}
    assert resource_matches(any_normal, "app", "")
    assert not resource_matches(any_normal, "config", "chunks")
    assert not resource_matches(any_normal, "local", "oplog.rs")


def test_database_wide_need_requires_database_wide_grant():
    assert not resource_matches({"db": "app", "collection": "users"}, "app", "")
    assert resource_matches({"db": "app", "collection": ""}, "app", "")


def test_specific_collection_grant():
    assert resource_matches({"db": "config", "collection": "chunks"}, "config", "chunks")
    assert not resource_matches({"db": "config", "collection": "chunks"}, "config", "shards")


def test_cluster_monitor_plus_read_any_satisfies_replset():
    privs = [
        priv({"cluster": True}, "serverStatus", "hostInfo", "getCmdLineOpts", "getParameter", "getLog",
             "top", "listDatabases", "allCollectionStats", "replSetGetConfig", "replSetGetStatus"),
        priv({"db": "", "collection": ""}, "listCollections", "dbStats", "collStats", "indexStats"),
        priv({"db": "local", "collection": ""}, "find"),
    ]
    assert missing(privs, required("replset", ["app"], include_auth=False)) == []


def test_missing_reported():
    privs = [priv({"db": "", "collection": ""}, "listCollections", "dbStats", "collStats")]
    miss = missing(privs, required("replset", ["app"], include_auth=False))
    actions = {n.action for n in miss}
    assert "indexStats" in actions and "serverStatus" in actions


def test_has():
    assert has([priv({"anyResource": True}, "find")], Need("config", "chunks", "find", ""))
