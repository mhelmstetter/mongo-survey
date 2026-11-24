from mongo_survey.targeting import Candidate, plan_rounds, predict


def member(shard, host, selectable=True, **tags):
    return Candidate(shard=shard, host=host, tags=tuple(sorted(tags.items())), selectable=selectable)


def atlas_like():
    """5 shards x 3 members; shard s4 has no member in zone a (seen on a real cluster)."""
    zones = {
        "s0": ["b", "c", "f"], "s1": ["a", "c", "f"], "s2": ["b", "c", "f"],
        "s3": ["b", "c", "f"], "s4": ["b", "c", "f"],
    }
    out = []
    for shard, zs in zones.items():
        for i, z in enumerate(zs):
            out.append(member(shard, f"{shard}-{i}", availabilityZone=z, nodeType="ELECTABLE",
                              provider="GCP", diskState="READY"))
    return out


def covered(plan):
    return {h for r in plan.rounds for h in r.expected.values() if h}


def test_atlas_like_covers_everyone():
    ms = atlas_like()
    plan = plan_rounds(ms)
    assert covered(plan) == {m.host for m in ms}
    assert not plan.untargetable
    # 3 zones per shard -> 3 rounds is optimal
    assert len(plan.rounds) == 3
    # every round ends with the {} fallback so a shard without the zone cannot fail the command
    assert all(r.read_preference_tags()[-1] == {} for r in plan.rounds)


def test_volatile_tags_never_used():
    plan = plan_rounds(atlas_like())
    for r in plan.rounds:
        for ts in r.tag_sets:
            assert "diskState" not in dict(ts)


def test_identical_tags_are_untargetable():
    ms = [member("s0", "a", zone="x"), member("s0", "b", zone="x"), member("s0", "c", zone="y")]
    plan = plan_rounds(ms)
    assert covered(plan) == {"c"}
    assert {m.host for m in plan.untargetable} == {"a", "b"}


def test_hidden_member_is_untargetable():
    ms = [member("s0", "a", zone="x"), member("s0", "b", zone="y"), member("s0", "h", selectable=False, zone="z")]
    plan = plan_rounds(ms)
    assert covered(plan) == {"a", "b"}
    assert [m.host for m in plan.untargetable] == ["h"]


def test_single_member_shard_needs_no_tags():
    ms = [member("s0", "only")]
    plan = plan_rounds(ms)
    assert covered(plan) == {"only"}


def test_untagged_replica_set_is_all_gaps():
    ms = [member("s0", "a"), member("s0", "b"), member("s0", "c")]
    plan = plan_rounds(ms)
    assert covered(plan) == set()
    assert len(plan.untargetable) == 3


def test_multiple_tagsets_pack_into_one_round():
    # Shards use disjoint tag vocabularies; one round should reach one member of each.
    ms = [member("s0", "a0", dc="east"), member("s0", "a1", dc="west"),
          member("s1", "b0", rack="r1"), member("s1", "b1", rack="r2")]
    plan = plan_rounds(ms)
    assert covered(plan) == {"a0", "a1", "b0", "b1"}
    assert len(plan.rounds) == 2
    assert all(len(r.tag_sets) == 2 for r in plan.rounds)


def test_predict_first_matching_tagset_wins():
    ms = [member("s0", "a", zone="x"), member("s0", "b", zone="y")]
    assert predict([(("zone", "y"),), (("zone", "x"),)], ms) == "b"
    assert predict([(("zone", "q"),)], ms) is None  # falls to {} with 2 eligible members
