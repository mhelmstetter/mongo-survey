"""Per-member targeting through mongos using read preference tag sets.

Background: on sharded clusters where direct shard connections reject
collection-level commands (Atlas 8.0+ and any 8.0+ cluster without the
directShardOperations role), the only way to read a specific member's
$indexStats or collection stats is to route through mongos with a read
preference that selects that member.

A read preference {mode: "nearest", tags: [T1, T2, ..., {}]} is evaluated by
mongos *per shard*: for each shard it uses the first tag set that matches at
least one eligible member, then picks among the matching members. So one
command (a "round") reads from one member per shard, and with tag sets that
match exactly one member we know which one in advance.

The planner below builds rounds greedily until every targetable member is
expected to be covered. The trailing {} keeps a round from failing on a shard
where no listed tag set matches (mongos would otherwise error "Could not find
host matching read preference"). That shard's answer then comes from an
arbitrary member, which is harmless: results carry a `host` field and coverage
is verified from what actually came back, never from the plan alone.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from itertools import combinations
from typing import Dict, FrozenSet, List, Optional, Sequence, Set, Tuple

# Tried first when choosing identifying tags. Stable keys only.
PREFERRED_KEYS = ("availabilityZone", "nodeType", "region", "provider", "workloadType")
# Atlas tags whose values can change during a run; never use them for targeting.
VOLATILE_KEYS = frozenset({"diskState", "cacheState"})
MAX_TAGSET_SIZE = 3

TagSet = Tuple[Tuple[str, str], ...]  # sorted (key, value) pairs, hashable


@dataclass(frozen=True)
class Candidate:
    """A member as the planner sees it."""
    shard: str
    host: str
    tags: Tuple[Tuple[str, str], ...]
    selectable: bool  # can a read preference ever pick it? (not hidden/arbiter, healthy)

    def matches(self, ts: TagSet) -> bool:
        mine = dict(self.tags)
        return all(mine.get(k) == v for k, v in ts)


@dataclass
class Round:
    tag_sets: List[TagSet]
    expected: Dict[str, Optional[str]]  # shard -> host we expect, None if unpredictable

    def read_preference_tags(self) -> List[Dict[str, str]]:
        return [dict(ts) for ts in self.tag_sets] + [{}]

    def label(self) -> str:
        parts = [",".join(f"{k}={v}" for k, v in ts) for ts in self.tag_sets]
        return "tags[" + " | ".join(parts) + " | {}]"


@dataclass
class Plan:
    rounds: List[Round]
    untargetable: List[Candidate] = field(default_factory=list)

    @property
    def expected_hosts(self) -> Set[str]:
        return {h for r in self.rounds for h in r.expected.values() if h}


def _key_order(key: str) -> Tuple[int, str]:
    return (PREFERRED_KEYS.index(key), key) if key in PREFERRED_KEYS else (len(PREFERRED_KEYS), key)


def _candidate_tagsets(member: Candidate) -> List[TagSet]:
    keys = sorted((k for k, _ in member.tags if k not in VOLATILE_KEYS), key=_key_order)
    values = dict(member.tags)
    out: List[TagSet] = []
    for size in range(1, min(MAX_TAGSET_SIZE, len(keys)) + 1):
        for combo in combinations(keys, size):
            out.append(tuple(sorted((k, values[k]) for k in combo)))
    return out


def predict(tag_sets: Sequence[TagSet], shard_members: Sequence[Candidate]) -> Optional[str]:
    """Which member mongos will pick in this shard, or None if it is a coin toss."""
    eligible = [m for m in shard_members if m.selectable]
    for ts in tag_sets:
        matched = [m for m in eligible if m.matches(ts)]
        if matched:
            return matched[0].host if len(matched) == 1 else None
    # falls through to {}
    return eligible[0].host if len(eligible) == 1 else None


def _first_matching_index(tag_sets: Sequence[TagSet], shard_members: Sequence[Candidate]) -> Optional[int]:
    eligible = [m for m in shard_members if m.selectable]
    for i, ts in enumerate(tag_sets):
        if any(m.matches(ts) for m in eligible):
            return i
    return None


def plan_rounds(members: Sequence[Candidate]) -> Plan:
    by_shard: Dict[str, List[Candidate]] = {}
    for m in members:
        by_shard.setdefault(m.shard, []).append(m)

    untargetable = [m for m in members if not m.selectable]
    uncovered: Set[Tuple[str, str]] = {(m.shard, m.host) for m in members if m.selectable}

    # Tag sets that identify exactly one selectable member within its shard.
    identifying: Dict[TagSet, Set[Tuple[str, str]]] = {}
    for shard, ms in by_shard.items():
        eligible = [m for m in ms if m.selectable]
        for m in eligible:
            for ts in _candidate_tagsets(m):
                if sum(1 for o in eligible if o.matches(ts)) == 1:
                    identifying.setdefault(ts, set()).add((shard, m.host))

    rounds: List[Round] = []
    while uncovered:
        tag_sets: List[TagSet] = []
        # Shards whose pick is already fixed by an earlier tag set in this round.
        decided: Set[str] = set()
        while True:
            best: Optional[TagSet] = None
            best_gain: Set[Tuple[str, str]] = set()
            for ts, idents in identifying.items():
                if ts in tag_sets:
                    continue
                # Adding ts must not change the pick on any already-decided shard,
                # and only helps on shards where nothing earlier matched.
                gain = {(s, h) for s, h in idents if s not in decided and (s, h) in uncovered}
                if not gain:
                    continue
                key = (len(gain), -len(ts), [str(x) for x in ts])
                if best is None or key > (len(best_gain), -len(best), [str(x) for x in best]):
                    best, best_gain = ts, gain
            if best is None:
                break
            tag_sets.append(best)
            for shard, ms in by_shard.items():
                if _first_matching_index(tag_sets, ms) is not None:
                    decided.add(shard)

        expected = {shard: predict(tag_sets, ms) for shard, ms in by_shard.items()}
        newly = {(s, h) for s, h in expected.items() if h and (s, h) in uncovered}
        if not newly:
            # Nothing left that a tag set can single out (identical tags, untagged
            # members in multi-member shards). Those become coverage gaps.
            # A shard with one selectable member needs no tags: {} picks it.
            break
        rounds.append(Round(tag_sets=tag_sets, expected=expected))
        uncovered -= newly

    remaining_hosts = {h for _, h in uncovered}
    untargetable += [m for m in members if m.host in remaining_hosts]
    return Plan(rounds=rounds, untargetable=untargetable)


def candidates_from_topology(members, selectable_states: FrozenSet[str] = frozenset({"PRIMARY", "SECONDARY"})) -> List[Candidate]:
    out = []
    for m in members:
        if m.arbiter:
            continue
        ok = not m.hidden and (m.state is None or m.state in selectable_states)
        out.append(Candidate(shard=m.shard or m.replset, host=m.host,
                             tags=tuple(sorted(m.tags.items())), selectable=ok))
    return out
