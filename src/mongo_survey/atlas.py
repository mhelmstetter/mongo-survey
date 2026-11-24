"""Atlas detection.

There is no single authoritative "this is Atlas" flag, so we collect evidence
and record every piece in the capture header. Any one piece counts.
"""
from __future__ import annotations

from typing import Any, Dict, List

from .topology import Topology

ATLAS_HOST_SUFFIXES = (
    ".mongodb.net",
    ".mongodb-dev.net",
    ".mongodb-qa.net",
    ".mongodbgov.net",
)

# Tag keys Atlas puts on every replica set member.
ATLAS_TAG_KEYS = {"nodeType", "provider", "workloadType", "region"}

ATLAS_INTERNAL_DB = "__mdb_internal_atlas"


def detect(topo: Topology, seed_hosts: List[str]) -> Dict[str, Any]:
    evidence: List[str] = []
    hosts = list(seed_hosts) + [m.host for m in topo.all_members]
    suffix_hits = sorted({h for h in hosts if h.split(":")[0].lower().endswith(ATLAS_HOST_SUFFIXES)})
    if suffix_hits:
        evidence.append(f"hostname suffix ({suffix_hits[0]}{' …' if len(suffix_hits) > 1 else ''})")
    tagged = [m.host for m in topo.all_members if ATLAS_TAG_KEYS <= set(m.tags)]
    if tagged:
        evidence.append(f"Atlas member tags {sorted(ATLAS_TAG_KEYS)} on {len(tagged)} member(s)")
    if ATLAS_INTERNAL_DB in topo.database_names:
        evidence.append(f"database {ATLAS_INTERNAL_DB} present")
    return {"isAtlas": bool(evidence), "evidence": evidence}
