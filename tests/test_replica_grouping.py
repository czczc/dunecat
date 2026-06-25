"""_group_replicas: grouping Rucio's per-PFN map into site-ordered records.
The hub and web copies are identical; test both to keep them in sync."""

import pytest

from dunecat.hub.rucio import _group_replicas as hub_group
from dunecat.web.rucio import _group_replicas as web_group

# Rucio's `pfns` map: pfn -> {rse, type, priority, ...}. root ranks above davs.
_PFNS = {
    "root://fndca:1094/a": {"rse": "FNAL_DCACHE", "type": "TAPE", "priority": 2},
    "davs://fndca:2880/a": {"rse": "FNAL_DCACHE", "type": "TAPE", "priority": 4},
    "root://ccx:1094/a": {"rse": "CCIN2P3_DISK", "type": "DISK", "priority": 1},
    "davs://ccd:2880/a": {"rse": "CCIN2P3_DISK", "type": "DISK", "priority": 3},
}


@pytest.mark.parametrize("group", [hub_group, web_group])
def test_groups_by_site_disk_first_root_first(group):
    out = group(_PFNS)

    # One record per site, disk site before tape site.
    assert [s["rse"] for s in out] == ["CCIN2P3_DISK", "FNAL_DCACHE"]
    assert [s["type"] for s in out] == ["DISK", "TAPE"]

    # Within each site, doors ordered by Rucio priority → root before davs.
    for site in out:
        assert [p["scheme"] for p in site["pfns"]] == ["root", "davs"]

    # Each door carries scheme + pfn + priority.
    door = out[0]["pfns"][0]
    assert door == {"scheme": "root", "pfn": "root://ccx:1094/a", "priority": 1}


@pytest.mark.parametrize("group", [hub_group, web_group])
def test_empty_pfns(group):
    assert group({}) == []


@pytest.mark.parametrize("group", [hub_group, web_group])
def test_unknown_type_sorts_last_and_uppercases(group):
    out = group({"root://h/a": {"rse": "R", "type": "disk", "priority": 1}})
    assert out[0]["type"] == "DISK"
