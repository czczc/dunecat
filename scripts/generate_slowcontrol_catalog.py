#!/usr/bin/env python3
"""Regenerate dunecat/data/slowcontrol_sensors.json.

The NP02/NP04 slow-control data APIs have no sensor-list endpoint: the
element-ID <-> name/label/unit mapping only exists inside the dashboard
web apps' AngularJS source. This script scrapes every page component
(JS) and template (HTML) of both apps and writes the merged catalog as
package data. Re-run it when the dashboards change.

Usage: uv run python scripts/generate_slowcontrol_catalog.py
"""

import html
import json
import re
import sys
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

APPS = {
    "np02": "https://np02-slow-control.app.cern.ch/",
    "np04": "https://np04-slow-control.app.cern.ch/",
}
OUT = Path(__file__).resolve().parent.parent / "dunecat" / "data" / "slowcontrol_sensors.json"

# Unicode KELVIN SIGN and DEGREE-prefixed variants seen in templates.
UNIT_NORMALIZE = {"K": "K", "°K": "K", "°C": "degC", "°": "deg"}


def get(base: str, path: str) -> str:
    try:
        with urllib.request.urlopen(base + path, timeout=20) as resp:
            return resp.read().decode("utf-8", "replace")
    except Exception as e:  # noqa: BLE001 - report and continue with other files
        print(f"warning: failed to fetch {base}{path}: {e}", file=sys.stderr)
        return ""


def strip_tags(txt: str) -> str:
    txt = re.sub(r"<[a-zA-Z][^>]*$", "", txt)  # trailing unclosed tag
    txt = html.unescape(re.sub(r"<[^>]+>", " ", txt))
    txt = re.sub(r"\{\{[^}]*\}\}", " ", txt)
    return re.sub(r"\s+", " ", txt).strip(" :>-")


def extract_label(context_before: str) -> str:
    """Pull a sensor-ish label (e.g. 'TE0516 (0.034m)') from the text
    immediately preceding a histogram link."""
    lbl = strip_tags(context_before)
    m = re.search(
        r"([A-Z]{1,4}\d{3,4}[A-Za-z0-9_]*\s*(?:\([^)]*\))?|\d{4}\s*\([^)]*\))$", lbl
    )
    return m.group(1).strip() if m else ""


def extract_unit(context_after: str) -> str:
    """Pull the unit that follows the value binding, e.g.
    '{{ ... | number: 1}} &#8490;</span>' -> 'K'."""
    m = re.search(r"\}\}\s*([^<{]{1,16})<", context_after)
    if not m:
        return ""
    unit = html.unescape(m.group(1)).strip()
    unit = UNIT_NORMALIZE.get(unit, unit)
    # Reject leftovers that are clearly not units (sentences, angular exprs).
    if not unit or len(unit) > 8 or " " in unit:
        return ""
    return unit


def scrape(base: str) -> list[dict]:
    index = get(base, "")
    js_files = [
        f
        for f in re.findall(r'src="([a-z0-9/._-]+\.js)(?:\?[^"]*)?"', index)
        if not f.startswith("dependencies")
    ]
    tmpl_files = sorted(
        {
            re.sub(r"\.component\.js$", ".template.html", f)
            for f in js_files
            if f.endswith(".component.js")
        }
    )
    rows: dict[str, dict] = {}

    def row(eid: str) -> dict:
        return rows.setdefault(
            eid, {"id": eid, "name": "", "label": "", "unit": "", "subsystems": set()}
        )

    for f in js_files:
        src = get(base, f)
        page = f.split("/")[0]
        for name, eid in re.findall(
            r'self\.([A-Za-z0-9_]+(?:\[\d+\])?)\s*=\s*res\["(\d+)"\]', src
        ):
            r = row(eid)
            r["subsystems"].add(page)
            if not r["name"]:
                r["name"] = name

    for f in tmpl_files:
        src = get(base, f)
        page = f.split("/")[0]
        for m in re.finditer(r"histogram/(\d+)", src):
            eid = m.group(1)
            r = row(eid)
            r["subsystems"].add(page)
            if not r["label"]:
                r["label"] = extract_label(src[max(0, m.start() - 260) : m.start()])
            if not r["unit"]:
                r["unit"] = extract_unit(src[m.end() : m.end() + 400])

    out = []
    for r in sorted(rows.values(), key=lambda r: (r["label"] or r["name"], r["id"])):
        r["subsystems"] = sorted(r["subsystems"])
        out.append(r)
    return out


def main() -> None:
    catalog = {
        "generated": datetime.now(UTC).strftime("%Y-%m-%d"),
        "detectors": {},
    }
    for det, base in APPS.items():
        sensors = scrape(base)
        named = sum(1 for s in sensors if s["name"] or s["label"])
        with_unit = sum(1 for s in sensors if s["unit"])
        print(f"{det}: {len(sensors)} sensors ({named} named, {with_unit} with unit)")
        catalog["detectors"][det] = {
            "api": f"https://{det}-data-api-slow-control.app.cern.ch",
            "sensors": sensors,
        }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(catalog, indent=1) + "\n")
    print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
