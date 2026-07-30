"""Query the NP02/NP04 slow-control (DCS) data APIs at CERN.

The dashboards at https://np{02,04}-slow-control.app.cern.ch are backed by
a public, unauthenticated JSON API:

    GET {api}/np{02,04}histogram/{elemId}/{start}/{end}
        -> {"<epoch_ms>": value, ...}   raw archive, ~3.5 s cadence
    GET {api}/np{02,04}histogram_average/{elemId}/{start}/{end}
        -> same shape, server-side averaged (fails on some old ranges)

Timestamps in the URL are UTC ``YYYY-MM-DDTHH:MM:SS``.

The API has no sensor-list endpoint, so the element-ID <-> name/label/unit
catalog is scraped from the dashboard apps' source and bundled as package
data (``data/slowcontrol_sensors.json``). Regenerate it with
``scripts/generate_slowcontrol_catalog.py``.
"""

import json
from dataclasses import dataclass
from datetime import datetime
from functools import cache
from importlib import resources

import requests

from .errors import DunecatError

DETECTORS = ("np02", "np04")
_TIMEOUT = 60  # the proxy at CERN times out at ~30 s; leave headroom


class SensorNotFoundError(DunecatError):
    pass


class AmbiguousSensorError(DunecatError):
    def __init__(self, query: str, candidates: list["Sensor"]):
        self.candidates = candidates
        lines = "\n".join(
            f"  {s.id}  {s.name or '-'}  {s.label or '-'}" for s in candidates[:15]
        )
        more = "" if len(candidates) <= 15 else f"\n  ... and {len(candidates) - 15} more"
        super().__init__(
            f"'{query}' matches {len(candidates)} sensors; be more specific:\n"
            f"{lines}{more}"
        )


class SlowControlAPIError(DunecatError):
    pass


@dataclass(frozen=True)
class Sensor:
    id: str
    name: str
    label: str
    unit: str
    subsystems: tuple[str, ...]

    @property
    def display_name(self) -> str:
        return self.label or self.name or self.id


@cache
def _catalog() -> dict:
    path = resources.files("dunecat").joinpath("data/slowcontrol_sensors.json")
    return json.loads(path.read_text())


def api_base(detector: str) -> str:
    return _detector_entry(detector)["api"]


def _detector_entry(detector: str) -> dict:
    try:
        return _catalog()["detectors"][detector]
    except KeyError:
        raise DunecatError(
            f"Unknown detector '{detector}'. Choose from: {', '.join(DETECTORS)}."
        )


@cache
def list_sensors(detector: str) -> tuple[Sensor, ...]:
    return tuple(
        Sensor(
            id=s["id"],
            name=s["name"],
            label=s["label"],
            unit=s["unit"],
            subsystems=tuple(s["subsystems"]),
        )
        for s in _detector_entry(detector)["sensors"]
    )


def subsystems(detector: str) -> dict[str, int]:
    """Subsystem (dashboard page) -> sensor count."""
    counts: dict[str, int] = {}
    for s in list_sensors(detector):
        for sub in s.subsystems:
            counts[sub] = counts.get(sub, 0) + 1
    return dict(sorted(counts.items()))


def resolve_sensor(detector: str, query: str) -> Sensor:
    """Resolve a user-supplied sensor reference to a unique catalog entry.

    Matches, in order: exact element ID; exact name or label
    (case-insensitive, the label's leading token counts too, so 'TE0516'
    finds 'TE0516 (0.034m)'); unique substring of name or label.
    """
    sensors = list_sensors(detector)
    q = query.strip().lower()
    for s in sensors:
        if s.id == q:
            return s
    exact = [
        s
        for s in sensors
        if q in (s.name.lower(), s.label.lower(), s.label.split(" ")[0].lower())
    ]
    if len(exact) == 1:
        return exact[0]
    if len(exact) > 1:
        raise AmbiguousSensorError(query, exact)
    partial = [s for s in sensors if q in s.name.lower() or q in s.label.lower()]
    if len(partial) == 1:
        return partial[0]
    if len(partial) > 1:
        raise AmbiguousSensorError(query, partial)
    raise SensorNotFoundError(
        f"No {detector} sensor matches '{query}'. "
        f"Browse the catalog with: dunecat slowcontrol sensors --detector {detector}"
    )


def parse_when(value: str, *, end_of_day: bool = False) -> str:
    """Normalize 'YYYY-MM-DD' or 'YYYY-MM-DDTHH:MM:SS' (UTC) to the API's
    timestamp format. A bare date means midnight, or 23:59:59 when it is
    the end of a range."""
    value = value.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(value, fmt).strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            pass
    try:
        day = datetime.strptime(value, "%Y-%m-%d")
    except ValueError:
        raise DunecatError(
            f"Cannot parse date '{value}'. Use YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS (UTC)."
        )
    suffix = "T23:59:59" if end_of_day else "T00:00:00"
    return day.strftime("%Y-%m-%d") + suffix


def fetch_history(
    detector: str,
    sensor_id: str,
    start: str,
    end: str,
    *,
    average: bool = False,
) -> dict[int, float]:
    """Fetch the archived timeseries; returns {epoch_ms: value} sorted by time."""
    endpoint = f"{detector}histogram_average" if average else f"{detector}histogram"
    url = f"{api_base(detector)}/{endpoint}/{sensor_id}/{start}/{end}"
    try:
        resp = requests.get(url, timeout=_TIMEOUT)
    except requests.RequestException as e:
        raise SlowControlAPIError(f"slow-control API request failed: {e}")
    if resp.status_code == 504:
        raise SlowControlAPIError(
            f"The {detector} slow-control backend is not responding "
            "(HTTP 504 from CERN). This is a server-side outage of its "
            "database connection; try again later."
        )
    if not resp.ok:
        raise SlowControlAPIError(
            f"slow-control API returned HTTP {resp.status_code} for {url}"
        )
    try:
        data = resp.json()
    except ValueError:
        raise SlowControlAPIError(f"slow-control API returned non-JSON for {url}")
    return {int(k): v for k, v in sorted(data.items(), key=lambda kv: int(kv[0]))}
