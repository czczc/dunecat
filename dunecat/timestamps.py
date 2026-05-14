import re
import sys
from collections.abc import Iterator
from datetime import UTC, date, datetime
from typing import Any

DEFAULT_REGEX = re.compile(r"(\d{8}T\d{6})")
DEFAULT_FORMAT = "%Y%m%dT%H%M%S"
DEFAULT_MAX_CANDIDATES = 10_000


def extract_run_time(
    filename: str,
    regex: re.Pattern[str] = DEFAULT_REGEX,
    fmt: str = DEFAULT_FORMAT,
) -> datetime | None:
    match = regex.search(filename)
    if match is None:
        return None
    try:
        raw = match.group(1)
    except IndexError:
        raw = match.group(0)
    try:
        return datetime.strptime(raw, fmt).replace(tzinfo=UTC)
    except ValueError:
        return None


def parse_date_range(value: str) -> tuple[date, date]:
    if ":" not in value:
        raise ValueError(f"--date-range expects 'FROM:TO', got {value!r}")
    from_s, to_s = value.split(":", 1)
    return (_parse_day(from_s.strip()), _parse_day(to_s.strip()))


def _parse_day(value: str) -> date:
    try:
        return datetime.strptime(value, "%Y-%m-%d").date()
    except ValueError as e:
        raise ValueError(f"Expected YYYY-MM-DD, got {value!r}") from e


def apply_date_range(
    stream: Iterator[dict[str, Any]],
    date_range: tuple[date, date],
    regex: re.Pattern[str] = DEFAULT_REGEX,
    fmt: str = DEFAULT_FORMAT,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
) -> Iterator[dict[str, Any]]:
    lo, hi = date_range
    seen = 0
    for item in stream:
        seen += 1
        _check_candidate_limit(seen, max_candidates)
        run_time = _extract_or_warn(item, regex, fmt)
        if run_time is None:
            continue
        if lo <= run_time.date() <= hi:
            yield item


def apply_one_per_day(
    stream: Iterator[dict[str, Any]],
    regex: re.Pattern[str] = DEFAULT_REGEX,
    fmt: str = DEFAULT_FORMAT,
    max_candidates: int = DEFAULT_MAX_CANDIDATES,
) -> Iterator[dict[str, Any]]:
    seen = 0
    seen_dates: set[date] = set()
    for item in stream:
        seen += 1
        _check_candidate_limit(seen, max_candidates)
        run_time = _extract_or_warn(item, regex, fmt)
        if run_time is None:
            continue
        d = run_time.date()
        if d in seen_dates:
            continue
        seen_dates.add(d)
        yield item


def _check_candidate_limit(seen: int, limit: int) -> None:
    if seen > limit:
        raise CandidateLimitExceeded(
            f"Exceeded --date-range-max-candidates ({limit}). "
            "Narrow with --runs, --run-range, --namespace, or --meta first."
        )


def _extract_or_warn(
    item: dict[str, Any], regex: re.Pattern[str], fmt: str
) -> datetime | None:
    name = item.get("name", "")
    run_time = extract_run_time(name, regex, fmt)
    if run_time is None:
        print(
            f"warning: no timestamp match in {item.get('namespace', '?')}:{name}",
            file=sys.stderr,
        )
    return run_time


class CandidateLimitExceeded(Exception):
    pass
