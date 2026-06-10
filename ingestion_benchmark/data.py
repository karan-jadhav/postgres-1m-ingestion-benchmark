from __future__ import annotations

import csv
import gzip
import json
import random
import shutil
import urllib.request
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable

from ingestion_benchmark.schema import CSV_COLUMNS


GH_EVENT_TYPES = [
    "PushEvent",
    "PullRequestEvent",
    "IssuesEvent",
    "CreateEvent",
    "WatchEvent",
    "ForkEvent",
    "IssueCommentEvent",
]


@dataclass(frozen=True)
class GhArchiveDatasetResult:
    output_path: Path
    rows_written: int
    raw_files: tuple[Path, ...]
    first_hour: str
    last_hour: str


def generate_dataset(path: Path, rows: int, seed: int = 42) -> None:
    """Generate local GH-like data when a network dataset is not needed."""
    if rows < 1:
        raise ValueError("rows must be at least 1")

    path.parent.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    base_time = datetime(2026, 1, 1, tzinfo=UTC)

    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(CSV_COLUMNS)

        for event_id in range(1, rows + 1):
            repo_id = rng.randint(1, max(rows // 20, 1))
            actor_id = rng.randint(1, max(rows // 5, 1))
            created_at = base_time + timedelta(seconds=event_id)
            event_type = rng.choice(GH_EVENT_TYPES)
            payload = {
                "action": rng.choice(["opened", "closed", "created", "pushed", "started"]),
                "size": rng.randint(1, 20),
                "distinct_size": rng.randint(1, 10),
                "ref": f"refs/heads/branch-{rng.randint(1, 50)}",
            }

            writer.writerow(
                [
                    str(event_id),
                    event_type,
                    actor_id,
                    f"user-{actor_id}",
                    repo_id,
                    f"org-{repo_id % 500}/repo-{repo_id}",
                    created_at.isoformat(),
                    "true",
                    json.dumps(payload, separators=(",", ":")),
                ]
            )


def build_gharchive_dataset(
    output_path: Path,
    rows: int,
    start_hour: str,
    raw_dir: Path,
    max_hours: int = 240,
    progress: Callable[[str], None] | None = None,
) -> GhArchiveDatasetResult:
    if rows < 1:
        raise ValueError("rows must be at least 1")
    if max_hours < 1:
        raise ValueError("max_hours must be at least 1")

    output_path.parent.mkdir(parents=True, exist_ok=True)
    raw_dir.mkdir(parents=True, exist_ok=True)
    temp_output_path = output_path.with_suffix(output_path.suffix + ".tmp")

    current_hour = parse_gharchive_hour(start_hour)
    rows_written = 0
    raw_files: list[Path] = []
    first_hour = format_gharchive_hour(current_hour)
    last_hour = first_hour

    try:
        with temp_output_path.open("w", newline="", encoding="utf-8") as output_file:
            writer = csv.writer(output_file)
            writer.writerow(CSV_COLUMNS)

            for _ in range(max_hours):
                hour_label = format_gharchive_hour(current_hour)
                raw_path = raw_dir / f"{hour_label}.json.gz"
                url = f"https://data.gharchive.org/{hour_label}.json.gz"
                if progress:
                    progress(f"Using {url}")
                download_file(url, raw_path)
                raw_files.append(raw_path)
                last_hour = hour_label

                rows_written += write_gharchive_rows(raw_path, writer, rows - rows_written)
                if rows_written >= rows:
                    break

                current_hour += timedelta(hours=1)

        if rows_written < rows:
            raise RuntimeError(
                f"only wrote {rows_written:,} rows after {max_hours} hour files; "
                "increase --max-hours or choose a busier start hour"
            )
        temp_output_path.replace(output_path)
    except Exception:
        temp_output_path.unlink(missing_ok=True)
        raise

    return GhArchiveDatasetResult(
        output_path=output_path,
        rows_written=rows_written,
        raw_files=tuple(raw_files),
        first_hour=first_hour,
        last_hour=last_hour,
    )


def parse_gharchive_hour(value: str) -> datetime:
    try:
        date_part, hour_part = value.rsplit("-", 1)
        year, month, day = (int(part) for part in date_part.split("-"))
        hour = int(hour_part)
        if hour < 0 or hour > 23:
            raise ValueError
        return datetime(year, month, day, hour, tzinfo=UTC)
    except ValueError as exc:
        raise ValueError("hour must look like YYYY-MM-DD-H, for example 2025-01-01-15") from exc


def format_gharchive_hour(value: datetime) -> str:
    return f"{value:%Y-%m-%d}-{value.hour}"


def download_file(url: str, output_path: Path) -> None:
    if output_path.exists() and output_path.stat().st_size > 0:
        return

    temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    try:
        request = urllib.request.Request(
            url,
            headers={"User-Agent": "postgres-ingestion-benchmark/0.1"},
        )
        with urllib.request.urlopen(request, timeout=120) as response:
            with temp_path.open("wb") as file:
                shutil.copyfileobj(response, file)
        temp_path.replace(output_path)
    except Exception as exc:
        temp_path.unlink(missing_ok=True)
        raise RuntimeError(f"failed to download {url}: {exc}") from exc


def write_gharchive_rows(raw_path: Path, writer: Any, remaining_rows: int) -> int:
    written = 0
    with gzip.open(raw_path, "rt", encoding="utf-8") as file:
        for line in file:
            if written >= remaining_rows:
                break
            event = json.loads(line)
            row = gharchive_event_to_row(event)
            if row is None:
                continue
            writer.writerow(row)
            written += 1
    return written


def gharchive_event_to_row(event: dict) -> list[str] | None:
    event_id = event.get("id")
    event_type = event.get("type")
    created_at = event.get("created_at")
    if not event_id or not event_type or not created_at:
        return None

    actor = event.get("actor") or {}
    repo = event.get("repo") or {}
    payload = event.get("payload") or {}
    if not isinstance(payload, dict):
        payload = {}

    return [
        str(event_id),
        str(event_type),
        _optional_int(actor.get("id")),
        str(actor.get("login") or ""),
        _optional_int(repo.get("id")),
        str(repo.get("name") or ""),
        str(created_at),
        "true" if bool(event.get("public")) else "false",
        json.dumps(compact_payload(event_type=str(event_type), payload=payload), separators=(",", ":"), sort_keys=True),
    ]


def compact_payload(event_type: str, payload: dict[str, Any]) -> dict[str, Any]:
    compact: dict[str, Any] = {"event_type": event_type}
    _copy_keys(
        payload,
        compact,
        [
            "action",
            "ref",
            "ref_type",
            "master_branch",
            "description",
            "pusher_type",
            "push_id",
            "size",
            "distinct_size",
            "number",
        ],
    )

    for nested_key in ["issue", "pull_request", "release", "comment"]:
        nested = payload.get(nested_key)
        if isinstance(nested, dict):
            compact[f"{nested_key}_id"] = nested.get("id")
            compact[f"{nested_key}_number"] = nested.get("number")
            compact[f"{nested_key}_state"] = nested.get("state")

    commits = payload.get("commits")
    if isinstance(commits, list):
        compact["commit_count"] = len(commits)

    return {key: value for key, value in compact.items() if value is not None}


def _copy_keys(source: dict[str, Any], target: dict[str, Any], keys: list[str]) -> None:
    for key in keys:
        if source.get(key) is not None:
            target[key] = source[key]


def _optional_int(value: object) -> str:
    if value is None or value == "":
        return ""
    if not isinstance(value, int | str):
        raise ValueError(f"expected integer-like value, got {type(value).__name__}")
    return str(int(value))
