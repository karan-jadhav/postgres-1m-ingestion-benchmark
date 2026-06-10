from __future__ import annotations

import csv
import json
import os
import platform
import shutil
import subprocess
import sys
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from statistics import median

from ingestion_benchmark.runner import RunResult
from ingestion_benchmark.schema import DatasetInfo


@dataclass(frozen=True)
class SummaryRow:
    method_code: str
    method_label: str
    runs: int
    row_count: int
    median_total_seconds: float
    median_ingest_seconds: float
    median_index_seconds: float
    median_rows_per_second: float
    speedup_vs_naive: float | None
    story_note: str
    story_order: int
    durability_note: str
    verified: bool
    errors: int


def summarize(results: list[RunResult]) -> list[SummaryRow]:
    groups: dict[str, list[RunResult]] = defaultdict(list)
    for result in results:
        groups[result.method_code].append(result)

    successful_groups = {
        code: [result for result in method_results if result.verified and result.error is None]
        for code, method_results in groups.items()
    }
    naive_ingest = _median_ingest(successful_groups.get("insert_autocommit", []))

    rows = []
    for code, method_results in groups.items():
        successes = successful_groups[code]
        sample = method_results[0]
        errors = len(method_results) - len(successes)
        if successes:
            median_total = median(result.total_seconds for result in successes)
            median_ingest = median(result.ingest_seconds for result in successes)
            speedup = naive_ingest / median_ingest if naive_ingest and median_ingest > 0 else None
            rows.append(
                SummaryRow(
                    method_code=code,
                    method_label=sample.method_label,
                    runs=len(successes),
                    row_count=sample.row_count,
                    median_total_seconds=median_total,
                    median_ingest_seconds=median_ingest,
                    median_index_seconds=median(result.index_seconds for result in successes),
                    median_rows_per_second=median(result.rows_per_second for result in successes),
                    speedup_vs_naive=speedup,
                    story_note=sample.story_note,
                    story_order=sample.story_order,
                    durability_note=sample.durability_note,
                    verified=True,
                    errors=errors,
                )
            )
        else:
            rows.append(
                SummaryRow(
                    method_code=code,
                    method_label=sample.method_label,
                    runs=0,
                    row_count=sample.row_count,
                    median_total_seconds=0.0,
                    median_ingest_seconds=0.0,
                    median_index_seconds=0.0,
                    median_rows_per_second=0.0,
                    speedup_vs_naive=None,
                    story_note=sample.story_note,
                    story_order=sample.story_order,
                    durability_note=sample.durability_note,
                    verified=False,
                    errors=errors,
                )
            )

    return sorted(rows, key=lambda row: (not row.verified, row.story_order))


def write_outputs(
    output_dir: Path,
    results: list[RunResult],
    dataset: DatasetInfo,
    command: list[str],
) -> list[Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary = summarize(results)

    paths = [
        output_dir / "raw.jsonl",
        output_dir / "summary.csv",
        output_dir / "summary.md",
        output_dir / "environment.md",
    ]

    _write_raw(paths[0], results)
    _write_summary_csv(paths[1], summary)
    _write_summary_markdown(paths[2], summary)
    _write_environment(paths[3], dataset, command, results)
    return paths


def render_markdown_table(rows: list[SummaryRow]) -> str:
    headers = [
        "Step",
        "Method",
        "Rows",
        "Ingest median",
        "Rows/sec",
        "Ingest speedup",
        "Change",
        "Bottleneck removed",
        "Note",
    ]
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]

    previous_ingest: float | None = None
    for rank, row in enumerate(rows, start=1):
        change = _change(previous_ingest, row.median_ingest_seconds)
        if row.median_ingest_seconds > 0:
            previous_ingest = row.median_ingest_seconds
        lines.append(
            "| "
            + " | ".join(
                [
                    str(rank),
                    row.method_label,
                    f"{row.row_count:,}",
                    _seconds(row.median_ingest_seconds),
                    f"{row.median_rows_per_second:,.0f}",
                    _speedup(row.speedup_vs_naive),
                    change,
                    row.story_note,
                    row.durability_note,
                ]
            )
            + " |"
        )
        if rank < len(rows):
            lines.append("| " + " | ".join([""] * len(headers)) + " |")
    return "\n".join(lines) + "\n"


def _write_raw(path: Path, results: list[RunResult]) -> None:
    with path.open("w", encoding="utf-8") as file:
        for result in results:
            file.write(json.dumps(result.to_dict(), sort_keys=True) + "\n")


def _write_summary_csv(path: Path, rows: list[SummaryRow]) -> None:
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(SummaryRow.__dataclass_fields__.keys()))
        writer.writeheader()
        for row in rows:
            writer.writerow(row.__dict__)


def _write_summary_markdown(path: Path, rows: list[SummaryRow]) -> None:
    content = "# Postgres ingestion benchmark summary\n\n"
    content += render_markdown_table(rows)
    content += (
        "\nMedian values only include successful, row-count-verified runs. "
        "Public tables use ingest time; Docker startup and cleanup remain in raw.jsonl.\n"
    )
    if any(row.method_code == "raw_landing_copy" and row.verified for row in rows):
        content += (
            "\nRaw landing loads the same CSV into all-text columns. It is included "
            "to isolate how much type parsing costs during the hot ingest path.\n"
        )
    path.write_text(content, encoding="utf-8")


def _write_environment(
    path: Path,
    dataset: DatasetInfo,
    command: list[str],
    results: list[RunResult],
) -> None:
    disk = shutil.disk_usage(dataset.path.parent)
    server_versions = sorted({result.server_version for result in results if result.server_version})
    content = "# Benchmark environment\n\n"
    content += f"- Command: `{' '.join(command)}`\n"
    content += f"- Dataset path: `{_display_path(dataset.path)}`\n"
    content += f"- Dataset rows: {dataset.row_count:,}\n"
    content += f"- Dataset size: {dataset.size_bytes:,} bytes\n"
    content += f"- Python: {sys.version.split()[0]}\n"
    content += f"- Platform: {platform.platform()}\n"
    content += f"- CPU count: {os.cpu_count()}\n"
    content += f"- Memory: {_memory_summary()}\n"
    content += f"- Dataset filesystem free space: {disk.free:,} bytes\n"
    content += f"- Docker: {_command_output(['docker', '--version'])}\n"
    content += f"- Postgres server version(s): {', '.join(server_versions) or 'unknown'}\n"
    path.write_text(content, encoding="utf-8")


def _median_ingest(results: list[RunResult]) -> float | None:
    if not results:
        return None
    return median(result.ingest_seconds for result in results)


def _seconds(value: float) -> str:
    if value >= 60:
        return f"{value / 60:.2f} min"
    return f"{value:.2f} s"


def _speedup(value: float | None) -> str:
    if value is None:
        return "-"
    return f"{value:.1f}x"


def _change(previous_total: float | None, current_total: float) -> str:
    if previous_total is None or current_total <= 0:
        return "baseline"
    improvement = ((previous_total / current_total) - 1) * 100
    if improvement >= 0:
        return f"+{improvement:.0f}% faster"
    return f"{improvement:.0f}% slower"


def _memory_summary() -> str:
    meminfo = Path("/proc/meminfo")
    if not meminfo.exists():
        return "unknown"
    for line in meminfo.read_text(encoding="utf-8").splitlines():
        if line.startswith("MemTotal:"):
            return line.split(":", 1)[1].strip()
    return "unknown"


def _display_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(Path.cwd().resolve()))
    except ValueError:
        return str(resolved)


def _command_output(command: list[str]) -> str:
    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        return result.stdout.strip()
    except Exception:
        return "unavailable"
