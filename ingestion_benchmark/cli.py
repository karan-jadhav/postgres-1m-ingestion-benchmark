from __future__ import annotations

import sys
from pathlib import Path
from typing import Annotated

import typer
from rich.console import Console
from rich.panel import Panel
from rich.table import Table

from ingestion_benchmark.data import build_gharchive_dataset, generate_dataset
from ingestion_benchmark import docker
from ingestion_benchmark.methods import build_method_matrix, method_codes
from ingestion_benchmark.reporting import summarize
from ingestion_benchmark.reporting import write_outputs
from ingestion_benchmark.runner import BenchmarkOptions, ProgressEvent, run_benchmark
from ingestion_benchmark.schema import CSV_COLUMNS, max_safe_insert_batch_size, validate_dataset


app = typer.Typer(
    add_completion=False,
    help="Benchmark PostgreSQL 18 CSV ingestion methods with fresh Docker volumes.",
)
console = Console()


@app.command()
def run(
    dataset_path: Annotated[
        Path,
        typer.Argument(
            exists=True,
            file_okay=True,
            dir_okay=False,
            readable=True,
            help="CSV dataset path. Header must match the fixed events schema.",
        ),
    ],
    runs: Annotated[int, typer.Option("--runs", min=1, help="Repetitions per method.")] = 3,
    methods: Annotated[
        str | None,
        typer.Option(
            "--methods",
            help=(
                "Comma-separated method codes to run. Defaults to the showcase benchmark ladder. "
                "Use `list-methods` to see codes."
            ),
        ),
    ] = None,
    batch_size: Annotated[
        int,
        typer.Option("--batch-size", min=1, help="Requested rows per multi-row INSERT batch."),
    ] = 7_000,
    postgres_image: Annotated[
        str,
        typer.Option("--postgres-image", help="Docker image to use for each fresh Postgres run."),
    ] = "postgres:18",
    output_dir: Annotated[
        Path,
        typer.Option("--output-dir", file_okay=False, dir_okay=True, help="Directory for result files."),
    ] = Path("results"),
) -> None:
    """Run the benchmark against one CSV dataset."""
    with console.status("Validating dataset and counting rows"):
        dataset = validate_dataset(dataset_path)
    selected_codes = parse_method_codes(methods)
    try:
        benchmark_methods = build_method_matrix(selected_codes=selected_codes)
    except ValueError as exc:
        raise typer.BadParameter(str(exc)) from exc
    if not benchmark_methods:
        raise typer.BadParameter("no benchmark methods selected")

    effective_batch_size = max_safe_insert_batch_size(batch_size)
    if effective_batch_size != batch_size:
        console.print(
            "Requested batch size exceeds PostgreSQL's bind-parameter limit for "
            f"{len(CSV_COLUMNS)} columns; using {effective_batch_size:,} rows per batch.",
            style="yellow",
        )

    console.print(
        Panel.fit(
            f"[bold]Dataset[/bold]: {dataset.path}\n"
            f"[bold]Rows[/bold]: {dataset.row_count:,}\n"
            f"[bold]Postgres image[/bold]: {postgres_image}\n"
            f"[bold]Runs per method[/bold]: {runs}",
            title="Benchmark",
        )
    )
    console.print("[bold]Methods[/bold]")
    for method in benchmark_methods:
        console.print(f"- {method.label} [{method.durability_note}]")

    docker.ensure_image(postgres_image, progress=docker_progress_message)

    progress_renderer = ProgressRenderer()
    options = BenchmarkOptions(
        runs=runs,
        batch_size=batch_size,
        postgres_image=postgres_image,
        progress=progress_renderer.handle,
    )
    results = run_benchmark(dataset, benchmark_methods, options)
    failed = [result for result in results if result.error]
    paths = write_outputs(output_dir, results, dataset, sys.argv)
    print_summary_table(results)

    console.print("\n[bold]Wrote[/bold]")
    for path in paths:
        console.print(f"- {path}")

    if failed:
        console.print(f"\n[red]{len(failed)} run(s) failed. See {output_dir / 'raw.jsonl'} for details.[/red]")
        raise typer.Exit(code=1)


@app.command("list-methods")
def list_methods(
) -> None:
    """Print method codes accepted by --methods."""
    for code in method_codes():
        console.print(code)


def parse_method_codes(value: str | None) -> list[str] | None:
    if value is None:
        return None
    codes = [code.strip() for code in value.split(",") if code.strip()]
    if not codes:
        raise typer.BadParameter("--methods must contain at least one method code")
    return codes


@app.command()
def cleanup() -> None:
    """Remove leftover benchmark containers and Docker volumes."""
    containers, volumes = docker.cleanup_labeled_resources(progress=docker_progress_message)
    console.print(f"Removed {containers} container(s) and {volumes} volume(s).")


@app.command("schema")
def print_schema() -> None:
    """Print the required CSV header and column meanings."""
    console.print(",".join(CSV_COLUMNS))
    console.print("\n[bold]Column contract[/bold]")
    console.print("- event_id: GitHub event id")
    console.print("- event_type: GitHub event type, for example PushEvent")
    console.print("- actor_id: GitHub actor id")
    console.print("- actor_login: GitHub actor login")
    console.print("- repo_id: GitHub repository id")
    console.print("- repo_name: owner/repository name")
    console.print("- created_at: event timestamp")
    console.print("- is_public: true/false")
    console.print("- payload: event payload JSON")


@app.command("generate-data")
def generate_data(
    output_path: Annotated[
        Path,
        typer.Argument(help="Where to write the generated CSV dataset."),
    ],
    rows: Annotated[int, typer.Option("--rows", min=1, help="Number of event rows to generate.")] = 1_000_000,
    seed: Annotated[int, typer.Option("--seed", help="Random seed for repeatable data.")] = 42,
) -> None:
    """Generate a deterministic GH-like CSV dataset for offline smoke runs."""
    with console.status(f"Generating {rows:,} rows"):
        generate_dataset(output_path, rows=rows, seed=seed)
    console.print(f"Wrote {rows:,} rows to {output_path}")


@app.command("download-gharchive")
def download_gharchive(
    output_path: Annotated[
        Path,
        typer.Argument(help="Where to write the converted benchmark CSV."),
    ] = Path("data/gharchive_events.csv"),
    rows: Annotated[int, typer.Option("--rows", min=1, help="Number of GitHub events to write.")] = 1_000_000,
    start_hour: Annotated[
        str,
        typer.Option(
            "--start-hour",
            help="First GH Archive hour to download, formatted like 2025-01-01-15.",
        ),
    ] = "2025-01-01-0",
    raw_dir: Annotated[
        Path,
        typer.Option("--raw-dir", file_okay=False, dir_okay=True, help="Cache directory for raw .json.gz files."),
    ] = Path("data/gharchive/raw"),
    max_hours: Annotated[
        int,
        typer.Option("--max-hours", min=1, help="Maximum hourly files to scan before failing."),
    ] = 240,
) -> None:
    """Download GH Archive hourly JSON files and convert them into benchmark CSV."""
    result = build_gharchive_dataset(
        output_path=output_path,
        rows=rows,
        start_hour=start_hour,
        raw_dir=raw_dir,
        max_hours=max_hours,
        progress=docker_progress_message,
    )
    console.print(
        f"Wrote {result.rows_written:,} rows to {result.output_path} "
        f"from {result.first_hour} through {result.last_hour}."
    )
    console.print(f"Cached {len(result.raw_files)} raw GH Archive file(s) under {raw_dir}.")


def docker_progress_message(message: str) -> None:
    console.print(f"[dim]{message}[/dim]")


class ProgressRenderer:
    def __init__(self) -> None:
        self.phase_rows: list[tuple[str, float, str]] = []

    def handle(self, event: ProgressEvent) -> None:
        event_type = event.get("type")
        if event_type == "run_start":
            self.phase_rows = []
            console.rule(
                f"[bold]Run {event['repetition']}/{event['runs']}[/bold] "
                f"{event['method_label']} [dim]({event['method_code']})[/dim]"
            )
        elif event_type == "phase_start":
            console.print(f"  [cyan]start[/cyan] {event['phase']}")
        elif event_type == "phase_done":
            detail = ""
            if "port" in event:
                detail = f"localhost:{event['port']}"
            self.phase_rows.append((str(event["phase"]), float(event["seconds"]), detail))
            suffix = f" [dim]{detail}[/dim]" if detail else ""
            console.print(f"  [green]done [/green] {event['phase']} {seconds(float(event['seconds']))}{suffix}")
        elif event_type == "run_done":
            self.print_run_table(event)
        elif event_type == "run_failed":
            console.print(
                f"  [red]failed[/red] {event['method_code']} after "
                f"{seconds(float(event.get('total_seconds', 0.0)))}: {event.get('error', '')}"
            )
        elif event_type == "cleanup_start":
            console.print("  [cyan]start[/cyan] Cleanup")
        elif event_type == "cleanup_done":
            console.print(f"  [green]done [/green] Cleanup {seconds(float(event['seconds']))}")

    def print_run_table(self, event: ProgressEvent) -> None:
        table = Table(show_header=True, header_style="bold", title="Run Timing")
        table.add_column("Phase")
        table.add_column("Time", justify="right")
        table.add_column("Detail")
        for phase, elapsed, detail in self.phase_rows:
            table.add_row(phase, seconds(elapsed), detail)
        table.add_section()
        table.add_row("Total", seconds(float(event["total_seconds"])), "")
        table.add_row("Ingest rows/sec", f"{float(event['rows_per_second']):,.0f}", "")
        console.print(table)


def print_summary_table(results) -> None:
    table = Table(title="How far can Postgres ingest be pushed?")
    table.add_column("Step", justify="right")
    table.add_column("Method")
    table.add_column("Runs", justify="right")
    table.add_column("Ingest median", justify="right")
    table.add_column("Rows/sec", justify="right")
    table.add_column("Ingest speedup", justify="right")
    table.add_column("Change")
    table.add_column("Bottleneck removed")
    table.add_column("Note")

    previous_ingest: float | None = None
    summary_rows = summarize(results)
    for rank, row in enumerate(summary_rows, start=1):
        if previous_ingest is None or row.median_ingest_seconds <= 0:
            change = "baseline"
        else:
            improvement = ((previous_ingest / row.median_ingest_seconds) - 1) * 100
            change = f"+{improvement:.0f}% faster" if improvement >= 0 else f"{improvement:.0f}% slower"
        previous_ingest = row.median_ingest_seconds if row.median_ingest_seconds > 0 else previous_ingest
        table.add_row(
            str(rank),
            row.method_label,
            str(row.runs),
            seconds(row.median_ingest_seconds),
            f"{row.median_rows_per_second:,.0f}",
            "-" if row.speedup_vs_naive is None else f"{row.speedup_vs_naive:.1f}x",
            change,
            row.story_note,
            row.durability_note,
        )
        if rank < len(summary_rows):
            table.add_section()

    console.print(table)


def seconds(value: float) -> str:
    if value >= 60:
        return f"{value / 60:.2f} min"
    return f"{value:.2f} s"
