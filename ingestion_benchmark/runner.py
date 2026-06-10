from __future__ import annotations

import time
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Callable, Literal, TypedDict

import psycopg
from psycopg import sql

from ingestion_benchmark import docker
from ingestion_benchmark.methods import BenchmarkMethod, IngestKind
from ingestion_benchmark.schema import (
    DatasetInfo,
    copy_sql,
    create_table_sql,
    insert_sql,
    iter_dataset_rows,
    max_safe_insert_batch_size,
    server_copy_sql,
)


class ProgressEvent(TypedDict, total=False):
    type: Literal["run_start", "phase_start", "phase_done", "run_done", "run_failed", "cleanup_start", "cleanup_done"]
    repetition: int
    runs: int
    method_code: str
    method_label: str
    phase: str
    seconds: float
    total_seconds: float
    ingest_seconds: float
    index_seconds: float
    schema_seconds: float
    rows_per_second: float
    port: int
    error: str


@dataclass(frozen=True)
class BenchmarkOptions:
    runs: int
    batch_size: int
    postgres_image: str
    progress: Callable[[ProgressEvent], None] | None = None


@dataclass
class RunResult:
    method_code: str
    method_label: str
    repetition: int
    row_count: int
    dataset_path: str
    dataset_size_bytes: int
    postgres_image: str
    durability_note: str
    story_note: str
    story_order: int
    schema_seconds: float
    ingest_seconds: float
    index_seconds: float
    total_seconds: float
    rows_per_second: float
    verified: bool
    server_version: str
    batch_size_requested: int | None = None
    batch_size_effective: int | None = None
    error: str | None = None

    def to_dict(self) -> dict:
        return asdict(self)


def run_benchmark(
    dataset: DatasetInfo,
    methods: list[BenchmarkMethod],
    options: BenchmarkOptions,
) -> list[RunResult]:
    if options.runs < 1:
        raise ValueError("runs must be at least 1")

    all_results: list[RunResult] = []
    for repetition in range(1, options.runs + 1):
        for method in methods:
            all_results.append(run_one_method(dataset, method, options, repetition))

    return all_results


def run_one_method(
    dataset: DatasetInfo,
    method: BenchmarkMethod,
    options: BenchmarkOptions,
    repetition: int,
) -> RunResult:
    started_at = time.perf_counter()
    schema_seconds = 0.0
    ingest_seconds = 0.0
    index_seconds = 0.0
    server_version = "unknown"
    resources: docker.DockerResources | None = None

    try:
        _emit(
            options,
            {
                "type": "run_start",
                "repetition": repetition,
                "runs": options.runs,
                "method_code": method.code,
                "method_label": method.label,
            },
        )
        phase_start = time.perf_counter()
        _phase_start(options, repetition, method, "Start PostgreSQL")
        resources = docker.start_postgres(
            docker.DockerConfig(
                postgres_image=options.postgres_image,
                dataset_path=dataset.path if method.server_side_copy else None,
                postgres_args=method.postgres_args,
            )
        )
        _phase_done(
            options,
            repetition,
            method,
            "Start PostgreSQL",
            phase_start,
            {"port": resources.target.port},
        )
        with psycopg.connect(resources.target.dsn) as conn:
            server_version = _server_version(conn)
            conn.commit()

            _phase_start(options, repetition, method, "Apply schema")
            schema_start = time.perf_counter()
            _apply_schema(conn, method.unlogged, method.raw_text, commit=not method.copy_freeze)
            if method.tuned:
                _apply_tuned_session_settings(conn)
            schema_seconds = time.perf_counter() - schema_start
            _phase_done(options, repetition, method, "Apply schema", schema_start)

            _phase_start(options, repetition, method, "Ingest dataset")
            ingest_start = time.perf_counter()
            _run_ingest(conn, dataset.path, method, options.batch_size)
            ingest_seconds = time.perf_counter() - ingest_start
            _phase_done(options, repetition, method, "Ingest dataset", ingest_start)

            _phase_start(options, repetition, method, "Verify row count")
            verify_start = time.perf_counter()
            verified_count = _row_count(conn)
            verified = verified_count == dataset.row_count
            if not verified:
                raise RuntimeError(
                    f"row count mismatch for {method.code}: expected "
                    f"{dataset.row_count}, got {verified_count}"
                )
            _phase_done(options, repetition, method, "Verify row count", verify_start)

        total_seconds = time.perf_counter() - started_at
        _emit(
            options,
            {
                "type": "run_done",
                "repetition": repetition,
                "runs": options.runs,
                "method_code": method.code,
                "method_label": method.label,
                "schema_seconds": schema_seconds,
                "ingest_seconds": ingest_seconds,
                "index_seconds": index_seconds,
                "total_seconds": total_seconds,
                "rows_per_second": dataset.row_count / ingest_seconds if ingest_seconds > 0 else 0.0,
            },
        )
        effective_batch = (
            max_safe_insert_batch_size(options.batch_size)
            if method.kind == IngestKind.INSERT_BATCHED
            else None
        )
        return RunResult(
            method_code=method.code,
            method_label=method.label,
            repetition=repetition,
            row_count=dataset.row_count,
            dataset_path=_display_path(dataset.path),
            dataset_size_bytes=dataset.size_bytes,
            postgres_image=options.postgres_image,
            durability_note=method.durability_note,
            story_note=method.story_note,
            story_order=method.story_order,
            schema_seconds=schema_seconds,
            ingest_seconds=ingest_seconds,
            index_seconds=index_seconds,
            total_seconds=total_seconds,
            rows_per_second=dataset.row_count / ingest_seconds if ingest_seconds > 0 else 0.0,
            verified=True,
            server_version=server_version,
            batch_size_requested=options.batch_size if method.kind == IngestKind.INSERT_BATCHED else None,
            batch_size_effective=effective_batch,
        )
    except Exception as exc:
        total_seconds = time.perf_counter() - started_at
        _emit(
            options,
            {
                "type": "run_failed",
                "repetition": repetition,
                "runs": options.runs,
                "method_code": method.code,
                "method_label": method.label,
                "total_seconds": total_seconds,
                "error": str(exc),
            },
        )
        return RunResult(
            method_code=method.code,
            method_label=method.label,
            repetition=repetition,
            row_count=dataset.row_count,
            dataset_path=_display_path(dataset.path),
            dataset_size_bytes=dataset.size_bytes,
            postgres_image=options.postgres_image,
            durability_note=method.durability_note,
            story_note=method.story_note,
            story_order=method.story_order,
            schema_seconds=schema_seconds,
            ingest_seconds=ingest_seconds,
            index_seconds=index_seconds,
            total_seconds=total_seconds,
            rows_per_second=0.0,
            verified=False,
            server_version=server_version,
            batch_size_requested=options.batch_size if method.kind == IngestKind.INSERT_BATCHED else None,
            batch_size_effective=(
                max_safe_insert_batch_size(options.batch_size)
                if method.kind == IngestKind.INSERT_BATCHED
                else None
            ),
            error=str(exc),
        )
    finally:
        if resources is not None:
            _emit(
                options,
                {
                    "type": "cleanup_start",
                    "repetition": repetition,
                    "runs": options.runs,
                    "method_code": method.code,
                    "method_label": method.label,
                },
            )
            cleanup_start = time.perf_counter()
            docker.cleanup(resources)
            _emit(
                options,
                {
                    "type": "cleanup_done",
                    "repetition": repetition,
                    "runs": options.runs,
                    "method_code": method.code,
                    "method_label": method.label,
                    "seconds": time.perf_counter() - cleanup_start,
                },
            )


def _apply_schema(
    conn: psycopg.Connection,
    unlogged: bool,
    raw_text: bool,
    commit: bool = True,
) -> None:
    with conn.cursor() as cur:
        cur.execute(create_table_sql(unlogged, raw_text=raw_text))
    if commit:
        conn.commit()


def _apply_tuned_session_settings(conn: psycopg.Connection) -> None:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SET synchronous_commit TO off"))
        cur.execute(sql.SQL("SET maintenance_work_mem TO '1GB'"))


def _run_ingest(
    conn: psycopg.Connection,
    dataset_path: Path,
    method: BenchmarkMethod,
    requested_batch_size: int,
) -> None:
    if method.kind == IngestKind.INSERT_AUTOCOMMIT:
        _insert_autocommit(conn, dataset_path)
    elif method.kind == IngestKind.INSERT_BATCHED:
        _insert_batched(conn, dataset_path, requested_batch_size)
    elif method.kind == IngestKind.COPY:
        if method.server_side_copy:
            _server_side_copy(conn, method.copy_freeze)
        else:
            _copy_from_csv(conn, dataset_path)
    else:  # pragma: no cover - guarded by enum exhaustiveness
        raise ValueError(f"unsupported ingest method: {method.kind}")


def _insert_autocommit(conn: psycopg.Connection, dataset_path: Path) -> None:
    conn.autocommit = True
    with conn.cursor() as cur:
        statement = insert_sql()
        for row in iter_dataset_rows(dataset_path):
            cur.execute(statement, row)
    conn.autocommit = False


def _insert_batched(conn: psycopg.Connection, dataset_path: Path, requested_batch_size: int) -> None:
    batch_size = max_safe_insert_batch_size(requested_batch_size)
    batch: list[tuple[str, ...]] = []
    with conn.cursor() as cur:
        for row in iter_dataset_rows(dataset_path):
            batch.append(row)
            if len(batch) >= batch_size:
                _execute_insert_batch(cur, batch)
                batch.clear()

        if batch:
            _execute_insert_batch(cur, batch)
    conn.commit()


def _execute_insert_batch(cur: psycopg.Cursor, batch: list[tuple[str, ...]]) -> None:
    params = [value for row in batch for value in row]
    cur.execute(insert_sql(len(batch)), params)


def _copy_from_csv(conn: psycopg.Connection, dataset_path: Path) -> None:
    with conn.cursor() as cur:
        with cur.copy(copy_sql()) as copy:
            with dataset_path.open("r", encoding="utf-8") as file:
                for chunk in iter(lambda: file.read(1024 * 1024), ""):
                    copy.write(chunk)
    conn.commit()


def _server_side_copy(conn: psycopg.Connection, freeze: bool) -> None:
    with conn.cursor() as cur:
        cur.execute(server_copy_sql(docker.BENCHMARK_INPUT_PATH, freeze=freeze))
    conn.commit()


def _row_count(conn: psycopg.Connection) -> int:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SELECT count(*) FROM events"))
        result = cur.fetchone()
    if result is None:
        raise RuntimeError("Postgres returned no row for count(*)")
    return int(result[0])


def _server_version(conn: psycopg.Connection) -> str:
    with conn.cursor() as cur:
        cur.execute(sql.SQL("SHOW server_version"))
        result = cur.fetchone()
    if result is None:
        raise RuntimeError("Postgres returned no row for server_version")
    return str(result[0])


def _display_path(path: Path) -> str:
    resolved = path.resolve()
    try:
        return str(resolved.relative_to(Path.cwd().resolve()))
    except ValueError:
        return str(resolved)


def _phase_start(options: BenchmarkOptions, repetition: int, method: BenchmarkMethod, phase: str) -> None:
    _emit(
        options,
        {
            "type": "phase_start",
            "repetition": repetition,
            "runs": options.runs,
            "method_code": method.code,
            "method_label": method.label,
            "phase": phase,
        },
    )


def _phase_done(
    options: BenchmarkOptions,
    repetition: int,
    method: BenchmarkMethod,
    phase: str,
    started_at: float,
    extra: ProgressEvent | None = None,
) -> None:
    event: ProgressEvent = {
        "type": "phase_done",
        "repetition": repetition,
        "runs": options.runs,
        "method_code": method.code,
        "method_label": method.label,
        "phase": phase,
        "seconds": time.perf_counter() - started_at,
    }
    if extra:
        event.update(extra)
    _emit(options, event)


def _emit(options: BenchmarkOptions, event: ProgressEvent) -> None:
    if options.progress:
        options.progress(event)
