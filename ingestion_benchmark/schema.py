from __future__ import annotations

import csv
import sys
from dataclasses import dataclass
from pathlib import Path

from psycopg import sql


CSV_COLUMNS = [
    "event_id",
    "event_type",
    "actor_id",
    "actor_login",
    "repo_id",
    "repo_name",
    "created_at",
    "is_public",
    "payload",
]

MAX_POSTGRES_PARAMETERS = 65535


def raise_csv_field_limit() -> None:
    limit = sys.maxsize
    while True:
        try:
            csv.field_size_limit(limit)
            return
        except OverflowError:
            limit //= 10


@dataclass(frozen=True)
class DatasetInfo:
    path: Path
    row_count: int
    size_bytes: int
    columns: tuple[str, ...]


def create_table_sql(unlogged: bool = False, raw_text: bool = False) -> sql.Composed:
    table_kind = sql.SQL("UNLOGGED TABLE") if unlogged else sql.SQL("TABLE")
    if raw_text:
        return sql.SQL("""
CREATE {} events (
    event_id text NOT NULL,
    event_type text NOT NULL,
    actor_id text,
    actor_login text,
    repo_id text,
    repo_name text,
    created_at text NOT NULL,
    is_public text NOT NULL,
    payload text NOT NULL
);
""").format(table_kind)

    return sql.SQL("""
CREATE {} events (
    event_id text NOT NULL,
    event_type text NOT NULL,
    actor_id bigint,
    actor_login text,
    repo_id bigint,
    repo_name text,
    created_at timestamptz NOT NULL,
    is_public boolean NOT NULL,
    payload jsonb NOT NULL
);
""").format(table_kind)


def copy_sql() -> sql.Composed:
    columns = sql.SQL(", ").join(sql.Identifier(column) for column in CSV_COLUMNS)
    return sql.SQL("COPY events ({}) FROM STDIN WITH (FORMAT csv, HEADER true)").format(columns)


def server_copy_sql(container_path: str, freeze: bool = False) -> sql.Composed:
    columns = sql.SQL(", ").join(sql.Identifier(column) for column in CSV_COLUMNS)
    options = sql.SQL("FORMAT csv, HEADER true")
    if freeze:
        options = sql.SQL("FORMAT csv, HEADER true, FREEZE true")
    return sql.SQL("COPY events ({}) FROM {} WITH ({})").format(
        columns,
        sql.Literal(container_path),
        options,
    )


def insert_sql(row_count: int = 1) -> sql.Composed:
    columns = sql.SQL(", ").join(sql.Identifier(column) for column in CSV_COLUMNS)
    row_placeholder = sql.SQL("(") + sql.SQL(", ").join([sql.Placeholder()] * len(CSV_COLUMNS)) + sql.SQL(")")
    placeholders = sql.SQL(", ").join([row_placeholder] * row_count)
    return sql.SQL("INSERT INTO events ({}) VALUES {}").format(columns, placeholders)


def max_safe_insert_batch_size(requested_batch_size: int) -> int:
    if requested_batch_size < 1:
        raise ValueError("batch size must be at least 1")
    max_rows = MAX_POSTGRES_PARAMETERS // len(CSV_COLUMNS)
    return min(requested_batch_size, max_rows)


def validate_dataset(path: Path) -> DatasetInfo:
    if not path.exists():
        raise FileNotFoundError(f"dataset does not exist: {path}")
    if not path.is_file():
        raise ValueError(f"dataset path is not a file: {path}")

    raise_csv_field_limit()
    with path.open("r", newline="", encoding="utf-8") as file:
        reader = csv.reader(file)
        try:
            header = next(reader)
        except StopIteration as exc:
            raise ValueError("dataset is empty; expected a CSV header") from exc

        missing = [column for column in CSV_COLUMNS if column not in header]
        extra = [column for column in header if column not in CSV_COLUMNS]
        if missing or extra or header != CSV_COLUMNS:
            raise ValueError(
                "dataset header must exactly match: "
                + ", ".join(CSV_COLUMNS)
                + _header_details(missing, extra)
            )

        row_count = sum(1 for _ in reader)

    if row_count == 0:
        raise ValueError("dataset has a valid header but no rows")

    return DatasetInfo(
        path=path.resolve(),
        row_count=row_count,
        size_bytes=path.stat().st_size,
        columns=tuple(header),
    )


def iter_dataset_rows(path: Path):
    raise_csv_field_limit()
    with path.open("r", newline="", encoding="utf-8") as file:
        reader = csv.reader(file)
        next(reader)
        for row in reader:
            yield tuple(row)


def _header_details(missing: list[str], extra: list[str]) -> str:
    details = []
    if missing:
        details.append("missing: " + ", ".join(missing))
    if extra:
        details.append("extra: " + ", ".join(extra))
    if not details:
        details.append("columns are present but in the wrong order")
    return " (" + "; ".join(details) + ")"
