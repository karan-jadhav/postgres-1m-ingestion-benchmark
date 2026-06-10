from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class IngestKind(StrEnum):
    INSERT_AUTOCOMMIT = "insert_autocommit"
    INSERT_BATCHED = "insert_batched"
    COPY = "copy"


@dataclass(frozen=True)
class BenchmarkMethod:
    code: str
    label: str
    kind: IngestKind
    durability_note: str
    story_note: str
    story_order: int
    unlogged: bool = False
    tuned: bool = False
    server_side_copy: bool = False
    copy_freeze: bool = False
    raw_text: bool = False
    postgres_args: tuple[str, ...] = ()


LAB_FAST_POSTGRES_ARGS = (
    "-c",
    "fsync=off",
    "-c",
    "synchronous_commit=off",
    "-c",
    "full_page_writes=off",
    "-c",
    "checkpoint_timeout=30min",
    "-c",
    "max_wal_size=8GB",
    "-c",
    "shared_buffers=512MB",
)


INSERT_AUTOCOMMIT = BenchmarkMethod(
    code="insert_autocommit",
    label="Naive INSERT",
    kind=IngestKind.INSERT_AUTOCOMMIT,
    durability_note="logged, durable",
    story_note="baseline: one round trip and commit per row",
    story_order=10,
)

INSERT_BATCHED = BenchmarkMethod(
    code="insert_batched",
    label="Batched INSERT",
    kind=IngestKind.INSERT_BATCHED,
    durability_note="logged, durable",
    story_note="removed most client/server round trips",
    story_order=30,
)

COPY_NO_INDEXES = BenchmarkMethod(
    code="copy_no_indexes",
    label="Production COPY",
    kind=IngestKind.COPY,
    durability_note="logged, durable",
    story_note="used Postgres bulk-load path",
    story_order=40,
)

UNLOGGED_COPY = BenchmarkMethod(
    code="unlogged_copy_no_indexes",
    label="UNLOGGED COPY",
    kind=IngestKind.COPY,
    durability_note="non-durable, lab-only",
    story_note="removed WAL for table data",
    story_order=70,
    unlogged=True,
    tuned=True,
)

RAW_LANDING_COPY = BenchmarkMethod(
    code="raw_landing_copy",
    label="Raw landing COPY",
    kind=IngestKind.COPY,
    durability_note="unsafe raw text landing, lab-only",
    story_note="removed type conversion from hot ingest path",
    story_order=90,
    unlogged=True,
    tuned=True,
    server_side_copy=True,
    copy_freeze=True,
    raw_text=True,
    postgres_args=LAB_FAST_POSTGRES_ARGS,
)

SHOWCASE_METHODS = [
    INSERT_AUTOCOMMIT,
    INSERT_BATCHED,
    COPY_NO_INDEXES,
    UNLOGGED_COPY,
    RAW_LANDING_COPY,
]


def all_methods() -> list[BenchmarkMethod]:
    return list(SHOWCASE_METHODS)


def method_codes() -> list[str]:
    return [method.code for method in all_methods()]


def build_method_matrix(selected_codes: list[str] | None = None) -> list[BenchmarkMethod]:
    if selected_codes is None:
        return list(SHOWCASE_METHODS)

    available = {method.code: method for method in all_methods()}
    unknown = [code for code in selected_codes if code not in available]
    if unknown:
        allowed = ", ".join(available)
        raise ValueError(f"unknown method(s): {', '.join(unknown)}. Allowed values: {allowed}")

    return [available[code] for code in selected_codes]
