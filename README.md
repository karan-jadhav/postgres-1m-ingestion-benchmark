# PostgreSQL 18 ingestion benchmark

Question: how far can PostgreSQL ingest be pushed?

This repo loads real GitHub event data into PostgreSQL 18 through a sequence of
ingestion paths. Each step changes one major bottleneck:

- naive `INSERT`: baseline
- batched `INSERT`: fewer client/server round trips
- production `COPY`: Postgres bulk-load path
- `UNLOGGED COPY`: less WAL for table data
- raw landing `COPY`: all-text landing table to remove type conversion from the
  hot ingest path

The harness starts a fresh Docker volume for every method and every repetition.
That resets PostgreSQL data and WAL state. It does not fully clear the host OS
page cache, so the result files emphasize repeated runs and medians.

The benchmark runs one focused ingestion ladder and writes reproducible result files.

## Setup

Install Docker on the benchmark host, then install Python dependencies with uv:

```bash
uv sync
```

The default image is `postgres:18`.

## Dataset contract

The input must be a CSV file with this exact header:

```csv
event_id,event_type,actor_id,actor_login,repo_id,repo_name,created_at,is_public,payload
```

Column types loaded into PostgreSQL:

| Column | PostgreSQL type |
| --- | --- |
| `event_id` | `text` |
| `event_type` | `text` |
| `actor_id` | `bigint` |
| `actor_login` | `text` |
| `repo_id` | `bigint` |
| `repo_name` | `text` |
| `created_at` | `timestamptz` |
| `is_public` | `boolean` |
| `payload` | `jsonb` |

The final raw landing benchmark intentionally creates the same columns as
`text`. That isolates the cost of parsing `bigint`, `timestamptz`, `boolean`,
and `jsonb` during ingest. In a real pipeline, that pattern means landing first
and transforming into typed tables after the hot ingest path.

The CLI derives row count from the file, so the same command works for 1M, 5M,
10M, or larger datasets.

## Run

Short Makefile commands are available for the common workflow:

```bash
make download
make run
```

Override defaults with Make variables:

```bash
make download DATASET=data/gharchive_events_5m.csv ROWS=5000000
make run DATASET=data/gharchive_events_5m.csv RUNS=3
make run-methods METHODS=copy_no_indexes,raw_landing_copy RUNS=3
```

The raw `uv` commands are shown below for clarity.

Download real public GitHub Events data from GH Archive and convert it to the
benchmark CSV:

```bash
uv run python main.py download-gharchive data/gharchive_events_1m.csv --rows 1000000
```

The command caches raw hourly `.json.gz` files under `data/gharchive/raw/`.
The `data/` directory is ignored by git.
The converted CSV keeps core event metadata and a compact JSONB payload extracted
from the original GH Archive payload.

For a deterministic offline smoke dataset instead:

```bash
uv run python main.py generate-data data/events_1m.csv --rows 1000000
```

Then run the benchmark:

```bash
uv run python main.py run /path/to/events.csv --runs 3
```

Useful options:

```bash
uv run python main.py run /path/to/events.csv \
  --runs 3 \
  --methods copy_no_indexes,raw_landing_copy \
  --batch-size 7000 \
  --postgres-image postgres:18 \
  --output-dir results
```

To list method codes:

```bash
uv run python main.py list-methods
```

If a run is interrupted, remove leftover benchmark containers and volumes:

```bash
uv run python main.py cleanup
```

## Outputs

Generated files are written to `results/`:

- `raw.jsonl`: every individual method/repetition result
- `summary.csv`: chart/table source
- `summary.md`: Markdown summary table
- `environment.md`: command, dataset, Python, Docker, PostgreSQL, CPU, memory,
  EC2 metadata when available, and storage/filesystem details

## Full benchmark command

On any benchmark host with Docker installed:

```bash
uv run python main.py download-gharchive data/gharchive_events_1m.csv --rows 1000000
uv run python main.py run data/gharchive_events_1m.csv --runs 3
```

Successful runs verify the final row count for every method and clean up each
benchmark container and Docker volume after use.
