from __future__ import annotations

import subprocess
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

import psycopg
from psycopg import sql


POSTGRES_USER = "postgres"
POSTGRES_PASSWORD = "postgres"
POSTGRES_DB = "benchmark"
BENCHMARK_LABEL = "postgres-ingestion-benchmark"
BENCHMARK_LABEL_VALUE = "true"
CONTAINER_PREFIX = "pg-ingest-bench-"
VOLUME_PREFIX = "pg-ingest-bench-data-"
POSTGRES_VOLUME_TARGET = "/var/lib/postgresql"
BENCHMARK_INPUT_PATH = "/tmp/benchmark-input.csv"


@dataclass(frozen=True)
class DockerConfig:
    postgres_image: str = "postgres:18"
    startup_timeout_seconds: int = 60
    dataset_path: Path | None = None
    postgres_args: tuple[str, ...] = ()


@dataclass(frozen=True)
class PostgresTarget:
    host: str
    port: int
    user: str = POSTGRES_USER
    password: str = POSTGRES_PASSWORD
    dbname: str = POSTGRES_DB

    @property
    def dsn(self) -> str:
        return (
            f"host={self.host} port={self.port} dbname={self.dbname} "
            f"user={self.user} password={self.password}"
        )


@dataclass(frozen=True)
class DockerResources:
    container_name: str
    volume_name: str
    target: PostgresTarget


def docker_volume_create_command(volume_name: str) -> list[str]:
    return [
        "docker",
        "volume",
        "create",
        "--label",
        f"{BENCHMARK_LABEL}={BENCHMARK_LABEL_VALUE}",
        volume_name,
    ]


def docker_run_command(
    container_name: str,
    volume_name: str,
    image: str,
    dataset_path: Path | None = None,
    postgres_args: tuple[str, ...] = (),
) -> list[str]:
    command = [
        "docker",
        "run",
        "--detach",
        "--name",
        container_name,
        "--label",
        f"{BENCHMARK_LABEL}={BENCHMARK_LABEL_VALUE}",
        "--env",
        f"POSTGRES_USER={POSTGRES_USER}",
        "--env",
        f"POSTGRES_PASSWORD={POSTGRES_PASSWORD}",
        "--env",
        f"POSTGRES_DB={POSTGRES_DB}",
        "--publish",
        "127.0.0.1::5432",
        "--mount",
        f"source={volume_name},target={POSTGRES_VOLUME_TARGET}",
    ]
    if dataset_path is not None:
        command.extend(
            [
                "--mount",
                f"type=bind,source={dataset_path.resolve()},target={BENCHMARK_INPUT_PATH},readonly",
            ]
        )
    command.append(image)
    command.extend(postgres_args)
    return command


def docker_port_command(container_name: str) -> list[str]:
    return ["docker", "port", container_name, "5432/tcp"]


def docker_remove_container_command(container_name: str) -> list[str]:
    return ["docker", "rm", "--force", container_name]


def docker_remove_volume_command(volume_name: str) -> list[str]:
    return ["docker", "volume", "rm", "--force", volume_name]


def docker_image_inspect_command(image: str) -> list[str]:
    return ["docker", "image", "inspect", image]


def docker_pull_command(image: str) -> list[str]:
    return ["docker", "pull", image]


def docker_list_benchmark_containers_command() -> list[str]:
    return [
        "docker",
        "ps",
        "--all",
        "--filter",
        f"label={BENCHMARK_LABEL}={BENCHMARK_LABEL_VALUE}",
        "--format",
        "{{.Names}}",
    ]


def docker_list_benchmark_volumes_command() -> list[str]:
    return [
        "docker",
        "volume",
        "ls",
        "--filter",
        f"label={BENCHMARK_LABEL}={BENCHMARK_LABEL_VALUE}",
        "--format",
        "{{.Name}}",
    ]


def docker_list_all_containers_command() -> list[str]:
    return ["docker", "ps", "--all", "--format", "{{.Names}}"]


def docker_list_all_volumes_command() -> list[str]:
    return ["docker", "volume", "ls", "--format", "{{.Name}}"]


def ensure_image(image: str, progress: Callable[[str], None] | None = None) -> None:
    if _run(docker_image_inspect_command(image), check=False).returncode == 0:
        if progress:
            progress(f"Docker image already present: {image}")
        return

    if progress:
        progress(f"Pulling Docker image: {image}")
    _stream(docker_pull_command(image), progress=progress)


def start_postgres(config: DockerConfig) -> DockerResources:
    suffix = uuid.uuid4().hex[:12]
    container_name = f"{CONTAINER_PREFIX}{suffix}"
    volume_name = f"{VOLUME_PREFIX}{suffix}"

    _run(docker_volume_create_command(volume_name))
    try:
        _run(
            docker_run_command(
                container_name,
                volume_name,
                config.postgres_image,
                dataset_path=config.dataset_path,
                postgres_args=config.postgres_args,
            )
        )
        target = PostgresTarget(host="127.0.0.1", port=_published_port(container_name))
        _wait_for_postgres(target, config.startup_timeout_seconds)
        return DockerResources(container_name, volume_name, target)
    except Exception:
        cleanup_resources(container_name, volume_name)
        raise


def cleanup(resources: DockerResources) -> None:
    cleanup_resources(resources.container_name, resources.volume_name)


def cleanup_resources(container_name: str, volume_name: str) -> None:
    _run(docker_remove_container_command(container_name), check=False)
    _run(docker_remove_volume_command(volume_name), check=False)


def cleanup_labeled_resources(progress: Callable[[str], None] | None = None) -> tuple[int, int]:
    containers = _unique(
        _names_from(docker_list_benchmark_containers_command())
        + [
            name
            for name in _names_from(docker_list_all_containers_command())
            if name.startswith(CONTAINER_PREFIX)
        ]
    )
    volumes = _unique(
        _names_from(docker_list_benchmark_volumes_command())
        + [name for name in _names_from(docker_list_all_volumes_command()) if name.startswith(VOLUME_PREFIX)]
    )

    for container_name in containers:
        if progress:
            progress(f"Removing container: {container_name}")
        _run(docker_remove_container_command(container_name), check=False)

    for volume_name in volumes:
        if progress:
            progress(f"Removing volume: {volume_name}")
        _run(docker_remove_volume_command(volume_name), check=False)

    return len(containers), len(volumes)


def _published_port(container_name: str) -> int:
    result = _run(docker_port_command(container_name))
    output = result.stdout.strip()
    if not output:
        raise RuntimeError(f"docker did not publish a Postgres port for {container_name}")

    endpoint = output.splitlines()[0].rsplit(":", 1)[-1]
    return int(endpoint)


def _wait_for_postgres(target: PostgresTarget, timeout_seconds: int) -> None:
    deadline = time.monotonic() + timeout_seconds
    last_error: Exception | None = None

    while time.monotonic() < deadline:
        try:
            with psycopg.connect(target.dsn, connect_timeout=2) as conn:
                with conn.cursor() as cur:
                    cur.execute(sql.SQL("SELECT 1"))
                    cur.fetchone()
                return
        except Exception as exc:  # pragma: no cover - depends on Docker startup timing
            last_error = exc
            time.sleep(0.5)

    raise TimeoutError(f"Postgres did not become ready within {timeout_seconds}s: {last_error}")


def _run(command: list[str], check: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(command, check=check, capture_output=True, text=True)


def _stream(command: list[str], progress: Callable[[str], None] | None = None) -> None:
    process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    assert process.stdout is not None
    for line in process.stdout:
        message = line.strip()
        if message and progress:
            progress(message)
    return_code = process.wait()
    if return_code != 0:
        raise subprocess.CalledProcessError(return_code, command)


def _names_from(command: list[str]) -> list[str]:
    result = _run(command, check=False)
    if result.returncode != 0:
        return []
    return [line.strip() for line in result.stdout.splitlines() if line.strip()]


def _unique(values: list[str]) -> list[str]:
    return list(dict.fromkeys(values))
