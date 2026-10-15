"""Command-line tools for creating and working with Tractatus research projects."""

from __future__ import annotations

import json
import os
import re
import shutil
import subprocess
from datetime import date
from pathlib import Path
from typing import Annotated

import boto3
import typer
from pyiceberg.catalog import load_catalog

from tractatus.catalog import CatalogConfig, Ledger, Publisher
from tractatus.catalog.discovery import _parse_parquet_path
from tractatus.config import TractatusConfig, config_template, default_config_path
from tractatus.logging import setup_logging
from tractatus.tickrake import TickrakeClient

app = typer.Typer(no_args_is_help=True, help="Tractatus research tools.")
init_app = typer.Typer(no_args_is_help=True, help="Create a research project.")
data_app = typer.Typer(no_args_is_help=True, help="Retrieve Tickrake data.")
config_app = typer.Typer(no_args_is_help=True, help="Manage Tractatus configuration.")
catalog_app = typer.Typer(no_args_is_help=True, help="Iceberg catalog operations.")
app.add_typer(init_app, name="init")
app.add_typer(data_app, name="data")
app.add_typer(config_app, name="config")
app.add_typer(catalog_app, name="catalog")

_PROJECT_FILES = {
    ".python-version": "3.11\n",
    ".gitignore": """.env\n.venv/\nmlruns/\nmlartifacts/\n.ipynb_checkpoints/\n.pytest_cache/\n.mypy_cache/\n.ruff_cache/\n__pycache__/\ndata/*\n!data/README.md\n""",
    ".env": """# Project-specific non-secret defaults. Exported environment variables override these.\nMLFLOW_TRACKING_URI=http://localhost:5000\nMLFLOW_EXPERIMENT_NAME=default\nAWS_PROFILE=default\nAWS_REGION=us-east-1\nS3_BUCKET=your-tickrake-bucket\nS3_REGION=us-east-1\nTICKRAKE_DATA_DIR=~/.tickrake/data\n""",
    ".env.example": """# MLflow server; use http://localhost:5000 for local research infrastructure\nMLFLOW_TRACKING_URI=http://localhost:5000\nMLFLOW_EXPERIMENT_NAME=my-first-study\n# AWS credentials are resolved by the normal AWS profile chain\nAWS_PROFILE=default\nAWS_REGION=us-east-1\nS3_BUCKET=your-tickrake-bucket\nS3_REGION=us-east-1\n# Tickrake's only cache. Do not point this at this project's data/ directory.\nTICKRAKE_DATA_DIR=/absolute/path/to/tickrake/data\n""",
    "data/README.md": """# Disposable study data\n\nUse this directory for disposable inputs and derived files. Tickrake market data remains exclusively in `TICKRAKE_DATA_DIR`; this is not a second Tickrake cache.\n""",
    "artifacts/.gitkeep": "",
    "research/__init__.py": "",
    "research/smoke_backtest.py": """\"\"\"A deliberately small reproducibility check for the research environment.\"\"\"\n\nfrom __future__ import annotations\n\nimport pandas as pd\n\n\ndef run(data: pd.DataFrame) -> dict[str, float]:\n    \"\"\"Return stable metrics from a historical option sample.\"\"\"\n    return {\"rows\": float(len(data)), \"columns\": float(len(data.columns))}\n""",
    "tests/test_smoke_backtest.py": """import pandas as pd\n\nfrom research.smoke_backtest import run\n\n\ndef test_smoke_backtest() -> None:\n    assert run(pd.DataFrame({\"price\": [1.0, 2.0]}))[\"rows\"] == 2.0\n""",
}


def _project_pyproject(name: str) -> str:
    return f'''[project]
name = "{name}"
version = "0.1.0"
description = "Reproducible trading research"
readme = "README.md"
requires-python = ">=3.11,<3.12"
dependencies = [
  "tractatus @ git+https://github.com/jwplatta/tractatus.git@4ce21b244e0907bcf7c738490bb2d14b5950dfaf",
  "numpy>=1.26", "pandas>=2.2", "scipy>=1.13", "pyarrow>=17.0",
  "scikit-learn>=1.5", "statsmodels>=0.14",
  "matplotlib>=3.9", "seaborn>=0.13", "jupyter>=1.1", "ipykernel>=6.29",
  "mlflow>=2.15", "python-dotenv>=1.0", "duckdb>=1.0", "optuna>=3.6",
  "tqdm>=4.66", "rich>=13.7",
]

[project.optional-dependencies]
torch = ["torch>=2.4"]

[dependency-groups]
dev = ["pytest>=8", "ruff>=0.6", "mypy>=1.11"]

[tool.pytest.ini_options]
testpaths = ["tests"]
'''


def _readme(name: str) -> str:
    return f"""# {name}\n\nA reproducible Tractatus research study.\n\n## Start\n\n```bash\nuv sync\ntractatus verify\ntractatus notebook\n```\n\nUse `uv sync --extra torch` only when this study needs PyTorch. Tickrake market data remains in `TICKRAKE_DATA_DIR`; `data/` is only for disposable study inputs and derived files.\n"""


@init_app.command("research")
def init_research(
    name: Annotated[str, typer.Option("--name", help="Project directory name.")] = "my-first-study",
) -> None:
    """Create a Git-initialized research repository and lock its dependencies."""
    if not name or Path(name).name != name or name in {".", ".."}:
        raise typer.BadParameter("name must be a single new directory name")
    target = Path.cwd() / name
    if target.exists():
        raise typer.BadParameter(f"target already exists: {target}")
    if shutil.which("uv") is None:
        raise typer.BadParameter("uv is required; install uv and run this command again")
    target.mkdir()
    try:
        for relative, content in _PROJECT_FILES.items():
            path = target / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(content)
        (target / "pyproject.toml").write_text(_project_pyproject(name))
        (target / "README.md").write_text(_readme(name))
        subprocess.run(["git", "init"], cwd=target, check=True, capture_output=True, text=True)
        subprocess.run(["uv", "lock"], cwd=target, check=True)
    except (OSError, subprocess.CalledProcessError) as exc:
        shutil.rmtree(target)
        detail = getattr(exc, "stderr", None) or str(exc)
        typer.echo(f"Could not initialize research project: {detail}", err=True)
        raise typer.Exit(1) from exc
    typer.echo(f"Created {target}. Next: cd {name} && uv sync")


@data_app.command("pull")
def data_pull_options(
    root: str,
    sample_date: str,
    provider: Annotated[str, typer.Option("--provider")] = "schwab",
    refresh_index: Annotated[bool, typer.Option("--refresh-index")] = False,
) -> None:
    """Fetch a published historical options parquet file into TICKRAKE_DATA_DIR."""
    try:
        parsed_date = date.fromisoformat(sample_date)
    except ValueError as exc:
        raise typer.BadParameter("DATE must use YYYY-MM-DD") from exc
    path = TickrakeClient().options_archive.get_parquet_path(
        root, parsed_date, provider=provider, refresh_index=refresh_index
    )
    if path is None:
        typer.echo(f"No published sample for {root} on {parsed_date.isoformat()}", err=True)
        raise typer.Exit(1)
    typer.echo(path)


@config_app.command("init")
def config_init(
    force: Annotated[
        bool, typer.Option("--force", help="Replace an existing config file.")
    ] = False,
) -> None:
    """Create a personal non-secret config template at ~/.tractatus/config.toml."""
    path = default_config_path()
    if path.exists() and not force:
        typer.echo(f"Config already exists: {path}. Use --force to replace it.", err=True)
        raise typer.Exit(1)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(config_template())
    typer.echo(f"Created {path}")


@config_app.command("show")
def config_show(
    effective: Annotated[bool, typer.Option("--effective", help="Show resolved settings.")] = False,
) -> None:
    """Show configuration without ever exposing credentials or tokens."""
    if not effective:
        raise typer.BadParameter("use --effective to display resolved configuration")
    typer.echo(json.dumps(TractatusConfig.load().as_dict(), indent=2, sort_keys=True))


@app.command()
def notebook() -> None:
    """Start JupyterLab from a generated research repository."""
    if not (Path.cwd() / ".venv").is_dir():
        raise typer.BadParameter("environment missing; run uv sync first")
    subprocess.run(["uv", "run", "jupyter", "lab"], check=True)


@app.command()
def verify() -> None:
    """Validate project configuration and run the packaged smoke backtest."""
    import mlflow  # type: ignore[import-not-found]  # noqa: PLC0415

    root = Path.cwd()
    if not (root / "research" / "smoke_backtest.py").exists():
        raise typer.BadParameter("run verify from a generated research project")
    config = TractatusConfig.load(project_dir=root)
    client = TickrakeClient()
    # The maintained fixture is intentionally configured by the research project/environment.
    fixture_root = os.environ.get("TRACTATUS_SMOKE_ROOT", "SPXW")
    fixture_date = date.fromisoformat(os.environ.get("TRACTATUS_SMOKE_DATE", "2026-01-02"))
    parquet = client.options_archive.get_parquet_path(fixture_root, fixture_date)
    if parquet is None:
        typer.echo(
            "Packaged smoke dataset is unavailable from the configured Tickrake archive", err=True
        )
        raise typer.Exit(1)
    import pandas as pd  # noqa: PLC0415
    from research.smoke_backtest import run  # type: ignore[import-not-found]  # noqa: PLC0415

    metrics = run(pd.read_parquet(parquet))
    mlflow.set_tracking_uri(config.mlflow_tracking_uri)
    mlflow.set_experiment(config.mlflow_experiment_name)
    with mlflow.start_run() as active_run:
        mlflow.log_params(
            {"root": fixture_root, "sample_date": fixture_date.isoformat(), "path": str(parquet)}
        )
        mlflow.log_metrics(metrics)
        artifact = root / "artifacts" / "smoke_metrics.json"
        artifact.write_text(__import__("json").dumps(metrics, indent=2))
        mlflow.log_artifact(str(artifact))
        typer.echo(f"MLflow run: {active_run.info.run_id}")


@catalog_app.command("publish")
def catalog_publish(
    table: Annotated[str, typer.Option("--table", help="Iceberg table to publish.")] = "options",
    dry_run: Annotated[
        bool, typer.Option("--dry-run", help="Discover only, skip upload and commit.")
    ] = False,
    provider: Annotated[
        list[str] | None, typer.Option("--provider", help="Limit to specific providers.")
    ] = None,
    limit: Annotated[
        int | None, typer.Option("--limit", help="Max files to upload and commit.")
    ] = None,
    after: Annotated[
        str | None, typer.Option("--after", help="Only include sample_date >= DATE (YYYY-MM-DD).")
    ] = None,
    before: Annotated[
        str | None, typer.Option("--before", help="Only include sample_date < DATE (YYYY-MM-DD).")
    ] = None,
) -> None:
    """Discover, upload, and commit parquet files to an Iceberg table."""
    setup_logging("catalog", verbose=True)

    parsed_after = date.fromisoformat(after) if after else None
    parsed_before = date.fromisoformat(before) if before else None

    config = CatalogConfig.load()
    publisher = Publisher(config)
    try:
        result = publisher.run(
            table_name=table,
            dry_run=dry_run,
            providers=provider,
            limit=limit,
            after=parsed_after,
            before=parsed_before,
        )
    finally:
        publisher.close()

    typer.echo(f"Table:      {table}")
    typer.echo(f"Discovered: {result.discovered}")
    typer.echo(f"Uploaded:   {result.uploaded}")
    typer.echo(f"Committed:  {result.committed}")
    typer.echo(f"Errors:     {result.errors}")
    for detail in result.details:
        typer.echo(detail)


@catalog_app.command("status")
def catalog_status(
    table: Annotated[
        str | None, typer.Option("--table", help="Filter to a specific table.")
    ] = None,
) -> None:
    """Show publication ledger summary."""
    config = CatalogConfig.load()
    ledger = Ledger(config.database_url)
    try:
        summary = ledger.summary(table_name=table)
    finally:
        ledger.close()

    if not summary:
        typer.echo("Ledger is empty.")
        return

    if table:
        typer.echo(f"Table: {table}")
    for status, count in sorted(summary.items()):
        typer.echo(f"  {status}: {count}")


@catalog_app.command("sync-ledger")
def catalog_sync_ledger(
    table: Annotated[str, typer.Option("--table", help="Iceberg table to sync.")],
) -> None:
    """Sync the ledger with files already committed in the Iceberg table.

    Scans the Iceberg table for registered data files and records them
    as 'committed' in the ledger. Use this to seed a new ledger (e.g.
    postgres) from an existing Iceberg table so the publisher doesn't
    try to re-publish files that are already committed.
    """
    config = CatalogConfig.load()

    props: dict[str, str] = {
        "type": "glue",
        "glue.region": config.aws_region,
        "s3.region": config.aws_region,
        "warehouse": config.iceberg_warehouse,
    }
    if config.aws_profile:
        props["glue.profile-name"] = config.aws_profile
        props["s3.profile-name"] = config.aws_profile

    catalog = load_catalog("glue", **props)
    full_name = f"{config.glue_database}.{table}"

    try:
        iceberg_table = catalog.load_table(full_name)
    except Exception:
        typer.echo(f"Iceberg table {full_name} not found.", err=True)
        raise typer.Exit(1) from None

    # Extract S3 paths from the Iceberg table
    prefix = f"s3://{config.iceberg_bucket}/{config.glue_database}/{table}/"
    tasks = iceberg_table.scan().plan_files()
    synced = 0
    skipped = 0

    ledger = Ledger(config.database_url)
    try:
        for task in tasks:
            s3_path = task.file.file_path
            if not s3_path.startswith(prefix):
                skipped += 1
                continue

            relative_path = s3_path[len(prefix) :]
            s3_key = f"{config.glue_database}/{table}/{relative_path}"

            # Parse provider/root/sample_date from the relative path
            parsed = _parse_parquet_path(Path(relative_path), Path("."))
            if parsed is None:
                # Try parsing with just the filename parts
                parts = relative_path.split("/")
                if len(parts) >= 5:
                    provider = parts[0]
                    match = re.match(r"^(.+)_samples_(\d{4}-\d{2}-\d{2})\.parquet$", parts[-1])
                    if match:
                        root = match.group(1)
                        sample_date = match.group(2)
                    else:
                        skipped += 1
                        continue
                else:
                    skipped += 1
                    continue
            else:
                provider, root, sample_date_obj = parsed
                sample_date = sample_date_obj.isoformat()

            existing = ledger.get(relative_path, table)
            if existing and existing.status == "committed":
                skipped += 1
                continue

            ledger.record_discovered(
                path=relative_path,
                table_name=table,
                sha256="",
                size_bytes=task.file.file_size_in_bytes,
                provider=provider,
                root=root,
                sample_date=sample_date,
            )
            ledger.mark_uploaded(relative_path, table, s3_key)
            ledger.mark_committed([relative_path], table)
            synced += 1

        typer.echo(f"Synced {synced} files as committed. Skipped {skipped}.")
    finally:
        ledger.close()


@catalog_app.command("reset")
def catalog_reset(
    table: Annotated[str, typer.Option("--table", help="Iceberg table to reset.")],
    delete_s3: Annotated[
        bool, typer.Option("--delete-s3", help="Also delete uploaded files from S3.")
    ] = False,
    drop_iceberg: Annotated[
        bool, typer.Option("--drop-iceberg", help="Also drop the Iceberg table from Glue.")
    ] = False,
    after: Annotated[
        str | None, typer.Option("--after", help="Only reset sample_date >= DATE (YYYY-MM-DD).")
    ] = None,
    before: Annotated[
        str | None, typer.Option("--before", help="Only reset sample_date < DATE (YYYY-MM-DD).")
    ] = None,
    provider: Annotated[
        str | None, typer.Option("--provider", help="Only reset a specific provider.")
    ] = None,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt.")] = False,
) -> None:
    """Reset the ledger and optionally clean up S3/Iceberg for a table.

    Use --after/--before/--provider for partial resets. Without filters,
    resets everything for the table.
    """
    is_filtered = any([after, before, provider])

    if drop_iceberg and is_filtered:
        typer.echo("Cannot use --drop-iceberg with date/provider filters.", err=True)
        raise typer.Exit(1)

    config = CatalogConfig.load()
    ledger = Ledger(config.database_url)

    try:
        filter_desc = []
        if after:
            filter_desc.append(f"after={after}")
        if before:
            filter_desc.append(f"before={before}")
        if provider:
            filter_desc.append(f"provider={provider}")
        scope = f" ({', '.join(filter_desc)})" if filter_desc else ""

        parts = ["ledger records"]
        if delete_s3:
            parts.append("S3 data")
        if drop_iceberg:
            parts.append("Iceberg table")
        msg = f"This will delete {', '.join(parts)} for table {table!r}{scope}. Continue?"
        if not yes and not typer.confirm(msg, default=False):
            raise typer.Abort()

        if delete_s3:
            session_kwargs: dict[str, str] = {"region_name": config.aws_region}
            if config.aws_profile:
                session_kwargs["profile_name"] = config.aws_profile
            s3 = boto3.Session(**session_kwargs).client("s3")  # type: ignore[arg-type]

            if is_filtered:
                s3_keys = ledger.get_s3_keys(table, after=after, before=before, provider=provider)
                deleted = 0
                # Delete in batches of 1000 (S3 limit)
                for i in range(0, len(s3_keys), 1000):
                    batch = [{"Key": k} for k in s3_keys[i : i + 1000]]
                    s3.delete_objects(
                        Bucket=config.iceberg_bucket,
                        Delete={"Objects": batch},  # type: ignore[typeddict-item]
                    )
                    deleted += len(batch)
                typer.echo(f"Deleted {deleted} S3 objects.")
            else:
                prefix = f"{config.glue_database}/{table}/"
                typer.echo(f"Deleting s3://{config.iceberg_bucket}/{prefix}*")
                paginator = s3.get_paginator("list_objects_v2")
                deleted = 0
                for page in paginator.paginate(Bucket=config.iceberg_bucket, Prefix=prefix):
                    objects = [{"Key": obj["Key"]} for obj in page.get("Contents", [])]
                    if objects:
                        s3.delete_objects(
                            Bucket=config.iceberg_bucket,
                            Delete={"Objects": objects},  # type: ignore[typeddict-item]
                        )
                        deleted += len(objects)
                typer.echo(f"Deleted {deleted} S3 objects.")

        if drop_iceberg:
            props: dict[str, str] = {
                "type": "glue",
                "glue.region": config.aws_region,
                "s3.region": config.aws_region,
                "warehouse": config.iceberg_warehouse,
            }
            if config.aws_profile:
                props["glue.profile-name"] = config.aws_profile
                props["s3.profile-name"] = config.aws_profile
            catalog = load_catalog("glue", **props)
            full_name = f"{config.glue_database}.{table}"
            try:
                catalog.drop_table(full_name)
                typer.echo(f"Dropped Iceberg table {full_name}.")
            except Exception:
                typer.echo(f"Iceberg table {full_name} not found, skipping.")

        count = ledger.delete_filtered(table, after=after, before=before, provider=provider)
        typer.echo(f"Deleted {count} ledger records for table {table!r}{scope}.")
    finally:
        ledger.close()
