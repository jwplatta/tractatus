"""Command-line tools for creating and working with Tractatus research projects."""

from __future__ import annotations

import contextlib
import json
import os
import re
import shutil
import subprocess
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Annotated

import boto3
import typer
from dotenv import load_dotenv
from pyiceberg.exceptions import NoSuchTableError
from pyiceberg.expressions import AlwaysTrue

from tractatus.catalog import CatalogConfig, Ledger, Publisher
from tractatus.catalog.discovery import _parse_options
from tractatus.config import TractatusConfig, config_template, default_config_path
from tractatus.logging import setup_logging
from tractatus.reader import Reader

load_dotenv()

app = typer.Typer(no_args_is_help=True, help="Tractatus research tools.")


@app.callback()
def main(
    env_file: Annotated[
        Path | None,
        typer.Option(
            "--env-file", help="Path to a .env file to load.", envvar="TRACTATUS_ENV_FILE"
        ),
    ] = None,
) -> None:
    if env_file is not None:
        load_dotenv(dotenv_path=env_file, override=True)


init_app = typer.Typer(no_args_is_help=True, help="Create a research project.")
data_app = typer.Typer(no_args_is_help=True, help="Retrieve Tickrake data.")
config_app = typer.Typer(no_args_is_help=True, help="Manage Tractatus configuration.")
catalog_app = typer.Typer(no_args_is_help=True, help="Iceberg catalog operations.")
cache_app = typer.Typer(no_args_is_help=False, help="Local cache management.")
app_cmd = typer.Typer(no_args_is_help=True, help="Local web apps.")
app.add_typer(init_app, name="init")
app.add_typer(data_app, name="data")
app.add_typer(config_app, name="config")
app.add_typer(catalog_app, name="catalog")
app.add_typer(app_cmd, name="app")
catalog_app.add_typer(cache_app, name="cache")

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
    try:
        path = Reader().get_option_parquet(root, parsed_date, provider=provider)
    except FileNotFoundError:
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
    reader = Reader()
    # The maintained fixture is intentionally configured by the research project/environment.
    fixture_root = os.environ.get("TRACTATUS_SMOKE_ROOT", "SPXW")
    fixture_date = date.fromisoformat(os.environ.get("TRACTATUS_SMOKE_DATE", "2026-01-02"))
    try:
        parquet = reader.get_option_parquet(fixture_root, fixture_date)
    except FileNotFoundError:
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
    provider: Annotated[
        list[str] | None, typer.Option("--provider", help="Limit to specific providers.")
    ] = None,
    limit: Annotated[int | None, typer.Option("--limit", help="Max groups to commit.")] = None,
    after: Annotated[
        str | None, typer.Option("--after", help="Only include sample_date >= DATE (YYYY-MM-DD).")
    ] = None,
    before: Annotated[
        str | None, typer.Option("--before", help="Only include sample_date < DATE (YYYY-MM-DD).")
    ] = None,
    cache: Annotated[
        bool,
        typer.Option(
            "--cache/--no-cache", help="Write parquet artifacts to MinIO cache after publish."
        ),
    ] = False,
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Show progress bars.")] = False,
) -> None:
    """Normalize and commit discovered files to an Iceberg table.

    Run ``tractatus catalog discover`` first to scan MinIO and populate the ledger.
    This command only processes files already recorded as ``discovered`` in the ledger.
    """
    setup_logging("catalog", verbose=True)

    parsed_after = date.fromisoformat(after) if after else None
    parsed_before = date.fromisoformat(before) if before else None

    config = CatalogConfig.load()
    publisher = Publisher(config, verbose=verbose)
    try:
        result = publisher.run(
            table_name=table,
            skip_discover=True,
            providers=provider,
            limit=limit,
            after=parsed_after,
            before=parsed_before,
            cache=cache,
        )
    finally:
        publisher.close()

    typer.echo(f"Table:      {table}")
    typer.echo(f"Committed:  {result.committed}")
    typer.echo(f"Cached:     {result.cached}")
    typer.echo(f"Errors:     {result.errors}")
    for detail in result.details:
        typer.echo(detail)


@catalog_app.command("discover")
def catalog_discover(
    table: Annotated[str, typer.Option("--table", help="Iceberg table to discover.")] = "options",
    provider: Annotated[
        list[str] | None, typer.Option("--provider", help="Limit to specific providers.")
    ] = None,
    after: Annotated[
        str | None, typer.Option("--after", help="Only include sample_date >= DATE (YYYY-MM-DD).")
    ] = None,
    before: Annotated[
        str | None, typer.Option("--before", help="Only include sample_date < DATE (YYYY-MM-DD).")
    ] = None,
    verbose: Annotated[bool, typer.Option("--verbose", "-v", help="Show progress bars.")] = False,
) -> None:
    """Scan MinIO for data files and record them in the ledger.

    Run this once per date range before ``tractatus catalog publish``.
    Already-known files are skipped (upsert by path).
    """
    setup_logging("catalog", verbose=True)

    parsed_after = date.fromisoformat(after) if after else None
    parsed_before = date.fromisoformat(before) if before else None

    config = CatalogConfig.load()
    publisher = Publisher(config, verbose=verbose)
    try:
        discovered = publisher._discover(
            table_name=table,
            providers=provider,
            after=parsed_after,
            before=parsed_before,
        )
    finally:
        publisher.close()

    typer.echo(f"Table:      {table}")
    typer.echo(f"Discovered: {discovered}")


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

    publisher = Publisher(config)
    try:
        catalog = publisher._iceberg_catalog()
    finally:
        publisher.close()

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

            # Parse provider/root/sample_date from the relative path
            parsed = _parse_options(relative_path)
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
                provider, root, sample_date_obj, _metadata = parsed
                sample_date = sample_date_obj.isoformat()

            existing = ledger.get(relative_path, table)
            if existing and existing.status == "committed":
                skipped += 1
                continue

            ledger.record_discovered(
                path=relative_path,
                table_name=table,
                size_bytes=task.file.file_size_in_bytes,
                provider=provider,
                symbol=root,
                sample_date=sample_date,
            )
            ledger.mark_committed([relative_path], table)
            synced += 1

        typer.echo(f"Synced {synced} files as committed. Skipped {skipped}.")
    finally:
        ledger.close()


@catalog_app.command("reset")
def catalog_reset(
    table: Annotated[str, typer.Option("--table", help="Iceberg table to reset.")],
    drop_iceberg: Annotated[
        bool, typer.Option("--drop-iceberg", help="Also drop the Iceberg table from Glue.")
    ] = False,
    after: Annotated[
        str | None, typer.Option("--after", help="Only reset sample_date >= DATE (YYYY-MM-DD).")
    ] = None,
    before: Annotated[
        str | None, typer.Option("--before", help="Only reset sample_date < DATE (YYYY-MM-DD).")
    ] = None,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt.")] = False,
) -> None:
    """Reset Iceberg data, S3 files, and ledger records for a table.

    Use --after/--before for partial resets scoped by date. Without filters,
    resets everything for the table.
    """
    is_filtered = any([after, before])

    if drop_iceberg and is_filtered:
        typer.echo("Cannot use --drop-iceberg with date filters.", err=True)
        raise typer.Exit(1)

    config = CatalogConfig.load()
    ledger = Ledger(config.database_url)

    publisher = Publisher(config)
    try:
        catalog = publisher._iceberg_catalog()
    finally:
        publisher.close()

    try:
        full_name = f"{config.glue_database}.{table}"

        try:
            iceberg_table = catalog.load_table(full_name)
        except NoSuchTableError:
            typer.echo(f"Iceberg table {full_name} not found.", err=True)
            raise typer.Exit(1) from None

        # Build Iceberg row filter from date range
        iceberg_filter = _build_iceberg_filter(after, before)

        # Preview: list files that will be affected
        scan = iceberg_table.scan(row_filter=iceberg_filter)
        planned_files = list(scan.plan_files())
        file_paths = [task.file.file_path for task in planned_files]

        ledger_count_rows = ledger._fetchall(*_build_ledger_count_query(table, after, before))
        ledger_count = ledger_count_rows[0][0] if ledger_count_rows else 0

        filter_desc = []
        if after:
            filter_desc.append(f"after={after}")
        if before:
            filter_desc.append(f"before={before}")
        scope = f" ({', '.join(filter_desc)})" if filter_desc else ""

        typer.echo(f"\nTable: {table!r}{scope}")
        typer.echo(f"  Iceberg data files to remove: {len(file_paths)}")
        for fp in file_paths:
            typer.echo(f"    {fp}")
        typer.echo(f"  Ledger records to delete: {ledger_count}")
        if drop_iceberg:
            typer.echo(f"  Will drop Iceberg table: {full_name}")
        typer.echo()

        if not file_paths and not ledger_count:
            typer.echo("Nothing to reset.")
            raise typer.Exit(0)

        if not yes and not typer.confirm("Proceed with reset?", default=False):
            raise typer.Abort()

        # 1. Delete rows from Iceberg (rewrites metadata, leaves data files as orphans)
        if file_paths:
            iceberg_table.delete(delete_filter=iceberg_filter)
            typer.echo("Deleted matching rows from Iceberg table.")

            # 2. Expire old snapshots so nothing references the dropped files
            iceberg_table.maintenance.expire_snapshots().older_than(datetime.now(UTC)).commit()
            typer.echo("Expired old snapshots.")

            # 3. Delete the now-orphaned data files from S3/MinIO
            if config.catalog_type == "sql":
                from botocore.config import Config as BotocoreConfig  # noqa: PLC0415

                s3 = boto3.client(
                    "s3",
                    endpoint_url=config.minio_endpoint,
                    aws_access_key_id=config.minio_access_key or "",
                    aws_secret_access_key=config.minio_secret_key or "",
                    region_name="us-east-1",
                    config=BotocoreConfig(signature_version="s3v4"),
                )
                iceberg_warehouse_bucket = config.iceberg_minio_bucket or ""
            else:
                session_kwargs: dict[str, str] = {"region_name": config.aws_region}
                if config.aws_profile:
                    session_kwargs["profile_name"] = config.aws_profile
                s3 = boto3.Session(**session_kwargs).client("s3")  # type: ignore[arg-type]
                iceberg_warehouse_bucket = config.iceberg_bucket or ""

            s3_keys = [fp.removeprefix(f"s3://{iceberg_warehouse_bucket}/") for fp in file_paths]
            for i in range(0, len(s3_keys), 1000):
                batch = [{"Key": k} for k in s3_keys[i : i + 1000]]
                s3.delete_objects(
                    Bucket=iceberg_warehouse_bucket,
                    Delete={"Objects": batch},  # type: ignore[typeddict-item]
                )
            typer.echo(f"Deleted {len(s3_keys)} orphan data files from S3.")

        # 3. Optionally drop the entire Iceberg table
        if drop_iceberg:
            catalog.drop_table(full_name)
            typer.echo(f"Dropped Iceberg table {full_name}.")

        # 4. Delete ledger records
        count = ledger.delete_filtered(table, after=after, before=before)
        typer.echo(f"Deleted {count} ledger records for table {table!r}{scope}.")
    finally:
        ledger.close()


@catalog_app.command("setup")
def catalog_setup() -> None:
    """Create Iceberg tables defined in the schema registry.

    Run once to initialize tables, or after adding new table definitions.
    Existing tables are left untouched.
    """
    from tractatus.catalog.schemas import TABLE_DEFS, ensure_table  # noqa: PLC0415

    config = CatalogConfig.load()
    publisher = Publisher(config)
    try:
        catalog = publisher._iceberg_catalog()
    finally:
        publisher.close()

    for table_name in sorted(TABLE_DEFS):
        full_name = f"{config.glue_database}.{table_name}"
        table = ensure_table(catalog, config.glue_database, table_name)
        typer.echo(f"{full_name} — {len(table.schema().fields)} fields")


@catalog_app.command("init-local")
def catalog_init_local() -> None:
    """Initialize the local Iceberg catalog (PostgreSQL + MinIO).

    Creates the tractatus-iceberg MinIO bucket if it does not exist, then
    creates all Iceberg tables in the local SQL catalog. Requires
    ICEBERG_CATALOG_TYPE=sql and MINIO_ICEBERG_BUCKET to be set.

    Safe to re-run — existing tables and buckets are left untouched.
    """
    from botocore.config import Config  # noqa: PLC0415
    from botocore.exceptions import ClientError  # noqa: PLC0415

    from tractatus.catalog.schemas import TABLE_DEFS, ensure_table  # noqa: PLC0415

    config = CatalogConfig.load()

    if config.catalog_type != "sql":
        typer.echo("Error: ICEBERG_CATALOG_TYPE must be 'sql' to use init-local.", err=True)
        raise typer.Exit(1)

    if not config.iceberg_minio_bucket:
        typer.echo("Error: MINIO_ICEBERG_BUCKET is not set.", err=True)
        raise typer.Exit(1)

    # Create MinIO bucket if it doesn't exist
    s3 = boto3.client(
        "s3",
        endpoint_url=config.minio_endpoint,
        aws_access_key_id=config.minio_access_key or "",
        aws_secret_access_key=config.minio_secret_key or "",
        region_name="us-east-1",
        config=Config(signature_version="s3v4"),
    )
    try:
        s3.head_bucket(Bucket=config.iceberg_minio_bucket)
        typer.echo(f"Bucket '{config.iceberg_minio_bucket}' already exists.")
    except ClientError:
        s3.create_bucket(Bucket=config.iceberg_minio_bucket)
        typer.echo(f"Created bucket '{config.iceberg_minio_bucket}'.")

    # Load SQL catalog and create tables
    publisher = Publisher(config)
    catalog = publisher._iceberg_catalog()
    publisher.close()

    for table_name in sorted(TABLE_DEFS):
        full_name = f"{config.glue_database}.{table_name}"
        table = ensure_table(catalog, config.glue_database, table_name)
        typer.echo(f"{full_name} — {len(table.schema().fields)} fields")


@catalog_app.command("expire-snapshots")
def catalog_expire_snapshots(
    table: Annotated[str, typer.Option("--table", help="Iceberg table to expire snapshots for.")],
) -> None:
    """Expire all Iceberg snapshots older than now for a table.

    Run this after bulk publishes to keep snapshot history small and scans fast.
    """
    setup_logging("catalog", verbose=True)
    config = CatalogConfig.load()
    publisher = Publisher(config)
    try:
        expired = publisher.expire_snapshots(table)
    finally:
        publisher.close()
    typer.echo(f"Table:    {table}")
    typer.echo(f"Expired:  {expired} snapshot(s)")


@catalog_app.command("purge")
def catalog_purge(
    table: Annotated[str, typer.Option("--table", help="Iceberg table to purge.")],
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt.")] = False,
) -> None:
    """Purge an Iceberg table: delete all data/metadata, drop from catalog, and reset ledger.

    Use this for full table rebuilds (e.g. schema migration). For partial resets
    scoped by date, use ``catalog reset`` instead.
    """
    config = CatalogConfig.load()
    full_name = f"{config.glue_database}.{table}"

    publisher = Publisher(config)
    try:
        catalog = publisher._iceberg_catalog()
    finally:
        publisher.close()

    # Verify table exists before prompting
    try:
        catalog.load_table(full_name)
    except NoSuchTableError:
        typer.echo(f"Table {full_name} does not exist.", err=True)
        raise typer.Exit(1) from None

    catalog_backend = config.catalog_type  # "sql" or "glue"
    ledger = Ledger(config.database_url)
    try:
        summary = ledger.summary(table)
        ledger_count = sum(summary.values())

        typer.echo(f"\nTable: {full_name}")
        typer.echo(f"  Catalog backend:          {catalog_backend}")
        typer.echo(f"  Ledger:                   {config.database_url}")
        typer.echo(f"  Ledger records to delete: {ledger_count} {summary}")
        typer.echo("  Will purge all data/metadata and drop from catalog")
        typer.echo()

        if not yes and not typer.confirm("Proceed with purge?", default=False):
            raise typer.Abort()

        catalog.purge_table(full_name)
        typer.echo(f"Purged {full_name} (data, metadata, and catalog entry removed).")

        count = ledger.delete_all(table)
        typer.echo(f"Deleted {count} ledger records.")
    finally:
        ledger.close()


def _build_iceberg_filter(after: str | None, before: str | None) -> str:
    """Build a PyIceberg filter expression string from date range parameters."""

    parts: list[str] = []
    if after:
        parts.append(f"sampled_at >= '{after}T00:00:00+00:00'")
    if before:
        parts.append(f"sampled_at < '{before}T00:00:00+00:00'")
    if not parts:
        return AlwaysTrue()  # type: ignore[return-value]
    return " and ".join(parts)


def _build_ledger_count_query(
    table_name: str, after: str | None, before: str | None
) -> tuple[str, tuple[str, ...]]:
    """Build a SELECT COUNT query for ledger records matching date filters."""
    where = "WHERE table_name = %s"
    params: list[str] = [table_name]
    if after:
        where += " AND sample_date >= %s"
        params.append(after)
    if before:
        where += " AND sample_date < %s"
        params.append(before)
    return f"SELECT COUNT(*) FROM catalog_files {where}", tuple(params)


@cache_app.callback(invoke_without_command=True)
def cache_callback(
    ctx: typer.Context,
    table: Annotated[
        str | None, typer.Option("--table", help="Iceberg table to cache from.")
    ] = None,
    provider: Annotated[
        list[str] | None,
        typer.Option("--provider", help="Limit to specific providers (repeatable)."),
    ] = None,
    after: Annotated[
        str | None,
        typer.Option(
            "--after", help="Only include sample_date >= DATE (YYYY-MM-DD). Not for candles."
        ),
    ] = None,
    before: Annotated[
        str | None,
        typer.Option(
            "--before", help="Only include sample_date < DATE (YYYY-MM-DD). Not for candles."
        ),
    ] = None,
    root: Annotated[
        list[str] | None, typer.Option("--root", help="Limit to specific roots (repeatable).")
    ] = None,
    interval: Annotated[
        str | None, typer.Option("--interval", help="Candle interval filter (candles only).")
    ] = None,
) -> None:
    """Read from Iceberg, group by artifact grain, write parquet to MinIO cache, upsert ledger.

    When called without a subcommand, ``--table`` is required.
    Use ``tractatus catalog cache status|evict|clear`` for cache management subcommands.
    """
    if ctx.invoked_subcommand is not None:
        return

    if not table:
        typer.echo(ctx.get_help())
        raise typer.Exit(0)

    setup_logging("catalog", verbose=True)

    parsed_after = date.fromisoformat(after) if after else None
    parsed_before = date.fromisoformat(before) if before else None

    config = CatalogConfig.load()
    publisher = Publisher(config)
    try:
        cached = publisher.cache_artifacts(
            table_name=table,
            providers=provider or None,
            after=parsed_after,
            before=parsed_before,
            roots=root or None,
            interval=interval,
        )
    finally:
        publisher.close()

    typer.echo(f"Table:  {table}")
    typer.echo(f"Cached: {cached}")


@cache_app.command("status")
def cache_status(
    table: Annotated[
        str | None, typer.Option("--table", help="Filter to a specific table.")
    ] = None,
) -> None:
    """Show cache summary (file count and total bytes)."""
    config = CatalogConfig.load()
    ledger = Ledger(config.database_url)
    try:
        summary = ledger.cache_summary(table_name=table)
    finally:
        ledger.close()

    if table:
        typer.echo(f"Table: {table}")
    typer.echo(f"  Cached files: {summary['cached_files']}")
    typer.echo(f"  Cached bytes: {summary['cached_bytes']}")


@cache_app.command("evict")
def cache_evict(
    older_than: Annotated[
        int, typer.Option("--older-than", help="Evict files not accessed in N days.")
    ],
    table: Annotated[
        str | None, typer.Option("--table", help="Filter to a specific table.")
    ] = None,
    provider: Annotated[
        str | None, typer.Option("--provider", help="Filter to a specific provider.")
    ] = None,
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt.")] = False,
) -> None:
    """Evict cached files older than N days from local MinIO and clear ledger entries."""
    config = CatalogConfig.load()
    ledger = Ledger(config.database_url)
    try:
        files = ledger.cached_files(table_name=table, older_than_days=older_than, provider=provider)
        if not files:
            typer.echo("No cached files match the criteria.")
            return

        typer.echo(f"Found {len(files)} cached file(s) to evict.")
        if not yes and not typer.confirm("Proceed with eviction?", default=False):
            raise typer.Abort()

        cache_client = None
        if config.minio_endpoint and config.cache_bucket:
            cache_client = boto3.client(
                "s3",
                endpoint_url=config.minio_endpoint,
                aws_access_key_id=config.minio_access_key or "",
                aws_secret_access_key=config.minio_secret_key or "",
                region_name="us-east-1",
            )

        evicted = 0
        for f in files:
            if cache_client and f.cache_key and config.cache_bucket:
                with contextlib.suppress(Exception):
                    cache_client.delete_object(Bucket=config.cache_bucket, Key=f.cache_key)
            ledger.clear_cache_entry(f.path, f.table_name)
            evicted += 1

        typer.echo(f"Evicted {evicted} cached file(s).")
    finally:
        ledger.close()


@cache_app.command("clear")
def cache_clear(
    table: Annotated[str, typer.Option("--table", help="Table to clear cache for.")],
    yes: Annotated[bool, typer.Option("--yes", "-y", help="Skip confirmation prompt.")] = False,
) -> None:
    """Clear all cached files for a table from local MinIO and ledger."""
    config = CatalogConfig.load()
    ledger = Ledger(config.database_url)
    try:
        files = ledger.cached_files(table_name=table)
        if not files:
            typer.echo(f"No cached files for table {table!r}.")
            return

        typer.echo(f"Found {len(files)} cached file(s) for table {table!r}.")
        if not yes and not typer.confirm("Proceed with clear?", default=False):
            raise typer.Abort()

        cache_client = None
        if config.minio_endpoint and config.cache_bucket:
            cache_client = boto3.client(
                "s3",
                endpoint_url=config.minio_endpoint,
                aws_access_key_id=config.minio_access_key or "",
                aws_secret_access_key=config.minio_secret_key or "",
                region_name="us-east-1",
            )

        cleared = 0
        for f in files:
            if cache_client and f.cache_key and config.cache_bucket:
                with contextlib.suppress(Exception):
                    cache_client.delete_object(Bucket=config.cache_bucket, Key=f.cache_key)
            ledger.clear_cache_entry(f.path, f.table_name)
            cleared += 1

        typer.echo(f"Cleared {cleared} cached file(s) for table {table!r}.")
    finally:
        ledger.close()


# ---------------------------------------------------------------------------
# app sub-commands
# ---------------------------------------------------------------------------


@app_cmd.command("coverage")
def app_coverage(
    port: Annotated[int, typer.Option("--port", help="Port to serve the app on.")] = 8501,
    ledger: Annotated[
        Path | None, typer.Option("--ledger", help="Path to catalog_ledger.db.")
    ] = None,
) -> None:
    """Launch the lakehouse coverage visualization app."""
    if shutil.which("streamlit") is None:
        typer.echo("ERROR: streamlit not found on PATH. Install it with: pip install streamlit")
        raise typer.Exit(1)
    app_path = Path(__file__).parent / "app" / "coverage.py"
    cmd = ["streamlit", "run", str(app_path), "--server.port", str(port)]
    if ledger is not None:
        cmd += ["--", str(ledger)]
    subprocess.run(cmd, check=True)
