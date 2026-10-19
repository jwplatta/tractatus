# Research node setup plan

## Goal

Make it possible for a researcher to set up a working local research node by
following one short guide. The node must be able to read published Tickrake
data from S3, reuse one local data cache, run a small deterministic backtest,
and log its result to MLflow.

The initial goal is research onboarding only. It does not include collecting
data, paper trading, live trading, or deploying strategies.

## Boundaries

- `quant-infra` owns local service orchestration.
- Tickrake owns data collection, validation, and publishing to S3.
- Tractatus owns research-facing data access, the research-node CLI, and the
  generated research project.
- A research-only user has read-only AWS access to published Tickrake data.
  They do not receive Tickrake's writer credentials or a `schwab_rb` token.
- MLflow records experiments. It is not a trading deployment system in this
  phase.

## Local data contract

There is one machine-local cache, shared by Tickrake, Tractatus, and Options
Monitor:

```text
~/.tickrake/data/
```

Tickrake writes collected data there. Tractatus first looks there; if a
published file is missing, it downloads it from S3 into the same Tickrake
layout. Options Monitor can then reuse that file without another download.

Tractatus must not create a second cache under `~/.cache`, a project-local
directory, or a separate volume. `TICKRAKE_DATA_DIR` remains the configuration
point for this shared cache.

For options data, downloaded files and cached discovery documents must preserve
the published Tickrake layout:

```text
~/.tickrake/data/options/<provider>/tickers.json
~/.tickrake/data/options/<provider>/<ROOT>.json
~/.tickrake/data/options/<provider>/<YYYY>/<MM>/<DD>/<ROOT>_samples_<date>.parquet
```

The S3 object URI and object version or ETag should be retained alongside the
cached object metadata so a run can record exactly what it used. A cache hit
must make no S3 download request.

## Researcher journey

The documented happy path should be close to the following. Exact install
commands can change as releases are added, but the number of conceptual steps
should not grow.

```sh
git clone git@github.com:jwplatta/quant-infra.git
cd quant-infra
cp envs/research.example.env envs/research.env
# Configure a read-only AWS profile and local MLflow settings.
make research-up

uv tool install "tractatus @ git+ssh://git@github.com/jwplatta/tractatus.git@<release>"
tractatus init research --name my-first-study
cd my-first-study
uv sync
tractatus verify
tractatus notebook
```

`tractatus init research` creates `./research` when `--name` is omitted.
With `--name my-first-study`, it creates `./my-first-study`.

The generated project contains:

- a pinned Tractatus dependency and committed `uv.lock`;
- `.env.example` with endpoint and AWS-profile names but no credentials;
- a configured MLflow experiment name;
- a notebook and a script for the smoke test;
- a small, repeatable backtest example;
- a README with the next command to run at each step.

## Minimal infrastructure profile

Add a research-only entry point to `quant-infra`, such as:

```sh
make research-up
make research-down
make research-ps
```

The minimum profile starts Postgres and MLflow. It does not start Tickrake,
Schwab authentication, scheduled collection jobs, Grafana, Prometheus, Loki,
or Promtail.

MinIO is optional. Start it only when a research task specifically needs local
intraday data or publishing behavior; it is not needed to read historical data
from the published S3 bucket.

The `research.example.env` file must identify the MLflow endpoint, the AWS SSO
or named profile, the S3 bucket and region, and `TICKRAKE_DATA_DIR`. It must not
contain collector or broker credentials.

## CLI surface

The first Tractatus CLI release needs only these commands:

```text
tractatus init research [--name NAME]
tractatus data pull ...
tractatus verify
tractatus notebook
```

`data pull` resolves a published Tickrake index, checks the shared local cache,
and downloads only missing objects. It is useful both directly and as the data
layer used by notebooks and backtests.

`verify` is the onboarding acceptance test. It must:

1. Check that the configured MLflow endpoint is reachable.
2. Check the configured AWS identity and read-only access to the Tickrake
   bucket.
3. Resolve and download a small, pinned published smoke-test dataset into
   `TICKRAKE_DATA_DIR` if it is absent.
4. Read that data through `TickrakeClient`.
5. Run a deterministic toy backtest.
6. Log parameters, data identity, metrics, and one small artifact to MLflow.
7. Print the MLflow run ID and URL.

The smoke-test dataset must be intentionally maintained as part of the
published-data contract. Do not make onboarding depend on whichever recent
symbol or sample date happens to be available.

`notebook` launches the generated project's Jupyter environment after checking
that its dependencies are installed.

## Required Tractatus data-access work

The current archive client checks a local root index before it can find a
historical Parquet object. A fresh research node will not have that index.

Implement this lookup order:

1. Read the local `<ROOT>.json` from `TICKRAKE_DATA_DIR` if present.
2. If it is absent or stale, fetch the published index from S3 and save it in
   the corresponding local Tickrake path.
3. Resolve the requested object URI from the index.
4. Return the local file if present; otherwise download it atomically into its
   final Tickrake path.
5. Validate the downloaded size and available integrity metadata before making
   it visible to readers.

Concurrent readers must not produce duplicate downloads or expose partial
files. Use a per-object lock and temporary file followed by an atomic rename.

## Read-only AWS access

Researchers use a distinct IAM Identity Center permission set or other
dedicated read-only role. It may allow `s3:GetObject`, `s3:GetObjectVersion`,
and narrowly scoped `s3:ListBucket`/`s3:GetBucketLocation` for published data
prefixes. It must not permit object writes, deletes, bucket-policy changes, or
access to Tickrake's writer credentials.

Keep S3 Block Public Access enabled. The bucket remains private; access comes
from authenticated, least-privilege identities rather than public objects.

## Implementation order

1. Add the CLI entry point and `init research` template generation.
2. Add the research-only `quant-infra` Compose targets and environment example.
3. Add S3 index fallback, atomic shared-cache downloads, and `data pull`.
4. Publish and pin a small smoke-test dataset.
5. Implement `verify`, including its MLflow smoke run.
6. Validate the guide from an empty user home directory or a clean machine.

## Done criteria

This work is done when a person who has Git, Docker, `uv`, and a read-only AWS
profile can follow the guide without a Tickrake checkout, Schwab credentials,
or manual data copying; `tractatus verify` completes; and the resulting MLflow
run identifies the exact data and environment used.
