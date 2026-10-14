# Tractatus Catalog — Iceberg Publisher

Publishes compacted options Parquet files to an Apache Iceberg table backed by AWS Glue Data Catalog and S3. A PostgreSQL ledger tracks file discovery, upload, and commit progress so runs are idempotent and can resume after failures.

## Architecture

```
~/.tickrake/data/options/         ← Tickrake produces compacted parquet here
        │
        ▼
   Discovery                      ← Scans for *_samples_*.parquet files
        │
        ▼
   PostgreSQL Ledger              ← Tracks: discovered → uploaded → committed
        │
        ▼
   S3 Upload                      ← Uploads parquet to the Iceberg warehouse bucket
        │
        ▼
   Iceberg Commit                 ← Registers files via PyIceberg add_files()
        │
        ▼
   AWS Glue Data Catalog          ← Iceberg metadata (table, schema, snapshots)
```

## Prerequisites

- **PostgreSQL** — the quant-infra shared instance (see below)
- **AWS credentials** — for S3 uploads and Glue catalog access (via AWS profile or environment)
- **S3 bucket** — a dedicated bucket for Iceberg data

## Environment Variables

| Variable              | Required | Default                                                          | Description                                          |
|-----------------------|----------|------------------------------------------------------------------|------------------------------------------------------|
| `ICEBERG_BUCKET`      | **yes**  | —                                                                | S3 bucket for Iceberg table data and metadata        |
| `CATALOG_DATABASE_URL`| no       | `postgresql://tractatus_user:tractatus@localhost:5432/tractatus`  | PostgreSQL connection string for the publication ledger |
| `OPTIONS_DATA_ROOT`   | no       | `$TICKRAKE_DATA_DIR/options`                                     | Local directory to scan for compacted parquet files   |
| `GLUE_DATABASE`       | no       | `tractatus`                                                      | AWS Glue database name                               |
| `GLUE_TABLE`          | no       | `options`                                                        | AWS Glue table name                                  |
| `AWS_PROFILE`         | no       | —                                                                | AWS profile for S3 and Glue access                   |
| `AWS_REGION`          | no       | `us-east-1`                                                      | AWS region for Glue and S3                           |

Variables inherited from `TractatusConfig`:
- `TICKRAKE_DATA_DIR` — base data directory (default `~/.tickrake/data`)
- `S3_BUCKET` — existing archive bucket (not used by catalog, but resolved by TractatusConfig)

## PostgreSQL Setup (quant-infra)

The catalog ledger lives in a `tractatus` database on the shared quant-infra PostgreSQL instance. The database and user are created by the quant-infra init script:

```
quant-infra/services/postgres/init/02-create-catalog-tables.sql
```

This creates:
- Database: `tractatus`
- User: `tractatus_user` (password: `tractatus`)

The `catalog_files` table and indexes are created automatically by the `Ledger` class on first connection (`CREATE TABLE IF NOT EXISTS`), so the schema definition lives in this repo alongside the code that uses it.

### For existing instances

If the postgres container already has its data volume, the database init script won't re-run automatically. Create the database and user manually:

```bash
# Dev (port 5433)
psql -h localhost -p 5433 -U postgres -c "CREATE DATABASE tractatus;"
psql -h localhost -p 5433 -U postgres -c "CREATE USER tractatus_user WITH PASSWORD 'tractatus';"
psql -h localhost -p 5433 -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE tractatus TO tractatus_user;"
psql -h localhost -p 5433 -U postgres -d tractatus -c "GRANT ALL ON SCHEMA public TO tractatus_user;"
```

The table itself will be created on the first `tractatus catalog publish` or `tractatus catalog status` run.

### Connection from containers

Inside the Docker network, use `postgres` as the hostname:

```
CATALOG_DATABASE_URL=postgresql://tractatus_user:tractatus@postgres:5432/tractatus
```

From the host machine (dev):

```
CATALOG_DATABASE_URL=postgresql://tractatus_user:tractatus@localhost:5433/tractatus
```

## AWS Setup

### S3 Bucket

Create a dedicated Iceberg bucket (or use a prefix in an existing bucket):

```bash
aws s3 mb s3://your-iceberg-bucket --region us-east-1
```

The publisher uploads parquet files under `warehouse/{glue_database}/{glue_table}/data/`.

### Glue Permissions

The AWS credentials (profile or role) need:
- `glue:GetDatabase`, `glue:CreateDatabase`
- `glue:GetTable`, `glue:CreateTable`, `glue:UpdateTable`
- `s3:PutObject`, `s3:GetObject`, `s3:ListBucket` on the Iceberg bucket

### IAM Policy (minimal)

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "glue:GetDatabase", "glue:CreateDatabase",
        "glue:GetTable", "glue:CreateTable", "glue:UpdateTable",
        "glue:GetTables"
      ],
      "Resource": "*"
    },
    {
      "Effect": "Allow",
      "Action": ["s3:PutObject", "s3:GetObject", "s3:ListBucket", "s3:DeleteObject"],
      "Resource": [
        "arn:aws:s3:::your-iceberg-bucket",
        "arn:aws:s3:::your-iceberg-bucket/*"
      ]
    }
  ]
}
```

## Usage

```bash
# Discover files and show what would be published (no upload/commit)
tractatus catalog publish --dry-run

# Publish all discovered parquet to Iceberg
tractatus catalog publish

# Limit to a specific provider
tractatus catalog publish --provider schwab

# Check ledger status
tractatus catalog status
```

## Running in quant-infra

The publisher will run as a container in quant-infra. It needs:
- Read-only mount of `~/.tickrake/data` (or `OPTIONS_DATA_ROOT`)
- Network access to the postgres service
- AWS credentials (via environment or mounted profile)
- `ICEBERG_BUCKET` environment variable

Example compose service (to be added to quant-infra):

```yaml
tractatus-publisher:
  image: tractatus-publisher:latest
  profiles: [prod]
  environment:
    ICEBERG_BUCKET: your-iceberg-bucket
    CATALOG_DATABASE_URL: postgresql://tractatus_user:tractatus@postgres:5432/tractatus
    AWS_PROFILE: default
    OPTIONS_DATA_ROOT: /data/options
  volumes:
    - ~/.tickrake/data:/data:ro
    - ~/.aws:/root/.aws:ro
  depends_on:
    - postgres
```
