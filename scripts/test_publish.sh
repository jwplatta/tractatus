#!/usr/bin/env bash
#
# End-to-end smoke test for the catalog publisher.
# Publishes a small batch, verifies, then tears everything down.
#
# Prerequisites:
#   - ICEBERG_BUCKET set (e.g. tractatus-lakehouse)
#   - AWS credentials configured
#   - Parquet files present under TICKRAKE_DATA_DIR/options/
#
# Usage:
#   ./scripts/test_publish.sh [--table options]

set -euo pipefail

TABLE="${1:---table}"
if [[ "$TABLE" == "--table" ]]; then
    TABLE="options"
else
    shift
fi

echo "=== Step 1: Dry run (discover only) ==="
uv run tractatus catalog publish --table "$TABLE" --dry-run
echo

LIMIT="${LIMIT:-2}"

echo "=== Step 2: Publish (limit=$LIMIT) ==="
uv run tractatus catalog publish --table "$TABLE" --limit "$LIMIT"
echo

echo "=== Step 3: Check ledger status ==="
uv run tractatus catalog status --table "$TABLE"
echo

read -rp "Reset everything for table '$TABLE'? [y/N] " confirm
if [[ "$confirm" =~ ^[Yy]$ ]]; then
    echo "=== Step 4: Reset (ledger + S3 + Iceberg) ==="
    uv run tractatus catalog reset --table "$TABLE" --delete-s3 --drop-iceberg
    echo
    echo "=== Verify ledger is clean ==="
    uv run tractatus catalog status --table "$TABLE"
else
    echo "Skipping reset. Data remains published."
fi

echo "Done."
