#!/usr/bin/env bash
# Discover, publish, and cache today's options data from tickrake-staging to local MinIO.
#
# Usage:
#   bash scripts/publish_daily_options.sh              # uses today's date
#   bash scripts/publish_daily_options.sh 2026-10-25   # specific date
#
# Run from repo root with the virtualenv active:
#   source .venv/bin/activate && bash scripts/publish_daily_options.sh

set -euo pipefail

DATE="${1:-$(date +%Y-%m-%d)}"
NEXT_DATE=$(date -v+1d -j -f "%Y-%m-%d" "$DATE" +%Y-%m-%d 2>/dev/null \
  || date -d "$DATE + 1 day" +%Y-%m-%d)

echo "==> Discovering options for $DATE..."
uv run tractatus catalog discover \
  --table options \
  --after "$DATE" \
  --before "$NEXT_DATE"

echo "==> Publishing and caching options for $DATE..."
uv run tractatus catalog publish \
  --table options \
  --after "$DATE" \
  --before "$NEXT_DATE" \
  --cache

echo "==> Done: $DATE"
