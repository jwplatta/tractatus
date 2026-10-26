#!/usr/bin/env bash
# Discover, publish, and cache today's order book data from tickrake-staging to local MinIO.
#
# Usage:
#   bash scripts/publish_daily_order_book.sh              # uses today's date
#   bash scripts/publish_daily_order_book.sh 2026-10-22   # specific date
#
# Run from repo root with the virtualenv active:
#   source .venv/bin/activate && bash scripts/publish_daily_order_book.sh

set -euo pipefail

DATE="${1:-$(date +%Y-%m-%d)}"
NEXT_DATE=$(date -v+1d -j -f "%Y-%m-%d" "$DATE" +%Y-%m-%d 2>/dev/null \
  || date -d "$DATE + 1 day" +%Y-%m-%d)

echo "==> Discovering order_book for $DATE..."
uv run tractatus catalog discover \
  --table order_book \
  --after "$DATE" \
  --before "$NEXT_DATE"

echo "==> Publishing and caching order_book for $DATE..."
uv run tractatus catalog publish \
  --table order_book \
  --after "$DATE" \
  --before "$NEXT_DATE" \
  --cache

echo "==> Done: $DATE"
