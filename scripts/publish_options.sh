#!/usr/bin/env bash
# Discover and publish Oct 19 2026 options samples to the lakehouse.
# Run from the repo root with the .venv active:
#   source .venv/bin/activate && bash scripts/publish_oct19_options.sh

set -euo pipefail

TABLE=options
AFTER=2026-10-19
BEFORE=2026-10-20

echo "=== Discovering $TABLE ($AFTER to $BEFORE) ==="
tractatus catalog discover --table "$TABLE" --after "$AFTER" --before "$BEFORE"

echo ""
echo "=== Publishing $TABLE ($AFTER to $BEFORE) ==="
tractatus catalog publish --table "$TABLE" --after "$AFTER" --before "$BEFORE"
