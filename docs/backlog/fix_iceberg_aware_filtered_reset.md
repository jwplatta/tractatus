---
type: bug
tags: [iceberg, lakehouse, s3, ledger, reset]
title: Make filtered catalog reset Iceberg-aware
description: Prevent reset from deleting S3 data files while active Iceberg metadata still references them
created: 2026-10-15
updated: 2026-10-15
status: completed
priority: high
source: tractatus/catalog
---

# Summary

The filtered `catalog reset --delete-s3` operation deletes matching ledger rows and their S3 data objects but leaves the Iceberg table snapshot and manifests unchanged. This creates a table whose metadata references missing objects, so queries over the affected dates can fail. Make date- and provider-scoped reset safe for the Iceberg table as well as the ledger and S3 storage.

## Requirements

- For a filtered reset, remove the matching records from the Iceberg table through an Iceberg-aware operation before deleting any data files still referenced by the table.
- Keep Iceberg metadata, manifests, and snapshots consistent with the remaining rows and files; do not directly delete referenced objects from S3.
- Delete matching ledger rows only after the Iceberg change succeeds. Make retries safe if the operation fails partway through.
- Preserve the current default behavior that leaves S3 objects in place when `--delete-s3` is omitted, while ensuring any Iceberg metadata change is explicit and accurately described by CLI help and confirmation text.
- Preserve local Parquet inputs under `~/.tickrake/data`; reset must never modify or delete files there.
- Add tests covering date/provider filters, failure ordering, retry behavior, and preservation of unrelated Iceberg rows, ledger records, and S3 objects.

## Dependencies & Resources

- `src/tractatus/cli.py`: `catalog reset`
- `src/tractatus/catalog/ledger.py`: filtered ledger lookup and deletion
- `src/tractatus/catalog/schema.py`: options Iceberg table partitioning by month
- Production reproduction: resetting December 2025 with `--delete-s3` would remove S3 objects while leaving Iceberg metadata references intact.
