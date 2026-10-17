---
type: bug
tags: [catalog, publisher, ledger, date-filter]
title: Apply publish filters to pending upload and commit work
description: Keep date- and provider-bounded publish runs from processing unrelated pending ledger rows
created: 2026-10-15
updated: 2026-10-15
status: completed
priority: high
source: tractatus/catalog
---

# Summary

`catalog publish --after/--before` applies the date bounds while discovering local files, but the subsequent upload and commit stages drain all pending ledger rows for the table. A bounded one-off run can therefore upload and commit files outside its requested date range when those rows were already pending, including rows discovered by the scheduled publisher. The quant-infra Make target passes the date arguments; the filter is lost inside Tractatus's publisher pipeline.

## Requirements

- Apply the same date and provider scope to ledger rows selected for upload and commit as to local discovery.
- Ensure a bounded publish run leaves pending ledger rows outside its scope untouched.
- Preserve current unbounded behavior when no date/provider filters are supplied.
- Keep upload and commit selection consistent so a run does not upload out-of-scope files or commit unrelated uploaded rows.
- Add regression tests that seed the ledger with in-scope and out-of-scope pending rows, run a bounded publish, and verify only matching rows are uploaded and committed.
- Cover inclusive `--after`, exclusive `--before`, and provider filters.

## Dependencies & Resources

- `src/tractatus/catalog/publisher.py`: `Publisher.run`, `_upload`, and `_commit`
- `src/tractatus/catalog/ledger.py`: `pending_upload` and `pending_commit` queries
- `src/tractatus/cli.py`: `catalog publish` date/provider option parsing
- Reproduction: a scheduled run with `--after 2026-10-01` populated October pending entries; a one-off December-bounded publish then uploaded October entries from the shared ledger.
