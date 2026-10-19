# Options Archive Index Compatibility

## Problem

`OptionsArchive#get_parquet_path` cannot read the live Tickrake SPXW archive
index. This prevents Options Monitor from falling back to archived option
snapshots when the intraday MinIO index has no matching expiry.

The failure is independent of AWS configuration. The Options Monitor container
successfully authenticates with the `tickrake` AWS profile and lists objects in
the `tickrake` archive bucket.

## Live index schema

The archive index at `s3://tickrake/options/schwab/SPXW.json` contains a
`historical` array. Each entry stores `files` as an array of descriptors:

```json
{
  "sample_date": "2025-12-18",
  "archived_at": "2026-09-14T03:24:54Z",
  "files": [
    {
      "format": "parquet",
      "uri": "s3://tickrake/options/schwab/2025/12/18/SPXW_samples_2025-12-18.parquet"
    },
    {
      "format": "csv",
      "uri": "s3://tickrake/options/schwab/2025/12/18/SPXW_samples_2025-12-18.csv"
    }
  ]
}
```

## Current failure

`tractatus.tickrake.options.archive` assumes `entry["files"]` is a mapping
and calls `files.get("parquet")`. With the live schema, `files` is a list,
which raises:

```text
AttributeError: 'list' object has no attribute 'get'
```

## Required fix

Update the archive descriptor lookup to support the list schema by selecting
the descriptor whose `format` is `"parquet"` and using its `uri`. Preserve
support for the mapping schema if it is still used by older indexes.

Add unit coverage for both forms:

1. `files: [{"format": "parquet", "uri": ...}, ...]`
2. `files: {"parquet": ...}`

The expected result is that `get_parquet_path("SPXW", sample_date)` returns
the archived Parquet path for the newest available sample date, allowing
Options Monitor's archive fallback to load the next-expiry window.
