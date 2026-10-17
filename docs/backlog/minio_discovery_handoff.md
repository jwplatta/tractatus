---
type: feature
tags: [minio, discovery, publisher, lakehouse]
title: Discover publishable files from MinIO instead of local filesystem
description: Replace local filesystem discovery with MinIO (S3-compatible) as the handoff boundary between tickrake and the tractatus catalog publisher
created: 2026-10-17
status: not-started
priority: high
source: tractatus/catalog-publisher
---

# Summary

The handoff boundary between tickrake (data collection/compaction) and the tractatus catalog publisher should be a locally running MinIO object store rather than the local filesystem. Tickrake writes compacted data to MinIO; tractatus discovers unpublished files from MinIO and publishes them to the Iceberg lakehouse.

## Motivation

Decouples tickrake's data pipeline from tractatus's publishing pipeline with a durable, S3-compatible interface. Avoids tight coupling to the local filesystem layout and enables the publisher to run on a different host from tickrake.

## Scope

- Update `CatalogConfig` to support an S3-compatible endpoint (MinIO URL, bucket, credentials)
- Update the discovery module to scan MinIO instead of local paths
- The rest of the pipeline (normalize → `table.append()`) stays the same — only the discovery source changes
- Per-table path parsers should work against S3 key prefixes with minimal changes
