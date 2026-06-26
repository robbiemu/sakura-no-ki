# Local Telemetry Archives

This is a draft design and research task for long-term local telemetry storage.
It depends on the local chDB checkpoint work: checkpoints make the hot store
recoverable; archives make older telemetry available after the hot store ages it
out.

## Problem

Maple local mode currently uses embedded chDB as a hot operational store. That
is a good fit for recent investigation, but it is not a long-term source of
record:

- local raw tables already have operational TTLs, commonly 30 days for logs and
  traces and 90 days for metrics and hourly rollups;
- a large hot store can increase disk usage, query cost, startup cost, and memory
  pressure;
- checkpoints protect recent recovery, but they are full hot-store restore
  points, not a two-year history;
- operators still need to answer questions like "when did this behavior first
  appear?" across months or years.

The feature should rotate sealed telemetry ranges out of the hot chDB store into
immutable cold chunks, with enough metadata that an operator can later locate and
scan those chunks independently.

## Non-Goals

- Do not restore old chunks back into Maple's normal local UI or query workflow
  in the first implementation.
- Do not copy the live chDB data directory as an archive mechanism.
- Do not add a second always-on OLTP mirror unless research proves file export
  cannot satisfy the retention goal.
- Do not delete hot data until a matching archive chunk is complete, checksummed,
  cataloged, and outside the late-arrival safety window.

## Current Local Data Shape

The first archive scope should be the raw signal tables:

| Signal  | Tables                                                                               | Event time column             | Current TTL |
| ------- | ------------------------------------------------------------------------------------ | ----------------------------- | ----------- |
| logs    | `logs`                                                                               | `TimestampTime` / `Timestamp` | 30 days     |
| traces  | `traces`                                                                             | `Timestamp`                   | 30 days     |
| metrics | `metrics_sum`, `metrics_gauge`, `metrics_histogram`, `metrics_exponential_histogram` | `TimeUnix`                    | 90 days     |

Derived tables such as `trace_list_mv`, `logs_aggregates_hourly`,
`error_events`, and service-map rollups are useful for Maple's interactive UI,
but they should not be the first cold-archive source of truth. They are derived
from raw tables, can have different TTLs, and can drift from raw-table retention
by design.

The archive manifest should still record derived-table row counts and time
ranges when available, because those counts are useful validation evidence.

## Recommended Architecture

Use a three-part model:

1. Hot store:
    - embedded chDB owned by `maple start`;
    - bounded recent working set;
    - protected by checkpoints for dirty shutdown recovery.

2. Cold archive chunks:
    - immutable directories or files on an operator-selected archive root;
    - target around 8 GiB compressed per chunk, but split on time boundaries;
    - written as `building/` first and atomically promoted only after validation.

3. Archive catalog:
    - append-only JSONL or small SQLite/libSQL catalog next to the chunks;
    - maps time ranges, signals, services, row counts, schema/build versions,
      checksums, and paths;
    - queryable without opening the hot chDB store.

Proposed layout:

```text
archives/
  catalog.jsonl
  chunks/
    building/
      local-2026-01-01T00-00-00Z_2026-01-08T00-00-00Z/
        manifest.json
        logs.parquet
        traces.parquet
        metrics_sum.parquet
        metrics_gauge.parquet
        metrics_histogram.parquet
        metrics_exponential_histogram.parquet
        checksums.sha256
    current/
      local-2026-01-01T00-00-00Z_2026-01-08T00-00-00Z/
```

If Parquet is not viable in chDB local mode, use newline-delimited JSON with
compression:

```text
logs.ndjson.zst
traces.ndjson.zst
metrics_sum.ndjson.zst
```

Parquet is preferred because DuckDB, ClickHouse, Spark, and many forensic tools
can scan it directly with predicate pushdown. NDJSON is simpler and safer if
chDB cannot stream/write Parquet without buffering too much in memory.

## Archive Flow

1. Choose a sealed time window:
    - never newer than `now - archive_lag`, default at least 24 hours;
    - align to day boundaries by default;
    - split to hourly windows only when a single day exceeds the target chunk
      size.

2. Estimate chunk size:
    - prefer `system.parts` active bytes per table/partition if chDB exposes it;
    - fall back to row counts and sampled average row size;
    - keep all selected raw signal tables for the same window in one chunk when
      practical.

3. Export to `archives/chunks/building/<chunk-id>/`:
    - use a server-owned archive command or admin endpoint while `maple start`
      owns chDB;
    - avoid `/local/query` for large exports because it wraps results into one
      JSON array and `Chdb.query()` returns an in-memory buffer;
    - prefer `SELECT ... INTO OUTFILE ... FORMAT Parquet` if chDB supports it
      safely with bounded memory;
    - otherwise page through bounded windows/limits and stream compressed NDJSON
      from the Maple process.

4. Validate:
    - count rows per table in chDB for the exact exported window;
    - count rows in the exported files with the selected independent reader;
    - compute min/max timestamps and service cardinality per file;
    - compute SHA-256 checksums after files are closed.

5. Write `manifest.json`:
    - chunk format version;
    - Maple version, chDB version, schema fingerprint;
    - source data dir and source checkpoint id if available;
    - time range, signal list, table list, SQL predicates;
    - row counts, byte sizes, checksums, min/max timestamps;
    - validation command and result.

6. Promote:
    - rename the completed chunk from `building/` to `current/` on the same
      filesystem;
    - append one catalog entry only after promotion succeeds.

7. Hot-store pruning:
    - initial version can rely on existing TTLs and only guarantee cold archive
      retention before TTL expiry;
    - explicit pruning should be a separate, gated step such as
      `maple archive prune-hot --before <timestamp>`;
    - pruning must only consider ranges already archived and validated.

## Querying Old Chunks

The first implementation does not need to load old chunks back into Maple.

Acceptable operator workflows:

```sql
-- DuckDB over Parquet
SELECT
  ServiceName,
  min(TimestampTime) AS first_seen,
  max(TimestampTime) AS last_seen,
  count(*) AS rows
FROM read_parquet('/archive/chunks/current/*/logs.parquet')
WHERE Body ILIKE '%timeout%'
GROUP BY ServiceName
ORDER BY first_seen;
```

```bash
# NDJSON fallback
zstdcat /archive/chunks/current/*/logs.ndjson.zst |
  jq 'select(.Body | test("timeout"; "i")) | {TimestampTime, ServiceName, Body}'
```

A later Maple command could be a thin convenience wrapper:

```bash
maple archive list --service api --from 2025-01-01 --to 2026-01-01
maple archive scan --sql 'SELECT ...' --engine duckdb
```

That wrapper is optional. The core requirement is durable, documented, indexed
files that standard tools can scan.

## Failure Cases

### Maple crashes during export

The incomplete chunk remains under `building/` and is ignored by the catalog.
The next archive run removes or resumes it after checking the manifest state.

### Maple crashes after files are written but before catalog append

The chunk may exist on disk but is not discoverable from the catalog. A
reconciliation command can scan `current/` chunks and append missing catalog
entries after verifying checksums.

### Late telemetry arrives for an already archived window

This is the biggest semantic risk. Use an archive lag and document that local
archives are based on event time plus a safety delay. Research should test how
common old timestamps are in local OTLP traffic and whether overlap chunks or
catalog supersession are needed.

### Schema changes between chunks

Chunks are immutable and carry their schema fingerprint plus column list. Query
tools should union only compatible chunks by default, or project a common column
subset when the operator opts in.

### Archive root fills

Do not delete hot data or old archive chunks silently. Report the needed space,
the oldest/newest archive ranges, and the hot TTL deadline.

## Research Task

The research node should answer these questions with first-hand evidence. Tag
claims as verified, documented, not reproduced, or not tested.

### 1. Export primitives

Using a disposable Maple local chDB store with known logs, traces, and all four
metric table types:

- test `SELECT ... INTO OUTFILE ... FORMAT Parquet`;
- test `SELECT ... INTO OUTFILE ... FORMAT JSONEachRow`;
- test whether compression can be written directly (`.zst`, `.gz`, ClickHouse
  compression settings, or external compression);
- test whether those commands work through the running Maple server, a direct
  `Chdb.open` harness, or both;
- measure whether the operation buffers the full result in Maple/chDB memory or
  writes in bounded memory.

Decision output: recommended archive format and export mechanism.

### 2. Live export safety

Run export while OTLP ingest is active.

- Confirm chDB's single connection serializes the export with inserts, or
  document any interleaving behavior;
- measure `/health`, ingest response latency, and OTLP client retry behavior
  during export;
- verify the exported sealed window is internally consistent by row count and
  min/max timestamp.

Decision output: whether v1 can export live, or should require stopped/offline
export.

### 3. Chunk sizing

Create stores at roughly 1 GiB, 4 GiB, and 8 GiB logical/archive size if
practical.

- measure export duration, compression ratio, disk write rate, and peak RSS;
- test day-sized chunks and hourly fallback chunks;
- determine whether `system.parts` can estimate bytes per table/time partition
  accurately enough to target 8 GiB.

Decision output: default target chunk size and expected operator guidance for
16 GiB RAM machines.

### 4. Independent scan path

For each candidate format:

- scan the exported logs for a body substring and service name over a multi-month
  range;
- scan traces for first/last occurrence of a status/error pattern;
- scan metrics for a metric-name threshold/regression window;
- record exact DuckDB, clickhouse-local, jq, or other commands.

Decision output: minimum viable "historical investigation" workflow.

### 5. Hot pruning

After a chunk validates, test pruning the archived range from a disposable hot
store.

- test `ALTER TABLE ... DROP PARTITION` for raw tables;
- test `ALTER TABLE ... DELETE WHERE ...` if partition drops are insufficient;
- enumerate all derived tables that would retain data for the same time range;
- verify `OPTIMIZE` or TTL materialization behavior if needed;
- measure reclaimed disk and latency impact.

Decision output: whether pruning should ship in v1, stay manual, or rely on
existing TTLs initially.

### 6. Catalog and reconciliation

Simulate crashes at each archive phase:

- before manifest;
- after manifest but before checksum;
- after checksum but before promotion;
- after promotion but before catalog append.

Decision output: exact `building/current/catalog` state machine and recovery
rules.

### 7. Schema compatibility

Create one chunk, alter the local schema in a controlled way, then create
another.

- prove old chunks remain independently queryable;
- define how the catalog reports incompatible chunks;
- decide whether query helpers should project common columns or fail closed.

Decision output: manifest fields and compatibility policy.

## Proposed First Implementation

If the research confirms Parquet `INTO OUTFILE` is safe:

1. Add `maple archive export --archive-dir <path> --before <time>
[--target-chunk-bytes 8GiB]`.
2. Add a server-owned archive admin path, not `/local/query`, so export can write
   files directly without returning multi-GB result buffers.
3. Export raw tables only.
4. Write manifests and `catalog.jsonl`.
5. Add `maple archive list` for catalog inspection.
6. Leave `prune-hot` behind an explicit flag or follow-up PR.

If Parquet export is not safe:

1. Export bounded NDJSON batches through Maple and compress them as they are
   written.
2. Keep the same manifest and catalog layout.
3. Document DuckDB/clickhouse-local/jq scan commands for operators.

If live export is not safe:

1. Add `maple archive export --offline --data-dir <path> --archive-dir <path>`.
2. Require Maple to be stopped.
3. Open chDB directly, export sealed windows, close cleanly.
4. Treat live archive export as a later optimization.

## Adoption Questions

- How much hot data should Maple keep once cold archives exist?
- Should archive export run automatically, or only through an operator command?
- Is 8 GiB the right default chunk target for 16 GiB RAM machines, or should the
  default be smaller and operator-tuned?
- How much service interruption is acceptable if live export blocks the single
  chDB connection?
- Should the archive root be required to live outside the hot data volume?
- Should Maple ever prune hot data automatically, or only after explicit
  operator confirmation?
- Is Parquet a support burden across Maple schema evolution, or is NDJSON a
  better long-term contract despite larger files and slower scans?
- Do we need a future archive query command, or is a manifest plus standard
  external tools sufficient?
