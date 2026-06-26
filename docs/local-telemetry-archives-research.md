# Local Telemetry Archives — Research Findings

**Date:** 2026-06-26
**Branch:** `research/local-telemetry-archives` (off `codex/local-telemetry-archives`)
**Prompt:** [`docs/local-telemetry-archives.md`](local-telemetry-archives.md)
**Method:** First-hand experiments against libchdb **v26.1.0** (chDB `26.1.2.1`) driven through
`bun:ffi` with Maple's real schema, plus the real `maple` binary from the
`codex/local-telemetry-archives` branch (which has `--chdb-config-file`, `maple checkpoint`,
and `maple restore`). All claims tagged `[VERIFIED]` (I ran it), `[STATIC]` (read from code),
or `[NOT TESTED]`.

## TL;DR — decision outputs

| Question | Decision |
|---|---|
| Format | **Parquet via `INTO OUTFILE`** — 9–36× compression, DuckDB-native with predicate/projection pushdown, schema/Types survive (including `Map(varchar,varchar)` attributes). |
| Export mechanism | **`Chdb.query()` with `INTO OUTFILE` on a dedicated admin path** — NOT `/local/query` (its `forceJsonEachRow` corrupts the output into an 8-byte JSON file) and NOT return-to-JS `Chdb.query()` (buffers the full result: **8.5 GB RSS for 1.1 GB output**). |
| Live vs offline export | **Offline (stop → export → restart) for v1.** Live export is architecturally possible (chDB serializes, so it's consistent) but blocks the whole server for its duration and there's no admin endpoint today. |
| **Multi-GB export safety** ⚠ | **Single `INTO OUTFILE` over a large table BUFFERS — 4.3 GB RSS for a 50 MB Parquet at 10M rows.** Must use **windowed/batched export** (one `INTO OUTFILE` per time window ≤ ~1M rows) to stay memory-bounded. This is the single most important finding. |
| Chunk target size | **Default ~1 GiB Parquet per chunk, split into windowed shards of ≤1M rows each.** The 8 GiB target is reachable only via windowed export; a single-shot 8 GiB export would OOM a 16 GB machine. |
| Hot pruning | **v1: rely on existing TTLs.** `DROP PARTITION` works but **must enumerate ALL derived MV target tables** (they retain data after the source is pruned). Ship explicit `prune-hot` later, gated. |
| Catalog/reconciliation | Append-only `catalog.jsonl` + reconcile-by-scan (mirrors the proven checkpoint promotion pattern). |
| Schema compat | Chunks carry schema fingerprint; DuckDB `union_by_name=true` merges evolved chunks (NULL-fill); default fails closed. |

---

## Task 1 — Export primitives

### What works [VERIFIED]

- **`SELECT * INTO OUTFILE '<path>' FORMAT Parquet`** works in chDB local mode and produces a valid **Apache Parquet** file (verified with `file` + DuckDB readback). Types survive: `DateTime64` → `timestamp with time zone`, `Map(LowCardinality(String),String)` → `map(varchar,varchar)`, etc.
- **`SELECT * INTO OUTFILE '<path>' FORMAT JSONEachRow`** works and produces NDJSON.
- **Compression by extension**: appending `.zst` (e.g. `out.parquet.zst`) makes chDB zstd-compress the output (verified: `file` reports "Zstandard compressed data"). **BUT** DuckDB's `read_parquet()` cannot read a zstd-wrapped Parquet directly (magic bytes differ) — it requires `zstdcat | duckdb`. So **use plain `.parquet`** (Parquet is already internally compressed via Snappy/Zstd column encoding; double-wrapping adds nothing and breaks DuckDB).
- **`SETTINGS compression_method`** is **not a valid setting** in this chDB build (`Code: 115 Unknown setting`). Use the file-extension trick or rely on Parquet's internal compression.

### What fails [VERIFIED]

- **`/local/query` corrupts `INTO OUTFILE`.** The handler's `forceJsonEachRow` appends `FORMAT JSONEachRow` after the statement's `FORMAT Parquet`, and chDB honors the *last* FORMAT clause — producing an **8-byte JSON file labeled `.parquet`**. This confirms the archive doc's warning: **export must use a dedicated admin path, never `/local/query`.**
- **Return-to-JS (`Chdb.query()` without `INTO OUTFILE`) buffers catastrophically.** At 2M rows the JSONEachRow result string is 1.1 GB; peak process RSS hit **8.5 GB** (Bun/V8 overhead + the `chdb_result_buffer` copy). This is the existing `/local/query` + `Chdb.query()` path and it is **unsafe for any non-trivial export**.

### Decision: Parquet via `INTO OUTFILE` on a dedicated admin path

---

## Task 2 — Live export safety

### Findings [VERIFIED]

- **Offline export via direct `Chdb.query()` → `INTO OUTFILE Parquet` works** and produces a consistent point-in-time snapshot. Verified: 100k traces exported, chDB count (100000/20000 errors) **matches DuckDB count exactly**.
- **`/local/query` cannot host export** (Task 1: corrupts the FORMAT).
- **Live export blocking is architectural:** chDB is single-connection + synchronous FFI, so any chDB op (export or otherwise) blocks the entire server. Verified earlier with `SELECT sleep(3)`: `/health` was blocked for ~1.8s while a chDB op ran. During a live export, OTLP clients would time out and **retry** (standard OTEL SDK behavior) — no data loss, just latency.
- **Consistency is guaranteed by construction:** because chDB serializes all operations on one JS thread, an export sees a quiescent snapshot — no mid-export mutation is possible. The exported sealed window is internally consistent by row count and min/max timestamp.

### Decision: offline export for v1; live export feasible but needs a dedicated admin endpoint

A live export isn't unsafe (it's consistent), but it blocks ingest for its full duration and there's no admin endpoint to run it through. v1 should use `maple archive export --offline --data-dir <path>` (stop → export → restart). Live export is a later optimization requiring a new admin endpoint that calls `INTO OUTFILE` directly (bypassing `forceJsonEachRow`).

---

## Task 3 — Chunk sizing and the multi-GB memory problem ⚠ (most important finding)

### The single-export memory cliff [VERIFIED]

I initially concluded (Task 1, at 2M rows) that Parquet `INTO OUTFILE` streams. **At scale that's wrong.** Scaling the export reveals a memory cliff:

| Rows (single export, no ORDER BY) | Live store | Parquet output | Peak RSS |
|---|---|---|---|
| 500k | 95 MB | 2.6 MB | 599 MB |
| 2M | 190 MB | 10.4 MB | 1,613 MB |
| 5M | 296 MB | 26.0 MB | 2,778 MB |
| **10M** | **483 MB** | **50 MB** | **4,349 MB** (4.3 GB) |

A **50 MB Parquet output consuming 4.3 GB RSS** is catastrophic for a 16 GB machine. For comparison, `SELECT count() FROM traces` over the same 10M rows (a full scan returning 1 row) peaks at **199 MB** — so the scan itself streams fine; it's the Parquet/JSON **writer** that buffers.

### Root cause: two components [VERIFIED]

| Component | Cost at 10M rows |
|---|---|
| `ORDER BY TraceId` sort buffer | ~2.3 GB (4,349 → 2,047 MB when removed) |
| Parquet writer row-group accumulation | ~2.0 GB (remains even without ORDER BY) |

JSONEachRow OUTFILE buffers too (4,239 MB at 10M). The buffering is **not Parquet-specific** — it's how chDB's OUTFILE pipeline works at scale.

### The fix: windowed export [VERIFIED]

RSS scales with **window size, not total store size.** Exporting bounded windows keeps memory bounded:

| Window (WHERE range, no ORDER BY) | Output | Peak RSS |
|---|---|---|
| 100k rows | 0.6 MB | **264 MB** (near baseline) |
| 1M rows | 5.4 MB | 888 MB |
| 5M rows | 28.2 MB | 1,506 MB |
| 10M (full) | 50 MB | 2,047 MB |

**Decision:** the archive exporter must **NOT** do a single `SELECT * INTO OUTFILE` over a large table. Instead, iterate over bounded time windows (≤ ~1M rows each), writing one Parquet file per window, and concatenate them in the chunk manifest. A chunk targeting 1 GiB Parquet would be ~10–20 windowed files. This keeps peak RSS under ~1 GB regardless of total archive size.

### `system.parts` estimation [VERIFIED]

`SELECT count(), formatReadableSize(sum(bytes_on_disk)) FROM system.parts WHERE table='traces' AND active` accurately reports part count and compressed size — usable for pre-export chunk sizing. At 10M rows it reported `12 parts, 105.36 MiB`, matching `du` (483 MB uncompressed store / ~105 MB compressed parts). Use this to decide window splits before exporting.

### Decision: windowed export, ~1M rows per shard, ~1 GiB Parquet per chunk

---

## Task 4 — Independent scan path [VERIFIED]

DuckDB 1.5.4 reads the exported Parquet directly with full optimization:

```sql
-- row count + time range
SELECT count(), min(Timestamp), max(Timestamp) FROM read_parquet('/archive/.../traces.parquet');

-- predicate pushdown (verified in EXPLAIN: "Filters:" pushed to READ_PARQUET)
SELECT count() FROM read_parquet('...') WHERE StatusCode='Error';

-- projection pushdown (only reads needed columns)
SELECT count() FROM (SELECT Timestamp, TraceId FROM read_parquet('...'));

-- forensic substring search
SELECT count() FROM read_parquet('...') WHERE StatusMessage LIKE '%timeout%';

-- multi-month aggregation
SELECT ServiceName, min(Timestamp), max(Timestamp), count()
FROM read_parquet('/archive/chunks/current/*/traces.parquet')
GROUP BY ServiceName;
```

Glob patterns over `current/*/traces.parquet` work — DuckDB scans all matching chunks in one query. **This is the minimum-viable historical-investigation workflow** and requires no Maple involvement.

NDJSON fallback (if Parquet were ever unavailable): `zstdcat .../traces.ndjson.zst | jq 'select(...)'` works but is far slower (no predicate pushdown, full scan).

---

## Task 5 — Hot pruning [VERIFIED]

- **`ALTER TABLE traces DROP PARTITION '<date>'` works** (partition key is `toDate(Timestamp)`). Removes all rows for that date instantly.
- **⚠ Derived MV target tables RETAIN data after the source is pruned.** After dropping the `traces` partition: `error_events` still had 20000 rows, `trace_list_mv` 100000, `service_overview_spans` 100000. **Pruning must enumerate ALL derived tables** and drop their matching partitions, or the hot store retains stale aggregates that disagree with the (now-pruned) raw data.
- **`OPTIMIZE TABLE ... FINAL` reclaims no additional space** after a DROP PARTITION (the parts are already gone; leftover is system/metadata).
- `DELETE WHERE` is the fallback if partition boundaries don't align with the archive window, but it's a heavyweight mutation (creates a new part via mutation).

### Decision: v1 relies on existing TTLs. Explicit `prune-hot` ships later, gated, and MUST drop partitions across raw + all derived tables atomically.

---

## Task 6 — Catalog and reconciliation [STATIC + design]

The crash-state machine mirrors the proven checkpoint promotion pattern:

| State on crash | Recovery action |
|---|---|
| `building/` only (no manifest) | Incomplete — delete `building/`, re-export |
| `building/` + manifest, not promoted | Verify checksums; if valid promote `building/`→`current/`, else delete |
| `current/` exists, no catalog entry | Reconcile: scan `current/`, verify checksums, append missing `catalog.jsonl` entry |
| `current/` + catalog entry | Healthy — no action |

A `maple archive reconcile` command scans `current/` chunks, verifies each `checksums.sha256`, and appends any missing catalog entries. Append-only `catalog.jsonl` is safe against partial writes (a truncated last line is skipped; complete lines are valid).

---

## Task 7 — Schema compatibility [VERIFIED]

- Adding a column (`ALTER TABLE traces ADD COLUMN NewField String DEFAULT 'added'`) and exporting produces a chunk with the new column; the old chunk lacks it.
- **DuckDB `union_by_name=true` merges them** (NULL-fills `NewField` for old-chunk rows):
  ```sql
  SELECT count(*), count(NewField) FROM read_parquet(['chunkA.parquet','chunkB.parquet'], union_by_name=true);
  ```
- **Without `union_by_name`, DuckDB fails closed** (column mismatch error) — safe default.
- The manifest must record `schemaFingerprint` + column list; query helpers project a common column subset across evolved chunks or fail closed.

---

## Revised implementation recommendation

Given Task 3's memory cliff, the archive doc's "Proposed First Implementation" needs one change: **the exporter must window the export**. Updated plan:

1. `maple archive export --archive-dir <path> --before <time> [--target-chunk-bytes 1GiB] [--window-rows 1000000]`
2. For each sealed day/hour window:
   - Query `system.parts` to estimate size; split to hourly if a day exceeds target.
   - **Iterate sub-windows of ≤ `--window-rows`**, running `SELECT * WHERE <time range> INTO OUTFILE '<shard>.parquet' FORMAT Parquet` per sub-window.
   - Concatenate shard paths in the chunk manifest.
3. Validate: row counts (chDB vs DuckDB per shard), min/max timestamps, checksums.
4. Promote `building/` → `current/`, append `catalog.jsonl`.
5. `maple archive list` for catalog inspection.
6. `prune-hot` behind a flag in a follow-up; enumerate raw + derived tables.

**Critical constraints for the implementer:**
- **Never** export via `/local/query` or via return-to-JS `Chdb.query()`. Use a dedicated admin path that calls `INTO OUTFILE` directly.
- **Never** single-shot export a large table. Always window.
- **Drop `ORDER BY`** from export queries — it doubles RSS for no archive benefit (chunks are scanned by time range later).
- Pruning must touch derived MV tables, not just raw.

---

## Reproducibility

Experiments ran on macOS arm64 (Darwin 25.5.0), libchdb v26.1.0 (chDB `26.1.2.1`), Bun 1.3.14,
DuckDB 1.5.4, against the `codex/local-telemetry-archives` branch (worktree at `/tmp/maple-archive`).
Scripts in `/tmp/`: `arch-export.ts`, `arch-buffering.ts`, `arch-iso.ts`, `arch-iso-export.ts`,
`arch-sizing2.ts`, `arch-live-codex.ts`, `win-export.ts`, `build-and-measure.ts`, `ffi2.ts`.

Peak RSS measured via `/usr/bin/time -l` (macOS `ru_maxrss`, in bytes) on fresh processes —
the only reliable way to catch fast memory spikes (`ps` polling misses them).
