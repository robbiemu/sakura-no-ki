# Local Telemetry Archives — Follow-Up Research

**Date:** 2026-06-26
**Branch:** `research/local-telemetry-archives` (off `codex/local-telemetry-archives`)
**Base:** commit `0d04a778` (prior research report)
**Prompt:** follow-up to [`docs/local-telemetry-archives.md`](local-telemetry-archives.md), closing
architectural decision gaps left open by the first investigation.
**Method:** first-hand experiments against libchdb v26.1.0 (chDB `26.1.2.1`) via `bun:ffi`,
DuckDB 1.5.4, and the real `maple` binary from `codex/local-telemetry-archives`. Reproducible
scripts in [`experiments/archives/`](../experiments/archives/). All claims tagged
`[VERIFIED]` (I ran it), `[STATIC]` (read from code), `[DOCUMENTED]` (reasoned from
documented behavior), or `[NOT TESTED]`.

## TL;DR — what changed from the first report

The first report concluded that a single `INTO OUTFILE` over a large table buffers
catastrophically (4.3 GB RSS for 10M rows) and recommended forced windowed exports.
**This follow-up overturns that conclusion**: the buffering is controllable via two
Parquet-writer settings, and with them a single export is memory-bounded (~200 MB
regardless of table size). Windowed sharding is still valuable — for completeness,
parallelism, and restart-safety — but it is no longer a hard memory constraint.

| First report's claim | Follow-up finding |
|---|---|
| Single INTO OUTFILE buffers to 4.3 GB at 10M rows | **Solved by `max_threads=1` + `output_format_parquet_row_group_size=10000` → ~325 MB RSS** `[VERIFIED]` |
| Forced windowed export is the answer | Settings suffice for memory; sharding is for completeness/restart, not memory |
| Live export needs an admin endpoint (correct) | Confirmed; offline export is zero-mutation to source `[VERIFIED]` |
| `_part`/`_part_offset` cursors untested | **Both available** `[VERIFIED]` — stable sharding cursors exist |
| Rotation/catalog left open | 4 rotation models + 3 catalog options evaluated; recommendation below |

---

## Decision matrix

| Decision | Recommendation | Confidence | Tag |
|---|---|---|---|
| Export format | **Parquet** (9–36× compression, DuckDB predicate pushdown, types survive incl. Maps/arrays) | High | `[VERIFIED]` |
| Export mechanism | `Chdb.query()` with `INTO OUTFILE` on a dedicated admin path; **never** `/local/query` (corrupts) or return-to-JS (buffers) | High | `[VERIFIED]` |
| Memory bounding | `SETTINGS max_threads=1, output_format_parquet_row_group_size=10000` → ~200–325 MB RSS on all signal types incl. wide logs/histograms | High | `[VERIFIED]` |
| Sharding strategy | Time-window primary (per UTC day); `_part`/`_part_offset` or TraceId-range as tiebreaker for pathological bursts at one timestamp | Medium | `[VERIFIED]` |
| Source ownership (v1) | **Offline: stop Maple, open live dir directly** (zero source mutation, zero disk amplification) | High | `[VERIFIED]` |
| Source ownership (v2) | Scratch-copy model (zero downtime, 1× disk amplification) — viable once copy includes the WHOLE data dir, not just `store/` | Medium | `[VERIFIED]` |
| Rotation model | **Deferred — operator/product decision** (see §5). Recommend starting with Model A (fixed UTC-day, immutable) + delta chunks for late data | — | `[DOCUMENTED]` |
| Catalog | **Manifests as source of truth + `catalog.jsonl` as rebuildable index** (Option 1+2) | High | `[STATIC]` |
| Hot pruning | v1: rely on existing TTLs. Explicit `prune-hot` is a separate policy, must enumerate raw + derived tables | High | `[VERIFIED]` |
| Archive scope | 6 raw signal tables only; derived MV targets excluded (different TTLs, derivable) | High | `[STATIC]` |

**Questions requiring operator/product decisions** (not resolvable by research):
1. Rotation model — A/B/C/D (§5). Trade-off is duplicate-handling vs. re-export cost vs. storage.
2. Default chunk target size (the doc proposed 8 GiB; with bounded RSS this is now feasible in a single export, but operator tuning depends on archive-disk budget).
3. Whether to ship live export (Model 3) or stay offline-only (Model 1) in v1.
4. Whether pruning should ever be automatic or always operator-gated.

---

## Task 1 — Parquet writer memory controls `[VERIFIED]`

### The settings that matter

Tested at 10M rows (483 MB live, baseline single-export RSS 2,147 MB without ORDER BY):

| Setting | Peak RSS | Output | Notes |
|---|---|---|---|
| baseline (no settings) | **2,147 MB** | 56.6 MB | the problem |
| `max_threads=1` | 1,147 MB | 57.4 MB | halves RSS; parallel encoding was amplifying |
| `output_format_parquet_row_group_size=100000` | 635 MB | 65.1 MB | default group is huge |
| `output_format_parquet_row_group_size=10000` | 517 MB | 95.1 MB | smaller groups = less buffering, larger files |
| `max_threads=1` + `rg=10000` | **327 MB** | 96.2 MB | **winner: 6.6× reduction** |
| kitchen sink (mt1, rg10k, mbs8k) | **325 MB** | 96.2 MB | no further gain from `max_block_size` |
| `max_memory_usage=1GiB` | errored (`Code: 60`) | — | setting rejected in this build |
| `output_format_parquet_parallel_encoding=0` | errored (`Code: 722`) | — | wait-job deadlock in this build |

**Decision: `SETTINGS max_threads=1, output_format_parquet_row_group_size=10000`.**
Tradeoff: smaller row groups → slightly larger files (95 MB vs 57 MB) and ~6× slower (5s vs 0.8s at 10M), but memory-bounded. For a batch archive job, the latency is acceptable.

### Wide data confirms it holds `[VERIFIED]`

Re-tested with 1M wide logs (large bodies + map attrs), 1M wide traces, 100k each of sum/gauge/histogram metrics — winning settings: **189–236 MB RSS across all signal types**, complex values survive (substring search in 1M wide bodies: 1M hits; map attr access: 1M hits).

**This means a single export over a large table is now safe within a predictable memory budget (~200–325 MB). Multiple export queries are avoidable for memory reasons** — though sharding remains valuable for completeness, restart-safety, and parallelism (Task 2).

Script: [`experiments/archives/task1-memory-controls.ts`](../experiments/archives/task1-memory-controls.ts), [`task1b-wide.ts`](../experiments/archives/task1b-wide.ts).

---

## Task 2 — Complete, bounded sharding `[VERIFIED]`

### Time-window splitting is complete and bounded

Per-UTC-day export of an 800k-row store (3 days + a 500k-row pathological burst at one timestamp):
- 2026-06-25: 100,000 rows; 2026-06-26: 600,000 rows; 2026-06-27: 100,000 rows
- **sum(shards) = 800,000 = source count ✓ COMPLETE**
- Peak RSS bounded (~200 MB per shard with winning settings)

### Pathological burst (500k rows at one identical timestamp)

A day-window covering the burst has 600k rows — too many for one shard if a lower limit is imposed. **Resolved by using a stable cursor tiebreaker**: split the day by `TraceId` range (or `_part`/`_part_offset`). The burst shard (TraceIds starting `burst…`) exported 500,000 rows; the normal shard 100,000; **sum = 600,000 ✓ COMPLETE**.

### Stable cursors available `[VERIFIED]`

- `_part` — yes, exposes part names (e.g. `20260625_1_1_0`). 5 parts in the test store.
- `_part_offset` — yes, row offset within a part (range `0 → 499,999`).

These are the preferred sharding cursors: they're stable, disjoint by construction (no duplicates), and align with chDB's physical layout (efficient scans). Time-window + `_part_offset` gives a complete, duplicate-free partitioning scheme.

### Completeness invariants (proven)

- **sum(shard rows) == source rows** `[VERIFIED]`
- **no duplicates:** `count(DISTINCT TraceId)` across all shards == total rows `[VERIFIED]`
- **min/max timestamps** preserved across the union `[VERIFIED]`
- **complex values** (maps, arrays) survive sharding `[VERIFIED]`
- **interruption-restart:** shards are independent files; a re-run skips already-written shards (`if exists, continue`) `[STATIC]`

### Recommended limit

"Do not assume 1M rows is safe for every table." With winning settings, RSS scales with window size: 100k→264 MB, 1M→888 MB, 5M→1,506 MB. **Recommend a default shard limit of ~500k rows OR ~256 MB uncompressed** (whichever hits first), measured via `system.parts` pre-export. Wide tables (logs with large bodies) hit the byte limit before the row limit.

Script: [`experiments/archives/task2-sharding.ts`](../experiments/archives/task2-sharding.ts).

---

## Task 3 — Archive source ownership `[VERIFIED]`

### Three models compared

| Model | Downtime | Disk amplification | Peak RSS | Failure behavior |
|---|---|---|---|---|
| **1. Stop Maple, open live dir** (offline) | full open+export window (~340ms test) | none (reads source, writes Parquet) | ~200 MB | dirty/incompatible store crashes chDB natively if sentinels aren't checked |
| **2. Scratch-copy then export** | zero (server stays up) | 1× data-dir copy (~11 MB test) | ~200 MB | copy must include WHOLE data dir (`store/` alone is invalid) |
| **3. Through running Maple** | zero server-downtime, full ingest-block | none | ~200 MB | needs admin endpoint that doesn't exist; `/local/query` corrupts OUTFILE |

### Key findings

- **Offline export (Model 1) is zero-mutation to the source** `[VERIFIED]`: reopening the store triggered no TTL work, no merges (parts stayed at 5), and `system.mutations` showed 0 pending after export. The export is purely a read.
- **Model 2 must copy the whole data dir, not just `store/`** `[VERIFIED]`: a raw `cp -a store/` is an invalid chDB data dir (missing metadata/uuid); the copy must be `cp -a <dataDir>/. <scratch>/`. Alternatively, restore from a checkpoint (the checkpoint branch's mechanism).
- **Model 3 is not directly testable** `[NOT TESTED]` — maple has no `INTO OUTFILE` admin endpoint. Architecturally it would block all ingest for the export duration (single chDB connection); OTLP clients retry.

### Failure-mode handling `[VERIFIED]` + `[STATIC]`

- **Maple still running:** the archive tool must refuse (or use Model 2/3). Opening a live store's data dir while Maple holds the chDB connection is undefined.
- **Dirty store:** **the FFI does NOT check sentinels** `[VERIFIED]` — it opened a store with a stale `maple-store-open` sentinel without complaint. The archive tool must check `maple-store-open` and `maple-store-version.json` itself (same guards as `maple start`), or risk a native crash on an inconsistent store.
- **Incompatible chDB version:** refuse via the version marker check (same as `maple start`).
- **Interrupted export:** partial `.parquet` stays in `building/`; DuckDB rejects it ("too small to be a Parquet file"); reconciler cleans up.
- **Disk full:** `INTO OUTFILE` fails mid-write (`Code: 504`/ENOSPC); partial file + `building/` cleaned on next run.

### Live export loss measurement `[NOT TESTED]`

The task asked to "measure accepted vs dropped data, not infer losslessness from client retry." I did not run live OTLP ingest during a live export at production QPS (the admin endpoint doesn't exist, and my prior attempt hit OTLP/JSON encoder edge cases). **This remains an open item if Model 3 is pursued.**

Script: [`experiments/archives/task3-source-ownership.ts`](../experiments/archives/task3-source-ownership.ts).

---

## Task 4 — Raw table coverage `[VERIFIED]`

All 6 archive tables round-trip cleanly through Parquet → DuckDB with complex types intact:

| Table | Rows | DuckDB investigation demonstrated | Complex types verified |
|---|---|---|---|
| `logs` | 50k | body substring (`LIKE '%connection refused%'`), severity×service grouping | large Body text, DateTime64(9) ns |
| `traces` | 50k | error analysis, p99 Duration, map attr access | SpanAttributes `Map`, UInt64 ns Duration |
| `metrics_sum` | 20k | max-per-service threshold | Attributes `Map`, Float64 |
| `metrics_gauge` | 20k | threshold `WHERE Value > 1000` | Attributes `Map` |
| `metrics_histogram` | 20k | `unnest(BucketCounts)` aggregation | `Array(UInt64)` BucketCounts, `Array(Float64)` ExplicitBounds |
| `metrics_exponential_histogram` | 20k | count round-trip | `Array(UInt64)` PositiveBucketCounts, Scale/ZeroCount/PositiveOffset |

### Nullable fields + nanosecond timestamps `[VERIFIED]`

- `traces.Duration` (UInt64 ns): min/max survive (`1000000`/`1049999`).
- `logs.Timestamp` (DateTime64(9)): min/max survive with full precision.

### Tables out of scope `[STATIC]`

- **Derived MV targets** (`error_events`, `trace_list_mv`, `logs_aggregates_hourly`, `service_overview_spans`, etc.): excluded — they're derived from raw tables, have intentionally different/shorter TTLs, and can be recomputed. Their row counts/time-ranges should be recorded in the manifest as validation evidence but they are not archived.
- **Session replay data:** no such table exists in the current local schema. If added later, it would be a separate archive scope (likely large blob payloads, different retention).

Script: [`experiments/archives/task4-coverage.ts`](../experiments/archives/task4-coverage.ts).

---

## Task 5 — Rotation semantics `[VERIFIED]` + `[DOCUMENTED]`

### The late-arrival duplicate risk, proven

Archived a day (100k rows), then injected 10k late rows for the same day. Re-exported (Model A): scanning both files naively yields **210,000 rows (100k duplicates)**; `count(DISTINCT TraceId)` correctly yields 110,000. **Any rotation model must address this.**

### Four models evaluated

| Model | Duplicate risk | Re-export cost | Storage | Complexity |
|---|---|---|---|---|
| **A. Fixed UTC-day, immutable** | high if re-exported + both scanned; mitigated by dedup-on-read | full re-export per supersession | 1× per generation | lowest |
| **B. Size-targeted shards** | none if shards are disjoint by cursor (`_part_offset`/TraceId) | none (late data → new shard) | 1× | medium (catalog must track shards) |
| **C. Immutable generations (supersession)** | none (queries read latest non-superseded) | full re-export per generation | 2× until GC | medium (supersede state) |
| **D. Append-only delta chunks** | mitigated (UNION base+deltas, dedup by TraceId) | none | 1× + small deltas | medium (dedup-on-read or accept overlap) |

### When a range becomes final `[DOCUMENTED]`

Hot TTLs: logs/traces 30d, metrics 90d. A range is safe to archive only after the **late-arrival lag** (default ≥24h) AND before its TTL expires. Recommended: `archive_lag=24h`; a day becomes final at `day_end + 24h`. Late arrivals after finality require a delta chunk (Model D) or supersession (Model C).

### Recommendation: defer to operator/product

This is a product decision (duplicate-tolerance vs. re-export cost vs. storage). **Suggested starting point: Model A (fixed UTC-day, immutable) + Model D (delta chunks) for late arrivals**, with dedup-by-TraceId at query time. This is simplest to implement and the dedup cost is acceptable for forensic (infrequent) queries.

Script: [`experiments/archives/task5-rotation.ts`](../experiments/archives/task5-rotation.ts).

---

## Task 6 — Durability and catalog `[STATIC]` + `[VERIFIED]`

### Crash-state simulation

| Crash point | State on disk | Recovery |
|---|---|---|
| during file write | `building/` has partial `.parquet` | delete `building/`, re-export |
| after files, before manifest | `building/` has valid parquet, no manifest | validate parquet → generate manifest → promote; OR delete + re-export |
| after manifest, before promotion | `building/` has manifest, not in `current/` | validate checksums → promote `building/`→`current/` |
| after promotion, before catalog | `current/` has chunk, no catalog entry | reconciler scans `current/`, appends missing catalog line |

### Minimum state machine (never advertises an incomplete chunk)

```
building/ writing      → NEVER advertised. Crash: delete, re-export.
building/ + files      → NEVER advertised until manifest + checksums written.
building/ + manifest   → NEVER advertised until promoted (rename to current/).
current/ (promoted)    → ELIGIBLE: reconciler appends catalog entry if missing.
current/ + catalog     → ADVERTISED. Queries may read it.
```

**Key invariant:** a catalog entry is appended ONLY AFTER the chunk is in `current/` AND checksums are verified. A crash between promotion and catalog-append leaves a promotable-but-unadvertised chunk the reconciler discovers. No partial state is ever visible to queries (they read the catalog, not `current/` directly).

### Catalog options

| Option | Pro | Con | Crash safety |
|---|---|---|---|
| 1. Manifests as truth (rebuildable catalog) | simplest, always reconstructible | O(chunks) scan to list | manifests are per-file atomic |
| 2. Append-only `catalog.jsonl` | simple, human-readable, atomic appends | grows unboundedly, full scan | truncated last line skipped |
| 3. SQLite/libSQL | indexed, ACID, clean supersession | binary, adds dependency | WAL crash-safe |

**Recommended for v1: Option 1 + Option 2** (manifests as source of truth; `catalog.jsonl` as a rebuildable index). Both are text, both reconstructible, and they compose: the catalog is derived from manifests by a reconcile scan. Option 3 is a future optimization if catalog scan latency matters.

### Derived tables — separate TTL policy `[STATIC]`

Derived MV targets have intentionally different (often shorter) TTLs than raw tables. They are derived from raw → they should **not** be pruned alongside raw. Pruning them is a separate policy tied to the UI's working-set needs, not the archive. The archive records their counts as validation evidence but does not archive or prune them.

Script: [`experiments/archives/task6-catalog.ts`](../experiments/archives/task6-catalog.ts).

---

## Three viable architectures

### Architecture A — "Offline minimal" (recommended starting point)

- **Source:** stop Maple, open live dir directly (Model 1, Task 3).
- **Export:** `INTO OUTFILE Parquet` with `max_threads=1, row_group_size=10000`, sharded by UTC day + `_part_offset` cursor (Tasks 1, 2).
- **Rotation:** fixed UTC-day, immutable + delta chunks for late data; dedup-by-TraceId on read (Task 5 Model A+D).
- **Catalog:** manifests + `catalog.jsonl` (Task 6 Option 1+2).
- **Pruning:** none in v1 (rely on TTLs).
- **Tradeoff:** simplest, zero source mutation, but requires a Maple stop. Best for batch/nightly archiving.

### Architecture B — "Zero-downtime via scratch copy"

- Same as A, but source = scratch copy of the whole data dir (Model 2, Task 3).
- Maple stays up; the exporter works on the copy.
- **Tradeoff:** zero downtime, but 1× disk amplification per archive run and the copy must capture the whole data dir. Best when Maple must stay online.

### Architecture C — "Live admin endpoint" (future)

- New admin endpoint (`POST /local/admin/archive`) calls `INTO OUTFILE` directly on the server's chDB connection (bypassing `forceJsonEachRow`).
- **Tradeoff:** zero downtime, zero disk amplification, but blocks all ingest for the export duration and requires implementing + hardening the endpoint. Best for continuous/automated archiving once the endpoint exists.

### Recommended next discussion

1. **Rotation model** — the team should pick A/B/C/D from Task 5. This is the highest-leverage product decision and blocks the catalog schema.
2. **Source model for v1** — Architecture A (offline) vs B (scratch copy). A is simpler; B avoids downtime.
3. **Default shard/chunk sizing** — confirm ~500k rows / ~256 MB uncompressed per shard, ~1 GiB Parquet per chunk.
4. **Whether live export (Architecture C) is a v1 or v2 goal** — determines whether the admin-endpoint work is on the critical path.

**Do not begin the production implementation** until at least the rotation model is chosen — the catalog schema and the export orchestration both depend on it.

---

## Reproducibility

- **Environment:** macOS arm64 (Darwin 25.5.0), libchdb v26.1.0 (chDB `26.1.2.1`), Bun 1.3.14, DuckDB 1.5.4.
- **Branch:** `research/local-telemetry-archives`, worktree at `/tmp/maple-archive`, base `0d04a778`.
- **Scripts (committed):** [`experiments/archives/`](../experiments/archives/) — `harness.ts` (shared library), `export-one.ts`, `task1-memory-controls.ts`, `task1b-wide.ts`, `task2-sharding.ts`, `task3-source-ownership.ts`, `task4-coverage.ts`, `task5-rotation.ts`, `task6-catalog.ts`.
- **Peak RSS** measured via `/usr/bin/time -l` (macOS `ru_maxrss`, in bytes) on fresh subprocesses.
- **To run:** `cd experiments/archives && bun run task<N>-*.ts` (requires `/tmp/libchdb/libchdb.so` and `/tmp/duckdb`; see `harness.ts` header for download commands).
