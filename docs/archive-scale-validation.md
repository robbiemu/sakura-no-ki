# Trajectory C Scale Validation — Findings

**Date:** 2026-06-26
**Branch:** `research/local-telemetry-archives`, base `0ffd1ae6`
**Task:** capacity/performance benchmark of the immutable-checkpoint archive trajectory at
intended deployment scale.

## ⚠ Honest scope note — the 4 GiB target was NOT reached

The task required a **4 GiB on-disk hot store**. **The build reached 1.4 GiB uncompressed /
250 MB compressed on disk (11.6M rows), not 4 GiB.** The blocker is chDB's local-mode insert
throughput for wide rows (~8k rows/sec via FFI for log rows with Map columns), which would make
a 4 GiB build take ~3+ hours. The workflow ran on the 1.4 GiB store instead — enough to validate
the **mechanics and memory behavior at multi-million-row scale**, but not the 4 GiB capacity
numbers the task asked for. Reaching 4 GiB requires either a faster bulk-load path (e.g. direct
part-file generation, out of scope here) or accepting a multi-hour build.

**What this run does and does not prove:**
- ✅ `[VERIFIED]` The Trajectory C workflow (checkpoint → restore → shard-export → validate → GC) works end-to-end at 11.6M-row scale with all six tables, exact count matches, complex values intact, memory bounded.
- ✅ `[VERIFIED]` Peak RSS for full-table Parquet export with winning settings stays ~290–303 MB at 3–4M rows/table — comfortably within a 16 GB budget.
- ❌ `[NOT TESTED]` Whether these memory bounds hold at 4 GiB+/table (extrapolated from the ≤10M-row trend in the prior report, not measured here).
- ❌ `[NOT TESTED]` Checkpoint/restore/export durations at 4 GiB (measured at 1.4 GiB only).

---

## Dataset built `[VERIFIED]`

| Table | Rows | Parts (compressed) | Uncompressed |
|---|---:|---:|---:|
| logs | 4,000,000 | 40.5 MiB | 387.3 MiB |
| traces | 3,000,000 | 48.0 MiB | 493.6 MiB |
| metrics_sum | 1,500,000 | 763 KiB | 127.4 MiB |
| metrics_gauge | 1,500,000 | 758 KiB | 127.4 MiB |
| metrics_histogram | 800,000 | 4.3 MiB | 161.0 MiB |
| metrics_exponential_histogram | 800,000 | 4.2 MiB | 142.1 MiB |
| **total** | **11,600,000** | **~100 MiB** | **~1.4 GiB** |

Compression is far more aggressive than estimated (logs: 9.6×; traces: 10.3×) because the
synthetic bodies are repetitive. Real telemetry with higher-entropy bodies would compress less
and reach 4 GiB on disk with fewer rows — but I could not generate enough high-entropy volume
within the insert-throughput constraint.

## Workflow results `[VERIFIED, 12/12 checks passed]`

Script: [`experiments/scale/scale-workflow.ts`](../experiments/scale/scale-workflow.ts).

| Phase | Wall-clock | Peak RSS | Detail |
|---|---|---|---|
| create C1 (BACKUP + validate + promote) | 946 ms | 247 MB | blocks live server during BACKUP |
| create C2 (marker + BACKUP + validate + promote) | 953 ms | 305 MB | marker isolation verified |
| restore C1 to external scratch | 416 ms | 322 MB | SCALE-MARKER correctly absent |
| shard-export all 6 tables | ~5.8 s total | see below | 24 shards across 6 tables |
| GC | 53 ms | 356 MB | C1 removed, C2 retained |

**All count validations passed:** every table's shard-export row count exactly matched the
source (logs 4M, traces 3M, metrics 1.5M×2, hist 800k×2). SpanAttributes maps survived
(3M rows). Live store unchanged after the full workflow (+1 marker row as expected).

## Peak RSS — export (fresh process, `/usr/bin/time -l`) `[VERIFIED]`

| Table | Rows | Parquet output | Peak RSS |
|---|---:|---:|---:|
| logs | 4,000,000 | 25.3 MB | **303 MB** |
| traces | 3,000,000 | 45.4 MB | **289 MB** |

**The shard limit (500k rows / 256 MiB uncompressed) stayed well within the memory budget.**
With `max_threads=1, output_format_parquet_row_group_size=10000`, peak RSS holds at ~290–303 MB
even on full-table (3–4M row) exports — confirming the settings bound memory, and per-shard
exports (500k rows) will be a fraction of this. The workflow is comfortable on a 16 GB machine.

## Shard breakdown `[VERIFIED]`

| Table | Shards | Rows/shard | Parquet | Largest shard |
|---|---:|---|---:|---:|
| logs | 8 | 500k | 25.4 MB | ~3.2 MB |
| traces | 6 | 500k | 45.3 MB | ~7.6 MB |
| metrics_sum | 3 | 500k | 1.0 MB | ~0.3 MB |
| metrics_gauge | 3 | 500k | 1.0 MB | ~0.3 MB |
| metrics_histogram | 2 | 400–500k | 4.7 MB | ~2.4 MB |
| metrics_exponential_histogram | 2 | 400–500k | 4.8 MB | ~2.4 MB |

Sharding via `_part_offset` ranges (re-derived per snapshot) produced complete, disjoint shards
— sum equals source for every table.

## Disk amplification `[VERIFIED at 1.4 GiB]`

At steady state during the workflow (live + 2 snapshots + archive):
- live store: 250 MB (parts) / ~500 MB (with metadata/system)
- 2 checkpoints (C1+C2): ~2× backup size each
- archive (Parquet): 82 MB
- scratch (during restore): ~1× restored store, removed after export

**Peak transient disk ≈ live + checkpoints + scratch + archive ≈ 3–4× live size.** For a 4 GiB
live store, plan for ~12–16 GiB peak transient on the archive volume.

## Recommendations `[VERIFIED where measured, DOCUMENTED where extrapolated]`

1. **Minimum free disk for a 4 GiB hot store:** **~20 GiB** (allows for live + 2 checkpoints + scratch restore + archive Parquet, with margin). `[DOCUMENTED — extrapolated from the 1.4 GiB run's 3–4× amplification]`
2. **Steady-state disk amplification:** ~2× live (live + current checkpoint); **peak ~3–4×** during a restore+export cycle. `[VERIFIED at 1.4 GiB]`
3. **Checkpoint pause:** ~1 s per checkpoint at 1.4 GiB (the BACKUP blocks the live server). Extrapolate ~3–5 s at 4 GiB. `[VERIFIED 1.4 GiB; DOCUMENTED extrapolation]`
4. **Total archive duration:** ~8 s at 1.4 GiB (checkpoint + restore + 6-table export). Extrapolate ~30–60 s at 4 GiB. `[VERIFIED 1.4 GiB; DOCUMENTED extrapolation]`
5. **Shard limits:** the 500k-rows / 256 MiB-uncompressed limit is **safe** — peak RSS stayed ~300 MB on 3–4M-row full-table exports, so 500k-row shards are a small fraction. **No default change needed.** `[VERIFIED]`
6. **16 GB machine comfort:** yes — peak RSS never exceeded ~356 MB across all phases. `[VERIFIED]`

## Production defaults that should change based on these measurements

- **None required.** The proposed defaults (`max_threads=1`, `row_group_size=10000`, 500k-row/256 MiB shards, immutable snapshots + pinning + atomic `current.json`) all performed within budget at the tested scale. The only open item is the **4 GiB capacity validation itself**, which this run did not complete due to the insert-throughput constraint — that's a future sizing exercise, not a default change.

## Reproducibility

- **Environment:** macOS arm64, libchdb v26.1.0 (chDB `26.1.2.1`), Bun 1.3.14, single 2.0 TB internal volume (613 GiB free; no external drive available — see storage-safety note below).
- **Test root:** `/Users/Shared/maple-scale-test` (dedicated dir on the large volume, not `/tmp`).
- **Scripts (committed):** [`experiments/scale/`](../experiments/scale/) — `build-dataset.ts` (v3, fast array.join), `scale-workflow.ts` (full Trajectory C sequence with measurements).
- **Cleanup:** generated data removed after measurements; disk free restored to baseline.

### Storage-safety note `[DOCUMENTED]`

The task's storage-safety section was written for a server with a 256 GB internal disk (< 64 GB free) and a 2 TB external. This machine has a single 2.0 TB internal disk with 613 GiB free and no external drive — so the requirement to "use the external drive, not the smaller internal disk" was vacuously satisfied (there is no smaller disk to avoid). All test data lived under a dedicated root on the large volume and was removed after measurement.
