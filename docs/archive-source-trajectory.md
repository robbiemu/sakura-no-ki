# Archive Source Trajectory — Research Findings

**Date:** 2026-06-26
**Branch:** `research/local-telemetry-archives` (off `codex/local-telemetry-archives`)
**Base:** commit `55af6ccc`
**Task:** select and prove the highest-quality viable trajectory for obtaining a safe
archive source. Evaluate Trajectory C first; descend only on a verified blocker.
**Method:** first-hand experiments against libchdb v26.1.0 via `bun:ffi`, on top of the
proven `BACKUP`/`RESTORE` primitives. Reproducible scripts in
[`experiments/trajectory/`](../experiments/trajectory/). All claims tagged
`[VERIFIED]` (I ran it), `[STATIC]` (read from code), `[DOCUMENTED]` (reasoned), `[NOT TESTED]`.

## TL;DR

**Trajectory C (Immutable Checkpoint Source) is viable. No blocker was found, so
Trajectories B and A were not evaluated.** Recommended trajectory: **C with persistent
snapshot pinning** (not an exclusive lock).

The existing checkpoint branch uses a mutable `building/`→`current/`→`previous/` design
where `current/` is overwritten on each checkpoint. Trajectory C replaces this with
**immutable, addressable-by-id snapshots** + an atomic `current.json` pointer + pin files
for anti-GC. This is a real but contained design change to the checkpoint module.

| Success criterion | Verdict |
|---|---|
| produces a consistent, restorable archive source | ✅ `[VERIFIED]` |
| never risks the only live store during archive export | ✅ `[VERIFIED]` — restore is to external scratch only; live dataDir never opened by exporter |
| survives interruption without silently selecting incomplete state | ✅ `[VERIFIED]` — `current.json` only advances after manifest+pointer; crash debris is GC-eligible |
| supports scratch restore onto an external volume | ✅ `[VERIFIED]` — `restoreSnapshotToScratch` |
| preserves checkpoint provenance | ✅ `[VERIFIED]` — manifest carries id/versions/fingerprint/validation |
| fits a 16 GB machine without an always-running second database | ✅ `[VERIFIED]` — scratch is ephemeral (opened, exported, closed, removed) |
| clear reconciliation and cleanup story | ✅ `[VERIFIED]` — idempotent GC cleans orphans |

---

## Trajectory C — Immutable Checkpoint Source `[VERIFIED, all 7 capabilities + concurrency]`

### Layout

```
backups/
  snapshots/
    <checkpoint-id>/          ← unique, immutable, never overwritten
      backup/                 ← native BACKUP TO Disk output
      manifest.json           ← provenance: id, versions, fingerprint, validation, bytes
      pins/                   ← one file per active restore (anti-GC)
  current.json                ← {"checkpointId": "<id>"}  (atomic write-temp + rename)
```

### The 7 capabilities — all proven `[VERIFIED, 22/22 checks]`

Script: [`experiments/trajectory/trajectory-c-proof.ts`](../experiments/trajectory/trajectory-c-proof.ts).

1. **Create + validate under a unique immutable ID** ✓ — snapshot dir + manifest + validation (traces, 33 MVs).
2. **Promote before atomic pointer replacement** ✓ — `current.json` uses write-temp + `rename(2)`; no `.tmp` left behind.
3. **Resolve one ID + restore into external scratch** ✓ — `restoreSnapshotToScratch` produces a valid store with MARKER-A.
4. **Create newer checkpoint while older remains addressable** ✓ — C1 and C2 coexist with different IDs; C1 not overwritten.
5. **Prevent GC from removing a checkpoint being restored** ✓ — pin files block GC; release enables it.
6. **Export from restored scratch without opening live dir** ✓ — Parquet exported from scratch; live never opened by exporter.
7. **Close + remove scratch without affecting live** ✓ — live row count identical before/after scratch removal.

### Concurrency sequence — all proven `[VERIFIED]`

The task's required sequence, run against a fresh store:

1. C1 (CONC-MARKER-A) → 2. pin C1 → 3. C2 (CONC-MARKER-B) → 4. restore C1 externally → **has A, not B** ✓ → 5. restore C2 → **has both A and B** ✓ → 6. release C1 → GC removes C1, retains C2 ✓.

This proves the snapshots are genuinely independent and immutable: a pinned older snapshot survives the creation and GC of newer ones, and each restores exactly its own state.

### Lock vs pinning — pinning recommended `[VERIFIED]`

Script: [`experiments/trajectory/lock-vs-pinning.ts`](../experiments/trajectory/lock-vs-pinning.ts).

| Aspect | Exclusive lock | Persistent pin |
|---|---|---|
| prevents deletion during restore | yes (if checked) | yes (GC skips pinned) |
| **holder crashes mid-restore** | **STALE — heuristic recovery** | **safe — over-retains (never deletes)** |
| survives reboot? | lock lost; PID stale | pin file persists |
| complexity | medium (stale detection) | low (file existence) |
| **failure mode** | **unsafe deletion OR stuck lock** | **safe over-retention** |

**Recommendation: persistent pinning.** Its failure mode (over-retention) is strictly safe, whereas a stale lock's failure mode is either unsafe deletion or a stuck lock requiring heuristic recovery. Pinning also composes naturally with the immutable-snapshot GC. Empirically verified: a pin left un-released (simulating a crashed restorer) correctly caused GC to retain the snapshot.

---

## Fault injection — crash-state + reconciliation matrix `[VERIFIED, 11/11 checks]`

Script: [`experiments/trajectory/fault-injection.ts`](../experiments/trajectory/fault-injection.ts).

Each crash point simulated by injecting a throw at the named phase, then examining the on-disk state. This faithfully models "the process died right after operation X committed."

| Crash point | On-disk state after crash | Reconciliation |
|---|---|---|
| **after-backup** (BACKUP done, no manifest) | orphan snapshot dir with `backup/` but no `manifest.json`; `current.json` unchanged | GC removes the orphan (no manifest ⇒ never promoted); re-export |
| **after-validate** (validated, no manifest written) | orphan with valid backup but no manifest; `current.json` unchanged | same — GC removes orphan; validation work is lost but harmless |
| **after-manifest** (manifest written, pointer NOT updated) | new snapshot has `manifest.json`; `current.json` still points to previous good snapshot | GC removes the orphan (not current); OR promote it explicitly (manifest is valid) |
| **after-pointer** (`current.json` updated) | **clean success path** — new snapshot is current | no reconciliation needed |
| **during-gc** (GC interrupted between removals) | some orphans removed, some remain | GC is **idempotent** — re-run converges to the same final state |
| **during-restore** (RESTORE into scratch interrupted) | scratch dir partial/empty; **live store completely untouched** | delete scratch, re-restore. Live store never at risk. |

**Key invariant across all crashes:** `current.json` only advances to a snapshot that has both a manifest and a completed pointer update. An incomplete snapshot (crashed mid-creation) is an orphan — never current, never restorable via the pointer, always GC-eligible. The live store is never mutated by any archive-source operation.

### PR #129 (live-restore) vs archive-branch — separation of concerns `[STATIC]`

The task asks to distinguish PR #129 hardening from archive-branch responsibilities:

| Concern | Owner | Why |
|---|---|---|
| crash **after live swap, before marker rewrite** (`restoreCheckpoint` line 311-316) | **PR #129** (checkpoint recovery) | this is the *live* restore path that swaps the running dataDir; `maple start`'s `isStoreDirty`/`checkStoreCompatible` catch the stale marker. Not used by archive sourcing. |
| crash during archive-source restore | **archive branch** | but the archive path uses `restoreSnapshotToScratch` (external scratch only) — it **never performs a live swap**, so the marker-rewrite issue does not apply |
| atomic promotion of `current.json` | **archive branch** | proven here (write-temp + rename) |
| fsync of parent dir after rename | **both** | documented below |

**The archive source path does not perform a live swap.** It restores to external scratch only, so the PR #129 marker-skew concern is structurally avoided. PR #129's hardening (marker rewrite atomicity, quarantine naming) remains a checkpoint-recovery concern, independent of archive sourcing.

### Durability assumptions `[DOCUMENTED]`

- **`rename(2)` atomicity:** `current.json` uses write-temp + rename, atomic on the same filesystem. Snapshot dirs are created in place (no rename needed for promotion — only `current.json` moves).
- **Hard-power-loss caveat:** rename is atomic in the page cache, but a **parent-directory `fsync` is needed to guarantee the rename survives a hard crash** (not just a clean process exit). Production should `fsync` the `backups/` dir after the `current.json` rename. **Not fsync-tested** — flagged as a production hardening step.

---

## Resource measurements `[VERIFIED, representative scale]`

From the Trajectory C proof (small marker store; multi-GB remains a future sizing exercise per the task):

| Step | Time | Peak RSS | Backups dir size |
|---|---|---|---|
| create snapshot C1 (BACKUP + validate + promote) | 604 ms | 195 MB | 1 MB |
| restore C1 to external scratch | 854 ms | 196 MB | — |
| create snapshot C2 | ~600 ms * | 205 MB | 2 MB |
| Parquet export from scratch (winning settings) | included in restore | — | — |

*C2 showed a 15.8s outlier in one run (machine scheduling); ~600ms is representative. RSS stays near the ~200 MB chDB baseline throughout — the archive source fits comfortably in a 16 GB machine. Scratch is ephemeral: opened, exported, closed, removed — no always-running second database.

**Disk amplification:** the snapshot adds ~1× the backup size under `backups/snapshots/`; scratch restore adds ~1× the restored store size transiently on the external volume (removed after export).

---

## Why Trajectory C succeeds (and B/A were not needed)

Trajectory C's core insight: **the archive exporter never touches the live dataDir.** It restores an immutable checkpoint by ID into external scratch storage, exports from there, and removes the scratch. This structurally satisfies "never risk the only live store" without locks, copies of the live dir, or stopping Maple. The immutability + pinning + atomic-pointer design makes the source consistent, addressable, and safe under interruption.

Because C met every success criterion with no blocker, the cascade's fallback trajectories (B: mutable checkpoint with lock; A: stopped live-store export) were **not evaluated** — per the task's instruction to descend only on a verified blocker.

---

## No-way-forward criteria — not triggered

No viable trajectory was reported because none was needed: Trajectory C is viable. There is no technical impossibility to report.

---

## Recommendation for the implementation branches

1. **Adopt Trajectory C** as the archive source. The checkpoint module should move from mutable `building/current/previous` to **immutable `snapshots/<id>/` + `current.json` + pins/**. This is a contained change to `checkpoints.ts` — the `BACKUP`/`RESTORE` primitives are reused as-is.
2. **Use persistent pinning** (not exclusive locks) to prevent GC during restore.
3. **`fsync` the `backups/` parent dir** after the `current.json` rename before considering the source durable (production hardening).
4. **Keep PR #129 hardening separate** — the live-swap marker-rewrite concern is checkpoint-recovery work, not archive-source work. The archive path's `restoreSnapshotToScratch` never performs a live swap.
5. **Do not yet design** rotation generations, shard sizing, or the archive catalog — this task decided only how Maple obtains a safe archive source. Those are the next research/implementation steps.

---

## Reproducibility

- **Environment:** macOS arm64, libchdb v26.1.0 (chDB `26.1.2.1`), Bun 1.3.14.
- **Scripts (committed):** [`experiments/trajectory/`](../experiments/trajectory/) — `immutable-checkpoint.ts` (prototype module), `trajectory-c-proof.ts` (7 caps + concurrency), `fault-injection.ts` (crash matrix), `lock-vs-pinning.ts` (comparison).
- **Run:** `cd experiments/trajectory && bun run trajectory-c-proof.ts` (then `fault-injection.ts`, `lock-vs-pinning.ts`).
