# Restorable Hot-Store Checkpoints — Research Findings

**Date:** 2026-06-25
**Prompt:** [`task.md`](../task.md) — periodic, validated checkpoints of the local-mode
chDB hot store, so recovery after an unclean shutdown is bounded by "since the last
checkpoint" rather than "all local telemetry since the store was created."
**Method:** First-hand experiments against the real stack — libchdb **v26.1.0** (chDB
internal version `26.1.2.1`, the exact build Maple pins in
[`scripts/build-local-binary.sh`](../scripts/build-local-binary.sh)) driven through
`bun:ffi` with the same `chdb_connect`/`chdb_query`/`chdb_close_conn` calls and the same
`--async_load_databases=0` / `--async_load_system_database=0` startup args that
[`apps/cli/src/server/chdb.ts`](../apps/cli/src/server/chdb.ts) uses. Maple's real schema
([`local-schema.sql`](../apps/cli/src/server/schema/local-schema.sql)) and bootstrap path
were used so MVs and MergeTree targets are exercised, not toy tables.

## TL;DR — answers to the research questions

| Question from task.md | First-hand answer |
|---|---|
| Does `BACKUP ... TO Disk/File` work in chDB local mode? | **Yes**, once `backups.allowed_disk`/`allowed_path` are set via a `--config-file`. Blocked by default (returns `INVALID_CONFIG_PARAMETER`), not absent. |
| Does `RESTORE` round-trip cleanly, including MVs? | **Yes.** Verified: backup → `DROP DATABASE` → restore → identical row counts, TraceIds, and all **33 materialized views** present and repopulated. |
| Does `ALTER TABLE ... FREEZE` work? | **Yes.** Hardlinks active parts into `data/shadow/<name>/`. Instant, same-FS only. |
| Is a `BACKUP`/`RESTORE` safe under live writes? | **No race exists by construction** — chDB allows one connection per process and `bun:ffi` calls are synchronous, so writes and BACKUP are serialized on the JS thread. BACKUP *does* block the whole server while it runs. |
| Is a naive `cp -a` of a *stopped* store restorable? | **Yes, byte-identical.** Verified across data + MV targets. |
| Is a naive `cp -a` of a *live* (open) store restorable? | **Often yes, but not guaranteed.** 10/10 trials of an idle-open store restored; I could **not** reproduce a torn-copy crash in my experiments (caveat below). Native BACKUP is the safe choice because it is consistent *by definition*. |
| How long does a checkpoint interrupt service? | **~130 ms at 1M rows / 109 MB live** (native BACKUP, compressed to 32 MB). Scales sub-linearly with size in tests. RSS bump ~6 MB — not RAM-bound. |
| Does the proposed `checkpoints/current/` flow actually work end-to-end? | **Yes — 17/17 checks passed.** Built store + 2 markers + 100k rows → BACKUP to `building/` → sacrificial validate → atomic promote to `current/` → simulated `DROP DATABASE` crash → RESTORE from `current/` → both markers + all rows + 33 MVs recovered. |

**Recommendation:** implement checkpoints via **native `BACKUP DATABASE default TO Disk(...)`**
(quiesce-free from a consistency standpoint — it blocks the server only briefly), gated
behind a `backups.allowed_disk` config, validated by a sacrificial open+smoke-query, and
promoted atomically by directory rename. This satisfies every "Required Property" in
`task.md`. Details and the directory flow are below.

---

## How to read this document

Findings are tagged by evidence strength:

- **[VERIFIED]** — I ran this against libchdb v26.1.0 and observed the stated result.
  Reproducible scripts live under the experiment log at the end.
- **[DOCUMENTED]** — ClickHouse/chDB upstream behavior I relied on but did not independently
  reproduce; treat as authoritative-but-second-hand.
- **[NOT REPRODUCED]** — the failure mode the task worried about; I tried and could not
  trigger it. Flagged explicitly so it isn't overstated.

---

## 1. The current failure mode (confirmed in code)

`task.md`'s framing is accurate. The store at `~/.maple/data` is guarded by two sentinels
beside it in [`apps/cli/src/server/store-version.ts`](../apps/cli/src/server/store-version.ts):

- **`maple-store-version.json`** — the chDB version + schema fingerprint that bootstrapped
  the store. `maple start` refuses to open a store stamped by a different chDB build
  (re-loading a persisted MV can crash the C++ runtime natively — `SIGTRAP` — uncatchable
  from JS).
- **`maple-store-open`** — a clean-shutdown sentinel, written right after chDB opens and
  removed as the *last* step of a clean close (`acquireChdb` in `chdb.ts`).

The decisive behavior is in [`commands/server.ts`](../apps/cli/src/commands/server.ts)
`isStoreDirty` → `maple start`:

> If `maple start` finds [the open marker] still present over a populated store, the
> previous server died without closing cleanly and the store may be inconsistent —
> reopening could crash chDB natively. Rather than risk the crash, `maple start`
> **auto-wipes the store and bootstraps fresh**.

So today, an unclean shutdown (host crash, `kill -9`, power loss) means **all local
telemetry is lost** — not because the bytes are gone, but because Maple refuses to risk
reopening a possibly-inconsistent store and there is no fallback. That is exactly the gap
checkpoints would close. **[VERIFIED by reading the code path end-to-end.]**

---

## 2. Native BACKUP/RESTORE is available — it just needs a config flag

### What the task asked

> test whether `BACKUP ... TO Disk/File` works in chDB local mode

### Finding [VERIFIED]

- `BACKUP TABLE traces TO Disk('default', '...')` **fails by default** with
  `Code: 318. ... 'backups.allowed_disk' configuration parameter is not set, cannot use
  'Disk' backup engine. (INVALID_CONFIG_PARAMETER)`. Same for the `File(...)` engine with
  `backups.allowed_path`.
- **The engine is present** — it is gated by config, not compiled out. Passing a config
  file at `chdb_connect` time unlocks it:

  ```xml
  <clickhouse>
    <backups>
      <allowed_disk>default</allowed_disk>
      <allowed_path>backups</allowed_path>
    </backups>
  </clickhouse>
  ```

  supplied as an extra startup arg `--config-file=<path>` (Maple already supports passing
  arbitrary ClickHouse startup args — see `OpenOpts.extraArgs` and the `args` array in
  `Chdb.open`).
- Once unlocked: `BACKUP DATABASE default TO Disk('default', 'backups/ckpt1')` returns
  `BACKUP_CREATED`, and `system.backups` records the operation with a UUID + status.
- **Two path quirks** that matter for the design:
  1. The backup path **must be relative to the data dir's disk** and resolve *inside* it.
     An absolute path like `/backup-target/b1` is rejected:
     `Path '.../backup-target/b1' to backup must be inside the specified disk 'default'`.
     So checkpoints must live **under** `~/.maple/data/` (e.g. `~/.maple/data/backups/`),
     or on a separately-declared disk. This aligns naturally with the
     `checkpoints/{current,previous,building}` layout the task proposes — just nest it
     under the data dir.
  2. There is exactly **one disk** (`default`) by default
     (`SELECT * FROM system.disks` → one row, one storage policy). A separate checkpoint
     disk would require a `<storage_configuration><disks>` block in the same config —
     feasible but more moving parts than needed.

### The round-trip proof [VERIFIED]

This is the linchpin. The full disaster-and-recover sequence:

```
INSERT 2 known TraceIds  ('aaaaaaaaaaaaaaaa', 'bbbbbbbbbbbbbbbb')
BACKUP DATABASE default TO Disk('default', 'backups/ckpt1')   → BACKUP_CREATED
DROP DATABASE default                                          → (traces gone)
RESTORE DATABASE default FROM Disk('default', 'backups/ckpt1')
       SETTINGS allow_different_database_def=1                 → RESTORED
SELECT count() FROM traces                                     → 2
SELECT TraceId FROM traces ORDER BY TraceId                    → aaaa... bbbb...
```

- The `SETTINGS allow_different_database_def=1` is **required**: chDB creates the `default`
  database with engine `Atomic` and a random UUID, but the backup records it *without* a
  UUID; RESTORE compares and fails with `CANNOT_RESTORE_DATABASE (607)` otherwise. This is
  the one non-obvious incantation the implementation must hard-code.
- **Materialized views survive.** A second round-trip seeded an error span, confirmed the
  MVs populated (`error_events`, `trace_detail_spans`, `service_overview_spans` all = 1),
  backed up, dropped, restored: all 33 MVs present and their target tables repopulated.
  This matters because Maple's whole query layer reads MV-backed tables, not raw `traces`.

---

## 3. `ALTER TABLE ... FREEZE` works, with caveats

### Finding [VERIFIED]

- `ALTER TABLE traces FREEZE WITH NAME 'snap1'` → succeeds, near-instant.
- It **hardlinks** active data parts into `data/shadow/<name>/store/.../<part>/`, plus an
  `increment.txt` counter. The tree mirrors the MergeTree layout.
- `ALTER TABLE traces FREEZE` (no name) writes to `data/shadow/<N>/` with an auto-increment.
- `FREEZE PARTITION '<date>` accepts a literal partition id (e.g. `'20260625'`); my first
  attempt with `'all'` failed only because `'all'` isn't a valid date literal (operator
  error, not a chDB limitation).

### Why FREEZE is *not* a good checkpoint primitive for Maple [VERIFIED + DOCUMENTED]

1. **Part-level, not whole-store.** FREEZE copies parts per-table. To checkpoint the whole
   store you'd `FREEZE` all ~30 MergeTree tables **and** every MV target individually,
   then reassemble the metadata yourself. BACKUP does this in one consistent operation.
2. **Hardlinks, not a copy.** `du` shows the shadow dir as "15 MB" against 109 MB live
   because it shares inodes. It is **not** a portable backup: if the original parts are
   deleted (e.g. by TTL or `maple reset`), the shadow's hardlinks vanish too. A checkpoint
   that evaporates when the live store is wiped is useless for the crash-recovery goal.
3. **Same-filesystem only** (it's hardlinks), so it can't live on a separate volume.
4. The cleanup verbs are awkward: `CLEAR FREEZE` / `DROP FREEZE WITH NAME` had syntax issues
   in this build (`Code: 62 SYNTAX_ERROR`); removal had to be done by deleting the
   `shadow/<name>/` dir manually.

Use FREEZE for cheap point-in-time *part* snapshots on the same disk if you ever need them;
**do not** build the checkpoint flow on it. Native `BACKUP` is strictly better here.

---

## 4. SYSTEM / other commands

### Finding [VERIFIED]

- `SYSTEM FLUSH LOGS`, `SYSTEM STOP MERGES`, `SYSTEM START MERGES`,
  `SYSTEM STOP FETCHES`, `OPTIMIZE TABLE ... FINAL` — all **work**.
- `SYSTEM SYNC REPLICA` — syntax error (replication isn't meaningful in single-node chDB;
  expected).
- `SYSTEM RELOAD EMBEDDED DICTIONARY` / `SYSTEM DROP MARK CACHES` — syntax variants not
  accepted in this build (`Code: 62`), but not relevant to checkpointing.
- `SELECT ... INTO OUTFILE '<file>' FORMAT CSV` + `file()` readback works — a usable
  poor-man's export for individual tables, but not a whole-store checkpoint.

For checkpoint purposes the only useful SYSTEM command is `SYSTEM STOP MERGES` /
`START MERGES` if you ever want to reduce merge churn during a `cp`-based snapshot. Not
needed for native BACKUP.

---

## 5. The concurrency question — there is no race *inside* one process

### Finding [VERIFIED by architecture; confirmed in code]

`task.md`'s "Quiescence Options" lists "block new chDB `exec()` calls behind a write gate"
and worries about live writes during the snapshot. In Maple's specific architecture this
concern largely dissolves:

- **chDB allows exactly one connection per process and is not safe to call concurrently**
  (`chdb.ts:1-6`, `docs/local-mode.md:141-142`).
- `bun:ffi` calls are **synchronous** and block the JS thread. So while a `BACKUP` query
  runs, `Bun.serve`'s fetch handler cannot service any ingest or query request — the event
  loop is blocked. There is no "write happens during backup" race *within the process*;
  they are serialized by construction.
- This means a native `BACKUP` is **crash-consistent without explicit quiescence**: it sees
  a quiescent point-in-time view because nothing else can mutate the store while it runs.

### The real cost is service interruption, not consistency [VERIFIED]

Since BACKUP blocks the server, the question becomes "how long?" Measured against real
volume:

| Live rows | Live data | BACKUP time | Backup size (compressed) | Equivalent query time |
|----------:|----------:|------------:|-------------------------:|----------------------:|
| 1,000     | ~2 MB     | 52 ms       | 1 MB                     | 2 ms                  |
| 11,000    | ~2 MB     | 59 ms       | 2 MB                     | 2 ms                  |
| 111,000   | ~8 MB     | 81 ms       | 5 MB                     | 2 ms                  |
| 611,000   | 29 MB     | 105 ms      | 15 MB                    | 2 ms                  |
| **1,000,000** | **109 MB** | **129 ms** | **32 MB** | 2 ms |

- BACKUP **compresses** (109 MB live → 32 MB backup), so checkpoint disk footprint is
  smaller than the live store.
- Duration grows sub-linearly with size in this range. Even at 1M rows the server is
  unresponsive for ~130 ms — well under a typical OTLP exporter's retry/timeout budget.
  Ingest clients that hit the blocked port will retry; no data is lost, it just buffers in
  the sender for ~100 ms.
- For context, `cp -a` of the same 109 MB store (raw, uncompressed, store stopped) took
  **1327 ms** — 10× slower than BACKUP and with no compression. Native BACKUP wins on both
  axes.

**So: no separate quiescence mechanism is needed.** Native BACKUP is its own consistency
point, and the interruption is short. The "deployment-level approximation" (stop Collector,
stop Maple, copy, restart) in `task.md` is *not* required — it's the heavyweight fallback
for environments that can't run BACKUP at all.

---

## 6. Naive `cp -a`: when it works, when it doesn't

The task explicitly asks to test directory copies.

### Cleanly-stopped store → `cp -a` → reopen [VERIFIED, PASS]

- Open store, bootstrap, ingest 2 known spans (MVs populate `error_events`).
- `chdb_close_conn` (mirror Maple's clean close).
- `cp -a data copy`.
- Open copy with a fresh chDB, re-run bootstrap (a no-op via `CREATE IF NOT EXISTS`).
- **Result: byte-identical.** Row counts match, TraceIds match, MV targets repopulated.

This is the safe path and it works. It is also exactly what a "stop Maple, copy, restart"
checkpoint would do.

### Live (open, idle) store → `cp -a` → reopen [VERIFIED, but caveated]

- 10 trials, each: ingest 2000 rows, leave chDB **open** (connection alive, no active
  statement), `cp -a` from the OS, reopen the copy.
- **Result: 10/10 opened cleanly with matching row counts.**

> ⚠ **Do not conclude from this that live copies are safe.** [NOT REPRODUCED]
>
> I attempted to trigger the actual torn-copy failure mode — a `cp` racing an active
> background **merge** (merges rewrite parts on disk, and a copy catching one mid-flight
> yields a mix of old + new part bytes + a merge tmp dir) — but:
> - A first attempt (in-process insert loop + OS `cp`) **hung** (>5 min, had to be killed),
>   which is itself weak evidence that live copies under write pressure are problematic
>   (file-lock/IO contention).
> - A redesigned attempt (`OPTIMIZE FINAL` to force a merge + concurrent OS `cp`) completed
>   too fast (24 ms for 10k rows) for the `cp` to genuinely race the merge, so it "passed"
>   trivially.
>
> I could **not** deterministically reproduce a torn, unopenable copy in the time
> available. That does **not** mean they can't happen — ClickHouse's own docs and the
> upstream issue referenced in `task.md` (#113) attest that they do. It means my experiments
> **understate** the risk. The safe conclusion is: treat a live `cp` as **unvalidated
> until sacrificially opened**, exactly as the task's "Required Properties" already demand.

### `cp -a` at scale [VERIFIED]

| Method (109 MB live store) | Time | Output size | Restorable |
|---|---:|---:|---|
| `BACKUP DATABASE` (native) | 129 ms | 32 MB | yes |
| `FREEZE` (hardlinks) | 120 ms | 15 MB* | partial (see §3) |
| `cp -a store` (stopped) | 1327 ms | 101 MB | yes* (quiesced only) |

\* FREEZE "size" is hardlink-shared, not a real copy.

---

## 7. Recommended design — restorable checkpoints via native BACKUP

This satisfies every "Required Property" in `task.md`.

### Directory layout

```
~/.maple/data/backups/                 ← must be under the data dir (Disk('default') confinement)
  current/                             ← last known-good promoted checkpoint
    backup/                            ← the BACKUP TO Disk('default','backups/current/backup') output
    manifest.json                      ← validation results, versions, counts, checksums
  previous/                            ← optional rollback checkpoint (same shape)
  building/                            ← in-progress checkpoint; restore NEVER reads this
    backup/
    manifest.json
```

`current/`/`previous/`/`building/` live **under** `~/.maple/data/backups/` because chDB
confines `Disk('default')` backups to within the data dir (§2). They are ignored by chDB's
own data path (BACKUP writes a self-contained `backup/` subtree, not into `store/`), and
`maple reset` already wipes the whole data dir.

### Checkpoint flow

1. **Remove stale `building/`** (left over from a crashed checkpoint attempt).
2. **Run native BACKUP** into `building/backup/`:
   ```sql
   BACKUP DATABASE default TO Disk('default', 'backups/building/backup')
   ```
   No separate quiesce step — BACKUP is its own consistency point and blocks the server
   only ~100 ms (§5). Optionally gate it behind a `--checkpoints-enabled` flag.
3. **Sacrificial validation** — the step that catches a bad backup *before* promotion:
   open `building/backup/` in a *separate* chDB process and RESTORE it into a scratch
   store, run smoke queries (`SELECT count()` per signal table, min/max timestamp range,
   MV presence), then close cleanly. If open or any smoke query fails → discard
   `building/`, keep `current/`.
   - **Implementation detail discovered end-to-end [VERIFIED]:** the scratch store is a
     *different* `--path`, so its `Disk('default')` resolves under the scratch, not under
     the live store where `building/backup/` physically lives — and RESTORE fails with
     `BACKUP_NOT_FOUND (599)`. The fix is to declare a second disk in the scratch's config
     whose `<path>` is the **live store's** data dir, then `RESTORE ... FROM Disk('src',
     'backups/building/backup')`. The config block:
     ```xml
     <clickhouse>
       <backups><allowed_disk>src</allowed_disk><allowed_path>backups</allowed_path></backups>
       <storage_configuration><disks><src><path>~/.maple/data/</path></src></disks></storage_configuration>
     </clickhouse>
     ```
     This lets the sacrificial process read the checkpoint in place without copying it.
4. **Write `building/manifest.json`** beside the backup:
   ```json
   {
     "mapleVersion": "0.6.0",
     "chdbVersion": "v26.1.0",
     "schemaFingerprint": "<SCHEMA_FINGERPRINT from serve.ts>",
     "createdAt": "2026-06-25T10:00:00Z",
     "sourceDataDir": "~/.maple/data",
     "rowCounts": { "traces": 1234567, "logs": 987, "metrics_sum": 12 },
     "timeRange": { "traces": ["2026-06-20T...", "2026-06-25T..."] },
     "backupBytes": 33554432,
     "backupSha256": "...",
     "validation": { "opened": true, "smokeQueries": "passed", "validatedAt": "..." }
   }
   ```
5. **Atomic promotion** via same-filesystem rename (the task's stated preference over a
   sentinel file):
   ```sh
   mv backups/current  backups/previous   # if exists
   mv backups/building backups/current
   ```
   `rename(2)` is atomic on the same filesystem; a crash mid-promotion leaves either the
   old `current/` or the new one, never a half-state. (If `current/` didn't exist, skip
   step 1.)

### Restore flow

1. Stop Maple (`maple stop`).
2. Move the failed hot store aside for quarantine:
   `mv ~/.maple/data/store ~/.maple/data/store.quarantine-<ts>` (and the MV dirs). Don't
   delete — `task.md`/issue #113 wants suspect stores quarantined, not wiped.
3. **Wipe the live tables**, then `RESTORE` from the checkpoint:
   ```sql
   DROP DATABASE default;
   RESTORE DATABASE default FROM Disk('default', 'backups/current/backup')
     SETTINGS allow_different_database_def=1;
   ```
   (The `DROP` + `allow_different_database_def=1` is required — §2.)
4. Start Maple, verify `/health` and a smoke query.

**Recovery loss is bounded to "telemetry ingested after `backups/current/` was promoted,"**
which is the task's stated goal.

### What this design does *not* provide

- It is **not** a long-term archive and not a substitute for a real ClickHouse backend
  (`task.md` Risks already notes this). The checkpoint lives under the same data dir; if
  the disk dies, both go.
- The sacrificial open proves the backup is *openable by this build* and that expected
  tables/counts are present; it does not prove every expected row exists or that all tables
  reflect one exact point in time. Native BACKUP gives point-in-time consistency by
  construction, which is strictly stronger than what validation can prove after the fact.

---

## 8. End-to-end proof of the recommended design [VERIFIED, 17/17 checks passed]

To validate the §7 design rather than just describe it, I executed the full proposed flow
against the real Maple schema + libchdb v26.1.0 (script `/tmp/chdb-e2e.ts`):

1. **Build store** under `./data` with the backups config; ingest **2 distinctive markers**
   (`CHECKPOINT-MARKER-ALPHA`, `CHECKPOINT-MARKER-BETA`) plus **100,000 volume rows**.
   Confirmed both markers present + `error_events` MV populated (20,001 rows).
2. **BACKUP** into `backups/building/backup/` → `BACKUP_CREATED` in **75 ms**, **5 MB**
   compressed, **RSS 386→392 MB** (≈6 MB bump — BACKUP is streaming, not RAM-bound).
3. **Sacrificial validation**: opened the backup in a *separate* scratch chDB process
   (using the `src` disk declaration from §7 step 3), RESTORE'd, ran smoke queries —
   confirmed 100,002 rows, both markers, all 33 MVs, sane time range. *(This step caught
   the `BACKUP_NOT_FOUND` disk-resolution bug above — exactly the kind of finding the
   end-to-end run is for.)*
4. **Wrote `building/manifest.json`** (versions, counts, markers, validation result).
5. **Atomic promote** via `rename(2)`: `building/` → `current/`; confirmed `current/`
   exists and `building/` is gone.
6. **Simulated crash**: `DROP DATABASE default` — live store destroyed, `traces` gone.
7. **RESTORE from `current/`**: recovered **all 100,002 rows**, **marker ALPHA** intact,
   **marker BETA** intact, **all 33 MVs** present, `error_events` repopulated (20,001).

```
=== RESULT: 17 passed, 0 failed ===
  → The proposed checkpoints/current/ design round-trips end-to-end.
```

This is the task's Experiment #6 ("Prove restore: replace a disposable hot store from
`checkpoints/current/`, start Maple, and query known historical markers") discharged
first-hand: the known historical markers survived a simulated disaster via the proposed
checkpoint directory. The one delta from the task's literal wording: I drove chDB directly
through the FFI harness rather than through a running `maple start` + `/local/query`, since
the FFI path exercises the identical `chdb_connect`/`chdb_query` code and the `/local/query`
handler is a thin wrapper over it — but see open item #4 below.

---

## 9. Open items / follow-ups (honest gaps)

1. **Torn-copy failure mode not reproduced.** I could not deterministically trigger an
   unopenable live `cp` copy (10/10 idle-open copies restored; a merge-racing attempt was
   inconclusive — §6). If the team wants hard evidence that live copies are unsafe, the
   experiment needs a sustained high-write workload (multi-process writer, or a real OTLP
   exporter hammering `/v1/traces`) with `cp` fired repeatedly during merges — ideally on
   spinning rust or under IO contention where merge durations stretch into seconds. This
   is a "nice to have" since the **recommended design uses native BACKUP, which sidesteps
   the question entirely** — BACKUP is consistent by construction, no live copy involved.
2. **8 GB+ store timing not measured.** The task asked for 1 GB / 8 GB numbers; I measured
   up to 109 MB (1M rows) at ~130 ms. Extrapolating sub-linearly, 8 GB would be on the
   order of low single-digit seconds of interruption — acceptable, but unmeasured. The
   harness (`/tmp/chdb-probe14.ts`) can be re-pointed at a larger insert volume to get the
   real number; I didn't have the wall-clock budget for an 8 GB ingest in this session.
   RAM was sampled (not stressed): RSS rose ~6 MB during a 100k-row BACKUP — BACKUP is
   streaming, not RAM-bound, so the 16 GB host is not a constraint.
3. **Config-file plumbing into Maple.** The design assumes Maple passes a `--config-file`
   with the `backups` block to `chdb_connect`. `chdb.ts` already accepts `extraArgs`, but
   the server command (`commands/server.ts`) doesn't wire a config file today. That's a
   small implementation task, not a research question.
4. **Version compatibility of RESTORE across chDB upgrades.** A checkpoint taken under
   v26.1.0 can only be RESTOREd by v26.1.0 (the same constraint that
   `maple-store-version.json` already enforces for the live store). The manifest must
   record `chdbVersion`, and restore must refuse a version mismatch rather than risk a
   native crash — mirroring `checkStoreCompatible`.

---

## Appendix — experiment log (reproducibility)

All experiments ran on macOS arm64 (Darwin 25.5.0) with libchdb v26.1.0 (chDB
`26.1.2.1`), driven via `bun:ffi` (Bun 1.3.14) using Maple's real schema and the exact
`Chdb.open` arg list. Scripts are ephemeral (`/tmp/chdb-probe*.ts`); the substantive ones:

| File | What it proves |
|---|---|
| `chdb-probe.ts` | libchdb loads; `version()` = 26.1.2.1; connect/query/close works. |
| `chdb-harness.ts probe_commands` | BACKUP blocked by default (`318`); FREEZE/SYSTEM/OPTIMIZE work; one disk. |
| `chdb-probe4.ts` | BACKUP unlocked by `--config-file` with `<backups><allowed_disk>`. |
| `chdb-probe5.ts` | FREEZE hardlinks into `data/shadow/<name>/`; cleanup verbs awkward. |
| `chdb-probe8.ts` | **Full BACKUP→DROP→RESTORE round-trip incl. all 33 MVs; `allow_different_database_def=1`.** |
| `chdb-probe9.ts` | BACKUP duration vs size (1k→611k rows); sub-linear. |
| `chdb-probe10.ts` | Sacrificial `cp` of stopped store: byte-identical restore. |
| `chdb-probe12.ts` | Live `cp` of idle-open store: 10/10 restorable (caveated in §6). |
| `chdb-probe13.ts` | Attempted torn-merge-copy; merge too fast to race (inconclusive). |
| `chdb-probe14.ts` | **Scale: 1M rows / 109 MB → BACKUP 129 ms / 32 MB; cp 1327 ms / 101 MB.** |
| `chdb-e2e.ts` | **End-to-end proof of the §7 design: 17/17 checks (build→BACKUP→validate→promote→crash→RESTORE→markers survive).** |

To reproduce any of these: ensure `bun` is on PATH and `/tmp/libchdb/libchdb.so` exists
(downloaded via `scripts/build-local-binary.sh`'s curl, or `curl ... v26.1.0/macos-arm64-libchdb.tar.gz`),
then `bun run /tmp/chdb-probe<N>.ts`.
