## Research: Restorable Hot-Store Checkpoints

### Why this came up

Our initial mental model was too close to "chDB keeps telemetry in memory and
flushes everything only at graceful shutdown." That is not accurate. Embedded
chDB is ClickHouse-like: it writes many files under the data directory during
runtime, while RAM is used for active inserts, queries, merges, caches, and
aggregation.

The real failure mode is subtler:

- the live store is continuously persisted, not purely in memory;
- the store is marked open/dirty while Maple owns chDB;
- after an unclean shutdown, the on-disk bytes may still exist;
- Maple/chDB may still refuse to reopen them because the store may be internally
  inconsistent or because marker/version metadata is missing;
- upstream issue #113 and the Pullfrog plan suggest Maple should quarantine
  suspect stores instead of destructively wiping them.

That means there may be value in periodic, validated checkpoints of the hot
store. A checkpoint would not replace a long-term archive, but it could give us
a known-good restore point after host crash, forced shutdown, or bad recovery.

### Hypothesis

If we can briefly quiesce writes, capture the chDB data directory, validate the
copy by opening and closing it with a sacrificial Maple/chDB process, and then
atomically promote it, we can maintain a recent restorable checkpoint.

This would bound recovery loss to "since the last completed checkpoint" rather
than "all local telemetry since the store was created."

### Required Properties

- The checkpoint must never be made from a torn live copy unless the copy is
  later treated as experimental and validated before promotion.
- Maple/chDB writes must be paused, drained, or cleanly stopped while the
  snapshot boundary is created.
- Promotion must be atomic. Prefer directory rename on the same filesystem over
  a "completed" sentinel inside an already-visible partial directory.
- Restore must only ever use the last promoted checkpoint, not an in-progress
  checkpoint.
- Validation must include opening the copy with the same Maple/chDB build,
  closing it cleanly, and running basic smoke queries/metadata checks.

### Candidate Directory Flow

Use names that make incomplete state unambiguous:

```text
checkpoints/
  current/      # last known-good promoted checkpoint
  previous/     # optional rollback checkpoint
  building/     # in-progress checkpoint, ignored by restore
```

Checkpoint flow:

1. Remove any stale `building/`.
2. Quiesce Maple writes.
3. Copy/snapshot/export the hot `data/` into `building/`.
4. Resume Maple writes if the snapshot boundary was safely captured.
5. Start a sacrificial Maple/chDB process against `building/data`.
6. Verify it opens, responds to `/health`, can answer row-count/time-range smoke
   queries, and closes with no dirty marker.
7. Write an external manifest beside `building/` with Maple version, chDB
   version, schema fingerprint, timestamps, checksums, row counts, and validation
   results.
8. Atomically promote:
   - move `current/` to `previous/`;
   - move `building/` to `current/`.

Restore flow:

1. Stop Maple.
2. Move the failed hot store aside for quarantine.
3. Copy `checkpoints/current/data` into the hot data location.
4. Start Maple and verify health/query smoke tests.

### Quiescence Options To Investigate

- Upstream Maple support:
  - add a checkpoint/admin endpoint or CLI command;
  - stop accepting new ingest;
  - wait for in-flight ingest/query work to finish;
  - block new chDB `exec()` calls behind a write gate;
  - run a native ClickHouse/chDB backup/freeze operation or close chDB cleanly.

- Deployment-level approximation:
  - stop or pause the Collector first so new telemetry queues outside Maple;
  - wait for Maple to drain in-flight requests;
  - stop Maple cleanly;
  - copy/snapshot the stopped store;
  - restart Maple;
  - restart Collector and let the queue drain.

- ClickHouse/chDB-native options:
  - test whether `BACKUP ... TO Disk/File` works in chDB local mode;
  - test whether `ALTER TABLE ... FREEZE` works for every Maple table and
    materialized-view target;
  - test whether any `SYSTEM` commands are useful for reducing merge churn;
  - confirm whether these commands create a restorable whole-store checkpoint or
    only table/partition-level data copies requiring extra metadata handling.

### Risks / Caveats

- A filesystem snapshot alone may not be database-consistent unless chDB is
  quiesced or the database provides the snapshot.
- A live `cp -a data building/data` can capture mixed moments across tables,
  parts, metadata, and materialized-view targets.
- Sacrificial open/close proves the copy is openable by this build; it does not
  prove every expected row exists or that all tables reflect one exact point in
  time.
- Checkpoints are for crash recovery. They are not a substitute for long-term
  cold archives or a proper ClickHouse backend.
- The host has only 16 GB RAM, so validation and restore tests must be sized to
  avoid making the checkpoint process itself destabilize the service.

### Experiments

- Create a disposable Maple data dir, ingest known traces/logs/metrics, stop
  cleanly, copy it, open the copy, and verify row counts.
- Repeat with deployment-level quiescence: stop Collector, stop Maple, copy,
  restart, and verify Collector queue drains.
- Try `BACKUP` and `ALTER TABLE ... FREEZE` through `/local/query` or a local
  Maple test harness; document which commands chDB accepts.
- Attempt a deliberately unsafe live copy under write load, then sacrificially
  open it, to learn how often validation catches torn copies.
- Measure checkpoint duration, disk IO, RAM pressure, and service interruption
  for 1 GB, 8 GB, and larger stores.
- Prove restore: replace a disposable hot store from `checkpoints/current/`,
  start Maple, and query known historical markers.

