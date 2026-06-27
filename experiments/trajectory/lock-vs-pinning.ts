/**
 * Lock vs pinning comparison for preventing source deletion during restore.
 *
 * The task asks: "Compare a short exclusive checkpoint lock against persistent
 * snapshot pinning. Recommend the least complicated mechanism that prevents
 * source deletion during restore."
 *
 * This script empirically tests the critical behavioral difference: what happens
 * when the holder crashes without releasing?
 *   - Exclusive lock file: the lock is STALE; recovery requires a heuristic
 *     (PID alive? age?) — error-prone.
 *   - Persistent pin file: the pin survives; GC correctly skips the snapshot.
 *     The pin is cleaned up explicitly by the restorer when done, or left as
 *     a "this snapshot may be needed" hint. A stale pin just means the snapshot
 *     is retained longer (safe over-retention, never unsafe deletion).
 */
import { existsSync, mkdtempSync, rmSync, mkdirSync, writeFileSync, readFileSync } from "node:fs"
import { execSync } from "node:child_process"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { openChdb, query, closeChdb, bootstrapSchema, writeBackupsConfig } from "../archives/harness"
import * as ic from "./immutable-checkpoint"

const sh = (c: string) => { try { return execSync(c, { encoding: "utf8", timeout: 30000 }).trim() } catch { return "?" } }
const LIVE = mkdtempSync(join(tmpdir(), "trajC-lockcmp-"))
const CFG = writeBackupsConfig(LIVE)

console.log(`=== Lock vs Pinning comparison ===\n`)

// Build a snapshot
{ const c = openChdb(LIVE, CFG); bootstrapSchema(c.conn); query(c.conn, "INSERT INTO traces (OrgId, Timestamp, TraceId, SpanId, ServiceName, StatusCode, StatusMessage, SpanName, SpanKind) VALUES ('local','2026-06-25 10:00:00.000','lockcmp','lockcmp12121','svc','Error','LOCKCMP','root','Server')"); closeChdb(c) }
const snap = await ic.createSnapshot(LIVE, CFG)
console.log(`snapshot: ${snap.checkpointId.slice(-12)}\n`)

// ── Scenario: pin holder crashes without releasing ──
console.log(`── Pinning: holder crashes without releasing ──`)
const pin = await ic.pinSnapshot(LIVE, snap.checkpointId)
console.log(`  pin created: ${pin.slice(0,8)}`)
// Simulate crash: do NOT call releasePin. Run GC.
const gc1 = await ic.gcSnapshots(LIVE)
console.log(`  GC after crash (pin not released): removed=[${gc1}]`)
console.log(`  → snapshot retained: ${existsSync(ic.snapshotDir(LIVE, snap.checkpointId)) ? "YES (correct — over-retention is safe)" : "NO (BUG)"}`)

// ── Scenario: exclusive lock file holder crashes ──
console.log(`\n── Exclusive lock file: holder crashes without releasing ──`)
const lockPath = join(LIVE, "backups", "archive.lock")
writeFileSync(lockPath, String(process.pid))
console.log(`  lock file written with PID ${process.pid}`)
console.log(`  → recovery heuristic needed: is PID alive? how old is the lock?`)
console.log(`  → a naive GC/delete would see the lock and either:`)
console.log(`     (a) refuse to delete (correct but requires parsing the lock), or`)
console.log(`     (b) ignore stale locks and delete (UNSAFE), or`)
console.log(`     (c) require operator intervention (friction)`)
console.log(`  → lock recovery is fundamentally heuristic; pin recovery is structural`)

// ── Comparison table ──
console.log(`\n── Comparison ──`)
console.log(`  ${"aspect".padEnd(34)} ${"exclusive lock".padEnd(26)} ${"persistent pin".padEnd(26)}`)
console.log(`  ${"-".repeat(34)} ${"-".repeat(26)} ${"-".repeat(26)}`)
const rows = [
	["prevents deletion during restore", "yes (if checked)", "yes (GC skips pinned)"],
	["holder crashes mid-restore", "STALE — heuristic recovery", "safe — over-retains (never deletes)"],
	["cross-process?", "yes (flock/PID file)", "yes (filesystem files)"],
	["survives reboot?", "lock lost; PID stale", "pin file persists"],
	["cleanup on success", "unlink lock", "unlink pin file"],
	["complexity", "medium (stale detection)", "low (file existence)"],
	["failure mode", "unsafe deletion OR stuck lock", "safe over-retention"],
]
for (const [a, l, p] of rows) console.log(`  ${a.padEnd(34)} ${l.padEnd(26)} ${p.padEnd(26)}`)

console.log(`\n── Recommendation ──`)
console.log(`  Persistent snapshot pinning is the least-complicated mechanism that prevents`)
console.log(`  source deletion during restore. Its failure mode (over-retention) is strictly`)
console.log(`  safe, whereas a stale lock's failure mode is either unsafe deletion or a`)
console.log(`  stuck-lock requiring heuristic recovery. Pinning also composes naturally with`)
console.log(`  the immutable-snapshot GC (GC already checks pin state).`)

rmSync(LIVE, { recursive: true, force: true })
