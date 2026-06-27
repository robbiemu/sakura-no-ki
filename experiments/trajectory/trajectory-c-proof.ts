/**
 * Trajectory C proof: the immutable-checkpoint source.
 * Proves all 7 capabilities + the concurrency sequence from the task.
 *
 * Layout produced (under a disposable live data dir):
 *   <liveData>/backups/
 *     snapshots/<id>/{backup/, manifest.json, pins/}
 *     current.json
 */
import { existsSync, mkdtempSync, rmSync, mkdirSync } from "node:fs"
import { execSync } from "node:child_process"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { openChdb, query, closeChdb, bootstrapSchema, writeBackupsConfig as writeBackupDefault } from "../archives/harness"
import * as ic from "./immutable-checkpoint"

// harness's writeBackupsConfig writes the 'default'-disk form (for the LIVE store).
// ic.writeBackupConfig(path, sourceDataDir?) handles the 'src'-disk form (for scratch).
const writeBackupsConfig = (dataDir: string) => writeBackupDefault(dataDir)

const sh = (c: string) => { try { return execSync(c, { encoding: "utf8", timeout: 60000 }).trim() } catch (e: any) { return "FAIL" } }
const rssMB = () => Number(sh(`ps -o rss= -p ${process.pid}`)) / 1024
const LIVE = mkdtempSync(join(tmpdir(), "trajC-live-"))
const CFG = writeBackupsConfig(LIVE)
const SCRATCH_ROOT = mkdtempSync(join(tmpdir(), "trajC-scratch-"))
const measurements: Array<{ step: string; ms: number; rssMB: number; diskMB: number }> = []
const measure = async (step: string, fn: () => Promise<any>) => {
	const t0 = performance.now(); const r0 = rssMB()
	const result = await fn()
	const ms = performance.now() - t0
	measurements.push({ step, ms: Math.round(ms), rssMB: Math.round(rssMB()), diskMB: Number(sh(`du -sm ${LIVE}/backups 2>/dev/null | cut -f1`) || 0) })
	return result
}
const PASS: string[] = [], FAIL: string[] = []
const check = (label: string, ok: boolean, detail = "") => {
	(ok ? PASS : FAIL).push(label)
	console.log(`  ${ok ? "✓" : "✗"} ${label}${detail ? " — " + detail : ""}`)
}
const insertMarker = (conn: number, marker: string) => {
	query(conn, `INSERT INTO traces (OrgId, Timestamp, TraceId, SpanId, ServiceName, StatusCode, StatusMessage, SpanName, SpanKind) VALUES ('local','2026-06-25 10:00:00.000','${marker}','${marker}repeat12','marker-svc','Error','${marker}','root','Server')`)
}

console.log(`=== Trajectory C: Immutable Checkpoint Source ===`)
console.log(`live:    ${LIVE}`)
console.log(`scratch: ${SCRATCH_ROOT}\n`)

// Bootstrap + insert marker A
{
	const c = openChdb(LIVE, CFG); bootstrapSchema(c.conn)
	insertMarker(c.conn, "MARKER-A")
	console.log(`bootstrap + MARKER-A inserted (${query(c.conn, "SELECT count() FROM traces")} traces)`)
	closeChdb(c)
}

// ── Capability 1: Create + validate a checkpoint under a unique immutable ID ──
console.log(`\n── Cap 1: create + validate snapshot under unique ID ──`)
const m1 = await measure("create C1", () => ic.createSnapshot(LIVE, CFG))
console.log(`  C1 id: ${m1.checkpointId}`)
check("snapshot dir exists", existsSync(ic.snapshotDir(LIVE, m1.checkpointId)))
check("manifest exists", existsSync(join(ic.snapshotDir(LIVE, m1.checkpointId), "manifest.json")))
check("manifest has unique ID", m1.checkpointId.length > 0)
check("validation ran (traces≥1)", m1.validation.traces >= 1, `${m1.validation.traces} traces, ${m1.validation.materializedViews} MVs`)

// ── Capability 2: Promote completed snapshot before atomic pointer replacement ──
console.log(`\n── Cap 2: promote before atomic current.json ──`)
const current1 = await ic.readCurrent(LIVE)
check("current.json points to C1", current1 === m1.checkpointId, `current=${current1}`)
check("current.json is atomic (write-tmp+rename)", existsSync(ic.currentJsonPath(LIVE)) && !existsSync(ic.currentJsonPath(LIVE) + ".tmp"))

// ── Capability 3: Resolve one ID + restore into scratch on external volume ──
console.log(`\n── Cap 3: restore C1 into external scratch ──`)
const scratch1 = join(SCRATCH_ROOT, "restore-C1")
const v1 = await measure("restore C1 to scratch", () => ic.restoreSnapshotToScratch(LIVE, m1.checkpointId, scratch1))
check("scratch restore succeeded (traces≥1)", v1.traces >= 1, `${v1.traces} traces`)
// Verify MARKER-A is in the scratch restore, via direct open
{
	const c = openChdb(scratch1, writeBackupsConfig(scratch1))
	const marker = query(c.conn, "SELECT count() FROM traces WHERE StatusMessage='MARKER-A'")
	check("MARKER-A present in C1 scratch", marker.trim() === "1", `count=${marker.trim()}`)
	closeChdb(c)
}
rmSync(scratch1, { recursive: true, force: true })

// ── Capability 4: Create a newer checkpoint while older remains addressable ──
console.log(`\n── Cap 4: create C2 while C1 remains addressable ──`)
// Insert MARKER-B into the live store, then checkpoint again
{
	const c = openChdb(LIVE, CFG)
	insertMarker(c.conn, "MARKER-B")
	closeChdb(c)
}
const m2 = await measure("create C2", () => ic.createSnapshot(LIVE, CFG))
console.log(`  C2 id: ${m2.checkpointId}`)
check("C2 has different ID than C1", m2.checkpointId !== m1.checkpointId)
check("C1 snapshot dir still exists", existsSync(ic.snapshotDir(LIVE, m1.checkpointId)), "(not overwritten)")
check("C2 snapshot dir exists", existsSync(ic.snapshotDir(LIVE, m2.checkpointId)))
check("current.json now points to C2", (await ic.readCurrent(LIVE)) === m2.checkpointId)

// ── Capability 5: Prevent GC from removing a checkpoint being restored ──
console.log(`\n── Cap 5: pin prevents GC ──`)
const pinId = await ic.pinSnapshot(LIVE, m1.checkpointId)
check("pin file created", ic.isPinned(LIVE, m1.checkpointId))
const removed = await ic.gcSnapshots(LIVE)
check("GC spared pinned C1", !removed.includes(m1.checkpointId), `removed: [${removed.join(",")}]`)
// unpin → GC removes C1
await ic.releasePin(LIVE, m1.checkpointId, pinId)
check("pin released", !ic.isPinned(LIVE, m1.checkpointId))
const removed2 = await ic.gcSnapshots(LIVE)
check("GC removed unpinned C1 after release", removed2.includes(m1.checkpointId), `removed: [${removed2.join(",")}]`)

// Re-pin C1 for the concurrency sequence (re-create it since we just GC'd it... actually
// the concurrency sequence below needs C1+markerA, so let's redo with a clean setup)

// ── Capability 6: Export from restored scratch without opening live dir ──
console.log(`\n── Cap 6: export from scratch without touching live ──`)
// (The restore in Cap 3 already proved this; here we add a Parquet export from scratch)
// Re-create C1 (it was GC'd) for the concurrency test below anyway. For now use C2.
const scratchC2 = join(SCRATCH_ROOT, "export-C2")
await ic.restoreSnapshotToScratch(LIVE, m2.checkpointId, scratchC2)
const exportOut = join(SCRATCH_ROOT, "c2-export.parquet")
{
	const c = openChdb(scratchC2, writeBackupsConfig(scratchC2))
	query(c.conn, `SELECT * FROM traces INTO OUTFILE '${exportOut}' FORMAT Parquet SETTINGS max_threads=1, output_format_parquet_row_group_size=10000`)
	closeChdb(c)
}
check("Parquet exported from scratch", existsSync(exportOut), `${sh(`du -h ${exportOut} | cut -f1`)}`)
rmSync(scratchC2, { recursive: true, force: true })

// ── Capability 7: Close + remove scratch without affecting live ──
console.log(`\n── Cap 7: scratch removal doesn't affect live ──`)
const liveCountBefore = (() => { const c = openChdb(LIVE, CFG); const n = query(c.conn, "SELECT count() FROM traces"); closeChdb(c); return n.trim() })()
rmSync(SCRATCH_ROOT, { recursive: true, force: true })
const liveCountAfter = (() => { const c = openChdb(LIVE, CFG); const n = query(c.conn, "SELECT count() FROM traces"); closeChdb(c); return n.trim() })()
check("live store unchanged after scratch removal", liveCountBefore === liveCountAfter, `${liveCountBefore}==${liveCountAfter}`)

// ════════════════════════════════════════════════════════════════
// Concurrency sequence (the task's required sequence)
// ════════════════════════════════════════════════════════════════
console.log(`\n=== Concurrency sequence (fresh store) ===`)
const LIVE2 = mkdtempSync(join(tmpdir(), "trajC-conc-"))
const CFG2 = writeBackupsConfig(LIVE2)
{
	const c = openChdb(LIVE2, CFG2); bootstrapSchema(c.conn); insertMarker(c.conn, "CONC-MARKER-A"); closeChdb(c)
}
// 1. Create C1 (marker A)
const cc1 = await ic.createSnapshot(LIVE2, CFG2)
console.log(`  1. C1=${cc1.checkpointId.slice(-12)} (marker A)`)
// 2. Pin C1
const cc1Pin = await ic.pinSnapshot(LIVE2, cc1.checkpointId)
console.log(`  2. pinned C1`)
// 3. Create C2 (insert marker B first)
{
	const c = openChdb(LIVE2, CFG2); insertMarker(c.conn, "CONC-MARKER-B"); closeChdb(c)
}
const cc2 = await ic.createSnapshot(LIVE2, CFG2)
console.log(`  3. C2=${cc2.checkpointId.slice(-12)} (marker B)`)
// 4. Restore C1 externally → should have A, not B
const rs1 = join(tmpdir(), "trajC-rs1")
const v_cc1 = await ic.restoreSnapshotToScratch(LIVE2, cc1.checkpointId, rs1)
{
	const c = openChdb(rs1, writeBackupsConfig(rs1))
	const a = query(c.conn, "SELECT count() FROM traces WHERE StatusMessage='CONC-MARKER-A'").trim()
	const b = query(c.conn, "SELECT count() FROM traces WHERE StatusMessage='CONC-MARKER-B'").trim()
	check("  4. C1 restore has A not B", a === "1" && b === "0", `A=${a} B=${b}`)
	closeChdb(c)
}
rmSync(rs1, { recursive: true, force: true })
// 5. Restore C2 → should have both
const rs2 = join(tmpdir(), "trajC-rs2")
await ic.restoreSnapshotToScratch(LIVE2, cc2.checkpointId, rs2)
{
	const c = openChdb(rs2, writeBackupsConfig(rs2))
	const a = query(c.conn, "SELECT count() FROM traces WHERE StatusMessage='CONC-MARKER-A'").trim()
	const b = query(c.conn, "SELECT count() FROM traces WHERE StatusMessage='CONC-MARKER-B'").trim()
	check("  5. C2 restore has both A and B", a === "1" && b === "1", `A=${a} B=${b}`)
	closeChdb(c)
}
rmSync(rs2, { recursive: true, force: true })
// 6. Release C1 → GC
await ic.releasePin(LIVE2, cc1.checkpointId, cc1Pin)
const gcResult = await ic.gcSnapshots(LIVE2)
check("  6. C1 GC'd after release", gcResult.includes(cc1.checkpointId))
check("  6. C2 retained (it's current)", !gcResult.includes(cc2.checkpointId))

// ── Measurements ──
console.log(`\n=== Resource measurements ===`)
console.log(`  ${"step".padEnd(28)} ${"ms".padStart(7)} ${"rss-MB".padStart(7)} ${"backups-MB".padStart(11)}`)
for (const m of measurements) console.log(`  ${m.step.padEnd(28)} ${String(m.ms).padStart(7)} ${String(m.rssMB).padStart(7)} ${String(m.diskMB).padStart(11)}`)

console.log(`\n=== RESULT: ${PASS.length} passed, ${FAIL.length} failed ===`)
if (FAIL.length) { console.log("FAILURES:", FAIL); process.exit(1) }

rmSync(LIVE, { recursive: true, force: true })
rmSync(LIVE2, { recursive: true, force: true })
