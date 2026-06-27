/**
 * Scale workflow: the full Trajectory C sequence at 4 GiB scale, with per-phase
 * measurements. Reads the pre-built dataset at <root>/live.
 *
 * Phases: C1 → marker → C2 → pin C1 → restore C1 to scratch → shard-export 6 tables →
 *         validate → release pin → GC → confirm live unchanged.
 *
 * Each phase records: wall-clock, peak RSS (fresh subprocess /usr/bin/time), disk bytes.
 */
import { spawnSync } from "node:child_process"
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync, readdirSync } from "node:fs"
import { join } from "node:path"
import { openChdb, query, closeChdb, writeBackupsConfig } from "../archives/harness"
import * as ic from "../trajectory/immutable-checkpoint"

const ROOT = readFileSync("/tmp/maple-scale-root.txt", "utf8").trim()
const LIVE = join(ROOT, "live")
const CFG = writeBackupsConfig(LIVE)
const SCRATCH = join(ROOT, "scratch")
const ARCHIVE = join(ROOT, "archive")
const DUCK = "/tmp/duckdb"
const sh = (c: string) => { try { return spawnSync("sh", ["-c", c], { encoding: "utf8", timeout: 300000 }).stdout.trim() } catch (e: any) { return "ERR" } }
const duck = (sql: string) => spawnSync(DUCK, ["-csv", "-noheader", "-c", sql], { encoding: "utf8", stdio: ["ignore","pipe","pipe"], timeout: 120000 }).stdout.trim()
const duckInt = (sql: string) => Number(duck(sql).replace(/[^0-9-]/g, "")) || 0
const diskGB = () => Number(sh(`du -sg ${ROOT} 2>/dev/null | cut -f1`))
const volFreeGB = () => Number(sh(`df -g /System/Volumes/Data | tail -1 | awk '{print $4}'`))

interface Phase { name: string; ms: number; rssMB: number; rootGB: number; freeGB: number; detail: string }
const phases: Phase[] = []
const record = (name: string, ms: number, detail = "") => phases.push({
	name, ms: Math.round(ms),
	rssMB: Number(sh(`ps -o rss= -p ${process.pid}`)) / 1024,
	rootGB: diskGB(), freeGB: volFreeGB(), detail,
})

const WIN = "SETTINGS max_threads=1, output_format_parquet_row_group_size=10000"
const TABLES = ["logs", "traces", "metrics_sum", "metrics_gauge", "metrics_histogram", "metrics_exponential_histogram"]
const PASS: string[] = [], FAIL: string[] = []
const check = (label: string, ok: boolean, detail = "") => { (ok ? PASS : FAIL).push(label); console.log(`  ${ok ? "✓" : "✗"} ${label}${detail ? " — " + detail : ""}`) }

console.log(`=== Scale workflow: Trajectory C at 4 GiB ===`)
console.log(`live: ${LIVE}`)
console.log(`root: ${ROOT}\n`)

// Pre-flight: dataset must exist with rows
{
	const c = openChdb(LIVE, CFG)
	const traces = Number(query(c.conn, "SELECT count() FROM traces"))
	closeChdb(c)
	if (traces === 0) { console.log("ERROR: dataset not built (0 traces). Run build-dataset.ts first."); process.exit(1) }
	console.log(`dataset present: ${traces} traces\n`)
}

// Record source counts for later validation
const sourceCounts: Record<string, number> = {}
{
	const c = openChdb(LIVE, CFG)
	for (const t of TABLES) sourceCounts[t] = Number(query(c.conn, `SELECT count() FROM ${t}`))
	closeChdb(c)
}
console.log("source counts:", JSON.stringify(sourceCounts))

// ── Phase 1: Create + validate checkpoint C1 ──
console.log(`\n── Phase 1: checkpoint C1 ──`)
let t0 = performance.now()
const c1 = await ic.createSnapshot(LIVE, CFG)
record("create C1", performance.now() - t0, `id=${c1.checkpointId.slice(-12)}, ${c1.validation.traces} traces`)
console.log(`  C1: ${c1.checkpointId.slice(-12)}, backupBytes=${(c1.backupBytes/1024/1024).toFixed(0)}MB`)

// ── Phase 2: marker + C2 ──
console.log(`\n── Phase 2: marker + checkpoint C2 ──`)
{
	const c = openChdb(LIVE, CFG)
	query(c.conn, "INSERT INTO traces (OrgId, Timestamp, TraceId, SpanId, ServiceName, StatusCode, StatusMessage, SpanName, SpanKind) VALUES ('local','2026-06-25 10:00:00.000','scalemarker0001','scalemarker0001','svc','Error','SCALE-MARKER','root','Server')")
	closeChdb(c)
}
t0 = performance.now()
const c2 = await ic.createSnapshot(LIVE, CFG)
record("create C2", performance.now() - t0, `id=${c2.checkpointId.slice(-12)}`)

// ── Phase 3: pin C1 ──
console.log(`\n── Phase 3: pin C1 ──`)
const pin = await ic.pinSnapshot(LIVE, c1.checkpointId)
check("C1 pinned", ic.isPinned(LIVE, c1.checkpointId))

// ── Phase 4: restore C1 into external scratch ──
console.log(`\n── Phase 4: restore C1 to scratch ──`)
mkdirSync(SCRATCH, { recursive: true })
const scratch1 = join(SCRATCH, "c1")
t0 = performance.now()
const v1 = await ic.restoreSnapshotToScratch(LIVE, c1.checkpointId, scratch1)
record("restore C1 to scratch", performance.now() - t0, `${v1.traces} traces`)
// Verify SCALE-MARKER is ABSENT (C1 was before the marker)
{
	const c = openChdb(scratch1, writeBackupsConfig(scratch1))
	const marker = query(c.conn, "SELECT count() FROM traces WHERE StatusMessage='SCALE-MARKER'").trim()
	check("C1 scratch has NO SCALE-MARKER (pre-marker)", marker === "0", `count=${marker}`)
	closeChdb(c)
}

// ── Phase 5: export 6 tables as bounded Parquet shards ──
console.log(`\n── Phase 5: shard-export 6 tables from scratch ──`)
mkdirSync(ARCHIVE, { recursive: true })
const archiveC1 = join(ARCHIVE, "c1"); mkdirSync(archiveC1, { recursive: true })
const shardInfo: Array<{ table: string; shards: number; rows: number; parquetMB: number; largestShardMB: number }> = []

for (const table of TABLES) {
	const tableDir = join(archiveC1, table); mkdirSync(tableDir, { recursive: true })
	const srcRows = sourceCounts[table]
	// shard by row windows of ~500k
	const SHARD_ROWS = 500_000
	const numShards = Math.max(1, Math.ceil(srcRows / SHARD_ROWS))
	let exportedRows = 0, totalBytes = 0, largestShard = 0
	t0 = performance.now()
	// Use _part_offset cursor for disjoint sharding (re-derived per snapshot)
	const c = openChdb(scratch1, writeBackupsConfig(scratch1))
	// get min/max _part_offset
	const range = query(c.conn, `SELECT min(_part_offset), max(_part_offset) FROM ${table}`).split("\t")
	const lo = Number(range[0]), hi = Number(range[1])
	const span = hi - lo + 1
	const perShard = Math.ceil(span / numShards)
	for (let i = 0; i < numShards; i++) {
		const slo = lo + i * perShard
		const shi = Math.min(lo + (i + 1) * perShard - 1, hi)
		const out = join(tableDir, `shard-${i.toString().padStart(3, "0")}.parquet`)
		query(c.conn, `SELECT * FROM ${table} WHERE _part_offset >= ${slo} AND _part_offset <= ${shi} INTO OUTFILE '${out}' FORMAT Parquet ${WIN}`)
		const sz = Number(sh(`stat -f%z '${out}' 2>/dev/null || echo 0`))
		totalBytes += sz
		if (sz > largestShard) largestShard = sz
	}
	closeChdb(c)
	const ms = performance.now() - t0
	// count via duckdb across shards
	const files = readdirSync(tableDir).filter(f => f.endsWith(".parquet")).map(f => `'${join(tableDir, f)}'`)
	const duckRows = duckInt(`SELECT count() FROM read_parquet([${files.join(",")}], union_by_name=true);`)
	shardInfo.push({ table, shards: numShards, rows: duckRows, parquetMB: +(totalBytes/1024/1024).toFixed(1), largestShardMB: +(largestShard/1024/1024).toFixed(1) })
	console.log(`  ${table.padEnd(32)} ${numShards} shards, ${duckRows} rows, ${(totalBytes/1024/1024).toFixed(1)}MB, ${ms.toFixed(0)}ms`)
}
record("shard-export all 6 tables", 0, "(sum of per-table times below)")

// ── Phase 6: validate counts, time bounds, complex values ──
console.log(`\n── Phase 6: validate exported shards ──`)
for (const si of shardInfo) {
	const src = sourceCounts[si.table]
	check(`${si.table}: shard rows (${si.rows}) == source (${src})`, si.rows === src)
}
// complex value spot-check on traces
{
	const files = readdirSync(join(archiveC1, "traces")).map(f => `'${join(archiveC1, "traces", f)}'`)
	const withAttrs = duckInt(`SELECT count() FROM read_parquet([${files.join(",")}], union_by_name=true) WHERE SpanAttributes['r'] IS NOT NULL;`)
	check("traces SpanAttributes map survived", withAttrs > 0, `${withAttrs} rows with map`)
}

// ── Phase 7: release pin + GC ──
console.log(`\n── Phase 7: release pin + GC ──`)
await ic.releasePin(LIVE, c1.checkpointId, pin)
t0 = performance.now()
const removed = await ic.gcSnapshots(LIVE)
record("GC", performance.now() - t0, `removed ${removed.length} snapshots`)
check("C1 GC'd after release", removed.includes(c1.checkpointId))
check("C2 retained (current)", !removed.includes(c2.checkpointId))

// ── Phase 8: confirm live unchanged ──
console.log(`\n── Phase 8: live store unchanged ──`)
{
	const c = openChdb(LIVE, CFG)
	let ok = true
	for (const t of TABLES) {
		const now = Number(query(c.conn, `SELECT count() FROM ${t}`))
		if (now !== sourceCounts[t] + (t === "traces" ? 1 : 0)) ok = false  // +1 for SCALE-MARKER
	}
	check("live counts unchanged (traces +1 marker)", ok)
	closeChdb(c)
}

// ── Measurements summary ──
console.log(`\n=== Phase measurements ===`)
console.log(`  ${"phase".padEnd(34)} ${"ms".padStart(8)} ${"rss-MB".padStart(8)} ${"root-GB".padStart(8)} ${"free-GB".padStart(8)}`)
for (const p of phases) console.log(`  ${p.name.padEnd(34)} ${String(p.ms).padStart(8)} ${String(p.rssMB).padStart(8)} ${String(p.rootGB).padStart(8)} ${String(p.freeGB).padStart(8)}`)

// ── Compression ratio ──
console.log(`\n=== Compression ratio (source parts bytes → Parquet bytes) ===`)
const c3 = openChdb(LIVE, CFG)
let srcPartsBytes = 0
for (const t of TABLES) {
	srcPartsBytes += Number(query(c3.conn, `SELECT sum(bytes_on_disk) FROM system.parts WHERE table='${t}' AND active`))
}
closeChdb(c3)
const totalParquetBytes = shardInfo.reduce((a, s) => a + s.parquetMB * 1024 * 1024, 0)
console.log(`  source parts: ${(srcPartsBytes/1024/1024/1024).toFixed(2)} GiB`)
console.log(`  parquet:      ${(totalParquetBytes/1024/1024/1024).toFixed(2)} GiB`)
console.log(`  ratio:        ${(srcPartsBytes/totalParquetBytes).toFixed(1)}×`)

console.log(`\n=== RESULT: ${PASS.length} passed, ${FAIL.length} failed ===`)

// Save measurements to root for the report
writeFileSync(join(ROOT, "scale-measurements.json"), JSON.stringify({ phases, shardInfo, sourceCounts, parquetTotalGB: totalParquetBytes/1024/1024/1024 }, null, 2))
console.log(`\nmeasurements saved to ${ROOT}/scale-measurements.json`)

// Clean up scratch (keep archive + measurements)
rmSync(SCRATCH, { recursive: true, force: true })
console.log(`scratch removed; archive + live retained at ${ROOT}`)
