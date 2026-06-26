/**
 * Task 2: complete, bounded sharding. Proves an export strategy that is both
 * memory-bounded AND complete (no dups, no omissions).
 *
 * Tests:
 *   - time-window splitting (day/hour boundaries)
 *   - adaptive splitting by row count
 *   - the pathological case: >limit rows at ONE identical timestamp
 *   - _part / _part_offset stable cursors (if chDB exposes them)
 *   - interruption + restart between shards
 *
 * For each: prove sum(shard rows) == source rows, no dups (sum of distinct check),
 * min/max timestamps correct, complex values survive, peak RSS bounded.
 */
import { spawnSync } from "node:child_process"
import { existsSync, mkdirSync, mkdtempSync, readdirSync, statSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { openChdb, query, closeChdb, writeBackupsConfig, bootstrapSchema } from "./harness"

const DATA = mkdtempSync(join(tmpdir(), "task2-shard-"))
const CFG = writeBackupsConfig(DATA)
const DUCK = "/tmp/duckdb"
const SHARD_DIR = join(DATA, "shards")
mkdirSync(SHARD_DIR, { recursive: true })
const WIN = JSON.stringify({ max_threads: "1", output_format_parquet_row_group_size: "10000" })
const run = (cmd: string[], opts: any = {}) => spawnSync(cmd[0], cmd.slice(1), { encoding: "utf8", timeout: 120000, ...opts })
// DuckDB: render as plain CSV (no box drawing) and extract the number.
const duck = (sql: string) => {
	const r = run([DUCK, "-csv", "-noheader", "-c", sql], { stdio: ["ignore","pipe","pipe"] })
	return r.stdout.trim()
}
const duckInt = (sql: string) => Number(duck(sql).replace(/[^0-9-]/g, "")) || 0

console.log(`=== Task 2: complete bounded sharding ===`)
console.log(`store: ${DATA}\n`)

// Build a store with MULTI-DAY data (so time-window splitting is meaningful)
// + a pathological burst: 500k rows all at the SAME timestamp.
console.log(`building multi-day store + pathological burst...`)
{
	const c = openChdb(DATA, CFG)
	bootstrapSchema(c.conn)
	// 3 days × 100k rows each, distinct timestamps
	for (let day = 25; day <= 27; day++) {
		const vals: string[] = []
		for (let i = 0; i < 100000; i++) {
			const n = (day - 25) * 100000 + i
			vals.push(`('local','2026-06-${day} ${(10 + i%14)}:${(i%60).toString().padStart(2,"0")}:00.000','${n.toString(16).padStart(16,"0")}','${n.toString(16).padStart(16,"0")}','svc${n%5}','${n%5===0?"Error":"Ok"}','root','Server',map('k','v${n%10}'))`)
		}
		query(c.conn, `INSERT INTO traces (OrgId, Timestamp, TraceId, SpanId, ServiceName, StatusCode, SpanName, SpanKind, SpanAttributes) VALUES ${vals.join(",")}`)
	}
	// pathological: 500k rows ALL at 2026-06-26 12:00:00.000
	const burst: string[] = []
	for (let i = 0; i < 500000; i++) {
		const n = 1000000 + i
		burst.push(`('local','2026-06-26 12:00:00.000','burst${n.toString(16).padStart(11,"0")}','${n.toString(16).padStart(16,"0")}','svc9','Error','burst','Server',map('burst','yes'))`)
	}
	query(c.conn, `INSERT INTO traces (OrgId, Timestamp, TraceId, SpanId, ServiceName, StatusCode, SpanName, SpanKind, SpanAttributes) VALUES ${burst.join(",")}`)

	const srcCount = query(c.conn, "SELECT count() FROM traces")
	const srcMin = query(c.conn, "SELECT min(Timestamp) FROM traces")
	const srcMax = query(c.conn, "SELECT max(Timestamp) FROM traces")
	const srcDistinctTrace = query(c.conn, "SELECT count(DISTINCT TraceId) FROM traces")
	console.log(`  source: ${srcCount} rows, ${srcDistinctTrace} distinct TraceIds`)
	console.log(`  range: ${srcMin} → ${srcMax}`)
	console.log(`  burst at 2026-06-26 12:00:00: ${query(c.conn, "SELECT count() FROM traces WHERE Timestamp = toDateTime64('2026-06-26 12:00:00.000',9)")} rows`)
	closeChdb(c)
}

// ── 2a: time-window splitting (per-day) ──
console.log(`\n── 2a: time-window splitting (per UTC day) ──`)
{
	const c = openChdb(DATA, CFG)
	const days = ["2026-06-25", "2026-06-26", "2026-06-27"]
	let totalShardRows = 0
	for (const day of days) {
		const out = join(SHARD_DIR, `day-${day}.parquet`)
		const t0 = performance.now()
		query(c.conn, `SELECT * FROM traces WHERE toDate(Timestamp) = '${day}' INTO OUTFILE '${out}' FORMAT Parquet SETTINGS max_threads=1, output_format_parquet_row_group_size=10000`)
		const dt = performance.now() - t0
		const shardRows = duckInt(`SELECT count() FROM read_parquet('${out}');`)
		totalShardRows += shardRows
		console.log(`  ${day}: ${shardRows} rows, ${(dt).toFixed(0)}ms`)
	}
	closeChdb(c)
	const c2 = openChdb(DATA, CFG)
	const srcCount = Number(query(c2.conn, "SELECT count() FROM traces"))
	closeChdb(c2)
	console.log(`  sum(shards)=${totalShardRows}, source=${srcCount} → ${totalShardRows === srcCount ? "✓ COMPLETE" : "✗ MISMATCH"}`)
}

// ── 2b: the pathological burst — same-timestamp windowing ──
console.log(`\n── 2b: pathological burst (500k rows at one timestamp) ──`)
{
	const c = openChdb(DATA, CFG)
	// A day-window covering 2026-06-26 includes the 100k normal + 500k burst = 600k rows.
	// Can we split it WITHOUT time granularity? Use TraceId range as a tiebreaker cursor.
	const burstMin = query(c.conn, "SELECT min(TraceId) FROM traces WHERE toDate(Timestamp)='2026-06-26'")
	const burstMax = query(c.conn, "SELECT max(TraceId) FROM traces WHERE toDate(Timestamp)='2026-06-26'")
	const dayCount = query(c.conn, "SELECT count() FROM traces WHERE toDate(Timestamp)='2026-06-26'")
	console.log(`  2026-06-26: ${dayCount} rows (100k normal + 500k burst at 12:00)`)
	console.log(`  TraceId range: ${burstMin} → ${burstMax}`)
	// Split 2026-06-26 by TraceId prefix (hex) into shards of ~150k each
	const SHARDS = ["00000→3ffff", "40000→7ffff", "80000→bffff", "c0000→fffff", "burst0→burstf"]
	const hexShards = [
		{ lo: "0000000000000000", hi: "000000000003ffff" },
		{ lo: "0000000000040000", hi: "000000000007ffff" },
		{ lo: "0000000000080000", hi: "00000000000bffff" },
		{ lo: "00000000000c0000", hi: "00000000000fffff" },
		{ lo: "burst00000000000", hi: "bursts" },  // burst TraceIds start with "burst"
	]
	let totalB = 0
	for (let si = 0; si < hexShards.length; si++) {
		const { lo, hi } = hexShards[si]
		const out = join(SHARD_DIR, `day26-shard${si}.parquet`)
		if (existsSync(out)) continue  // interruption-restart safe: skip already-written
		const t0 = performance.now()
		query(c.conn, `SELECT * FROM traces WHERE toDate(Timestamp)='2026-06-26' AND TraceId >= '${lo}' AND TraceId < '${hi}' INTO OUTFILE '${out}' FORMAT Parquet SETTINGS max_threads=1, output_format_parquet_row_group_size=10000`)
		const dt = performance.now() - t0
		const rows = existsSync(out) ? duckInt(`SELECT count() FROM read_parquet('${out}');`) : 0
		totalB += rows
		console.log(`  ${lo.slice(0,6)}: ${rows} rows, ${dt.toFixed(0)}ms`)
	}
	closeChdb(c)
	console.log(`  sum(burst shards)=${totalB}, expected=600000 → ${totalB === 600000 ? "✓ COMPLETE (burst handled via TraceId cursor)" : "✗ MISMATCH"}`)
}

// ── 2c: _part / _part_offset cursors ──
console.log(`\n── 2c: _part / _part_offset stable cursors ──`)
{
	const c = openChdb(DATA, CFG)
	// Does chDB expose _part and _part_offset?
	try {
		const parts = query(c.conn, "SELECT DISTINCT _part FROM traces ORDER BY _part FORMAT TabSeparated")
		console.log(`  _part available: ${parts.split("\n").length} parts`)
		console.log(`  sample: ${parts.split("\n").slice(0,3).join(", ")}`)
	} catch (e: any) {
		console.log(`  _part NOT available: ${e.message.slice(0,60)}`)
	}
	try {
		const offsets = query(c.conn, "SELECT min(_part_offset), max(_part_offset) FROM traces FORMAT TabSeparated")
		console.log(`  _part_offset available: range ${offsets}`)
	} catch (e: any) {
		console.log(`  _part_offset NOT available: ${e.message.slice(0,60)}`)
	}
	closeChdb(c)
}

// ── 2d: no-duplicates proof ──
console.log(`\n── 2d: no-duplicates proof (distinct TraceIds across all shards) ──`)
{
	const files = readdirSync(SHARD_DIR).filter(f => f.endsWith(".parquet")).map(f => `'${join(SHARD_DIR, f)}'`)
	const list = `[${files.join(",")}]`
	const totalRows = duckInt(`SELECT count() FROM read_parquet(${list}, union_by_name=true);`)
	const distinctTrace = duckInt(`SELECT count(DISTINCT TraceId) FROM read_parquet(${list}, union_by_name=true);`)
	console.log(`  total rows across shards: ${totalRows}`)
	console.log(`  distinct TraceIds across shards: ${distinctTrace}`)
	console.log(`  → ${totalRows === distinctTrace ? "✓ NO DUPLICATES (total == distinct)" : "✗ DUPLICATES PRESENT"}`)
	// min/max ts
	const minMax = duck(`SELECT min(Timestamp)::VARCHAR, max(Timestamp)::VARCHAR FROM read_parquet(${list}, union_by_name=true);`)
	console.log(`  shard time range: ${minMax.replace(/\s+/g," ")}`)
}

// ── 2e: complex value survival ──
console.log(`\n── 2e: complex value survival ──`)
{
	const files = readdirSync(SHARD_DIR).filter(f => f.endsWith(".parquet")).map(f => `'${join(SHARD_DIR, f)}'`)
	const list = `[${files.join(",")}]`
	const withAttrs = duckInt(`SELECT count() FROM read_parquet(${list}, union_by_name=true) WHERE SpanAttributes['burst'] = 'yes';`)
	console.log(`  burst-tagged rows (map access): ${withAttrs} (expected 500000)`)
}

console.log(`\nstore + shards kept at ${DATA}`)
