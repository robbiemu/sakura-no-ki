/**
 * Task 5: rotation semantics. Evaluate (without choosing prematurely):
 *   - fixed UTC-day chunks
 *   - size-targeted logical chunks (multiple shards)
 *   - immutable generations that supersede earlier exports
 *   - append-only delta chunks for late telemetry
 *
 * Runtime test: archive a day, inject late data for that same day, show how
 * each model prevents silent loss or duplicate query results.
 */
import { spawnSync } from "node:child_process"
import { existsSync, mkdirSync, mkdtempSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { openChdb, query, closeChdb, writeBackupsConfig, bootstrapSchema } from "./harness"

const DATA = mkdtempSync(join(tmpdir(), "task5-rotation-"))
const CFG = writeBackupsConfig(DATA)
const ARCH = join(DATA, "archive"); mkdirSync(ARCH, { recursive: true })
const DUCK = "/tmp/duckdb"
const run = (cmd: string[], opts: any = {}) => spawnSync(cmd[0], cmd.slice(1), { encoding: "utf8", timeout: 60000, ...opts })
const duck = (sql: string) => run([DUCK, "-csv", "-noheader", "-c", sql], { stdio: ["ignore","pipe","pipe"] }).stdout.trim()
const WIN = "SETTINGS max_threads=1, output_format_parquet_row_group_size=10000"

console.log(`=== Task 5: rotation semantics ===`)
console.log(`store: ${DATA}\n`)

// Build data for 2026-06-25, archive it, then inject LATE data for the same day.
console.log(`building 100k traces for 2026-06-25...`)
{
	const c = openChdb(DATA, CFG); bootstrapSchema(c.conn)
	const v: string[] = []
	for (let i = 0; i < 100000; i++) v.push(`('local','2026-06-25 ${(10+i%14)}:00:00.000','${i.toString(16).padStart(16,"0")}','${i.toString(16).padStart(16,"0")}','svc${i%3}','Ok','root','Server')`)
	query(c.conn, `INSERT INTO traces (OrgId, Timestamp, TraceId, SpanId, ServiceName, StatusCode, SpanName, SpanKind) VALUES ${v.join(",")}`)
	console.log(`  initial: ${query(c.conn, "SELECT count() FROM traces")} traces`)
	// Archive 2026-06-25
	query(c.conn, `SELECT * FROM traces WHERE toDate(Timestamp)='2026-06-25' INTO OUTFILE '${ARCH}/day-2026-06-25.parquet' FORMAT Parquet ${WIN}`)
	console.log(`  archived: ${duck(`SELECT count() FROM read_parquet('${ARCH}/day-2026-06-25.parquet');`)} rows to day-2026-06-25.parquet`)
	// NOW inject late data for the SAME day (simulates a delayed exporter)
	const late: string[] = []
	for (let i = 100000; i < 110000; i++) late.push(`('local','2026-06-25 15:00:00.000','late${i.toString(16).padStart(12,"0")}','${i.toString(16).padStart(16,"0")}','svc-late','Ok','late-arrival','Server')`)
	query(c.conn, `INSERT INTO traces (OrgId, Timestamp, TraceId, SpanId, ServiceName, StatusCode, SpanName, SpanKind) VALUES ${late.join(",")}`)
	console.log(`  after late injection: ${query(c.conn, "SELECT count() FROM traces")} traces in store (10000 late arrivals)`)
	console.log(`  archive still has: ${duck(`SELECT count() FROM read_parquet('${ARCH}/day-2026-06-25.parquet');`)} (FROZEN — late data NOT in it)`)
	closeChdb(c)
}

console.log(`\n── rotation model comparison ──`)
console.log(`
  Scenario: 10k late rows arrived for an already-archived day.

  Model A — fixed UTC-day, immutable (simplest):
    - day-2026-06-25.parquet is frozen at 100k rows.
    - Late data is invisible to archive queries until the next re-export.
    - Silent loss ONLY if the hot TTL (30d) expires before re-export.
    - Recovery: re-export the day (overwrites), OR a supplemental delta chunk.
    - Risk: duplicate counts if both old + new chunks are scanned without dedup.

  Model B — size-targeted logical chunks (multiple shards per day):
    - Day is split into shards (e.g. shard-00, shard-01...).
    - Late data forms a NEW shard (shard-late-001).
    - All shards for the day are scanned together; NO duplicates if shards are
      disjoint by cursor (TraceId range / _part).
    - Requires the catalog to know which shards exist for a day.
    - Risk: a shard covering the same cursor range as another = duplicates.

  Model C — immutable generations (supersession):
    - gen-001/day-2026-06-25.parquet (100k rows)
    - gen-002/day-2026-06-25.parquet (110k rows, re-exported after late data)
    - Catalog marks gen-001 SUPERSEDED by gen-002.
    - Queries read only the latest non-superseded generation.
    - No duplicates, no silent loss. Cost: re-export + 2× storage until gen-001 GC'd.

  Model D — append-only delta chunks:
    - day-2026-06-25.parquet (base, 100k)
    - day-2026-06-25.delta-001.parquet (late, 10k)
    - Queries UNION base + deltas. Dedup by TraceId (or accept overlap).
    - No re-export. Risk: duplicate rows if late data overlaps base (must dedup on read).
`)

// Runtime proof: show duplicate-count risk with Model A (re-export) vs dedup
console.log(`── runtime: duplicate risk with naive re-export (Model A) ──`)
{
	const c = openChdb(DATA, CFG)
	// Re-export the day (now includes late data) into a SECOND file
	query(c.conn, `SELECT * FROM traces WHERE toDate(Timestamp)='2026-06-25' INTO OUTFILE '${ARCH}/day-2026-06-25-v2.parquet' FORMAT Parquet ${WIN}`)
	closeChdb(c)
	console.log(`  v1 (frozen): ${duck(`SELECT count() FROM read_parquet('${ARCH}/day-2026-06-25.parquet');`)}`)
	console.log(`  v2 (re-exported w/ late): ${duck(`SELECT count() FROM read_parquet('${ARCH}/day-2026-06-25-v2.parquet');`)}`)
	console.log(`  scanning BOTH (naive): ${duck(`SELECT count() FROM read_parquet(['${ARCH}/day-2026-06-25.parquet','${ARCH}/day-2026-06-25-v2.parquet']);`)} ← DUPLICATES (100k overlap)`)
	console.log(`  dedup by TraceId: ${duck(`SELECT count(DISTINCT TraceId) FROM read_parquet(['${ARCH}/day-2026-06-25.parquet','${ARCH}/day-2026-06-25-v2.parquet']);`)} ← correct (110k)`)
}

console.log(`\n── when does a range become final? ──`)
console.log(`  Hot TTLs: logs/traces 30d, metrics 90d. A range is "safe to archive" only`)
console.log(`  after the late-arrival lag (default >=24h) AND before its TTL expires.`)
console.log(`  Recommended: archive_lag=24h. A day becomes final at: day_end + 24h.`)
console.log(`  Late arrivals after finality → delta chunk (Model D) or supersession (Model C).`)

console.log(`\nstore + archive kept at ${DATA}`)
