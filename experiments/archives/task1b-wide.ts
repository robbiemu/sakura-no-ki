/**
 * Task 1b: verify winning settings (max_threads=1, row_group_size=10000) hold for
 * WIDE data (large bodies, map attributes) and all signal types. Measures peak RSS
 * and confirms DuckDB can round-trip complex values.
 */
import { spawnSync } from "node:child_process"
import { existsSync, mkdtempSync, statSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { openChdb, query, closeChdb, writeBackupsConfig, bootstrapSchema } from "./harness"

const DATA = mkdtempSync(join(tmpdir(), "task1b-wide-"))
const CFG = writeBackupsConfig(DATA)
const DUCK = "/tmp/duckdb"

console.log(`=== Task 1b: wide data with winning settings (mt=1, rg=10000) ===`)
console.log(`store: ${DATA}\n`)

// Build wide logs + traces + all 4 metric types
console.log(`building 1M wide logs, 1M wide traces, metrics...`)
{
	const c = openChdb(DATA, CFG)
	bootstrapSchema(c.conn)
	const BATCH = 10000
	for (let s = 0; s < 1_000_000; s += BATCH) {
		const lv: string[] = [], tv: string[] = []
		for (let i = s; i < Math.min(s + BATCH, 1_000_000); i++) {
			const id = i.toString(16).padStart(16, "0")
			lv.push(`('local','2026-06-25 10:00:00.000','2026-06-25 10:00:00','ERROR','svc${i%4}','request ${i} failed: upstream timeout connecting to db-${i%8}.internal:5432 after 30000ms trace=${id} user_id=${i%5000} region=us-${i%3}')`)
			// ClickHouse Map syntax uses single-quoted keys/values, not JSON.
			tv.push(`('local','2026-06-25 10:00:00.000','${id}','${id}','svc${i%5}','${i%5===0?"Error":"Ok"}','root','Server',map('http.route','/api/v1/users/${i%100}/orders','db.statement','SELECT * FROM orders WHERE id = ${i}'))`)
		}
		query(c.conn, `INSERT INTO logs (OrgId, Timestamp, TimestampTime, SeverityText, ServiceName, Body) VALUES ${lv.join(",")}`)
		query(c.conn, `INSERT INTO traces (OrgId, Timestamp, TraceId, SpanId, ServiceName, StatusCode, SpanName, SpanKind, SpanAttributes) VALUES ${tv.join(",")}`)
	}
	// metrics: 100k rows per type, batched
	const MB = 10000
	for (let s = 0; s < 100000; s += MB) {
		const sv: string[] = [], gv: string[] = [], hv: string[] = []
		for (let i = s; i < Math.min(s + MB, 100000); i++) {
			const attrs = `map('host','h${i%4}','region','us-${i%2}')`
			sv.push(`('local','svc${i%3}','http.request.duration',${(i*0.001).toFixed(3)},'2026-06-25 10:00:00.000',${attrs})`)
			gv.push(`('local','svc${i%3}','memory.usage',${(i*10).toFixed(1)},'2026-06-25 10:00:00.000',${attrs})`)
			hv.push(`('local','svc${i%3}','latency','2026-06-25 10:00:00.000',${attrs},${i%100},${(i*0.5).toFixed(1)},[0.1,0.5,1.0,5.0],[${i%50},${(i+1)%50},${(i+2)%50},${(i+3)%50}])`)
		}
		query(c.conn, `INSERT INTO metrics_sum (OrgId, ServiceName, MetricName, Value, TimeUnix, Attributes) VALUES ${sv.join(",")}`)
		query(c.conn, `INSERT INTO metrics_gauge (OrgId, ServiceName, MetricName, Value, TimeUnix, Attributes) VALUES ${gv.join(",")}`)
		query(c.conn, `INSERT INTO metrics_histogram (OrgId, ServiceName, MetricName, TimeUnix, Attributes, Count, Sum, ExplicitBounds, BucketCounts) VALUES ${hv.join(",")}`)
	}
	console.log(`  logs: ${query(c.conn, "SELECT count() FROM logs")}`)
	console.log(`  traces: ${query(c.conn, "SELECT count() FROM traces")}`)
	console.log(`  metrics_sum: ${query(c.conn, "SELECT count() FROM metrics_sum")}`)
	closeChdb(c)
}

// Export each table with winning settings, measure peak RSS.
const WIN = JSON.stringify({ max_threads: "1", output_format_parquet_row_group_size: "10000" })
console.log(`\n  ${"table".padEnd(28)} ${"ms".padStart(7)} ${"outMB".padStart(7)} ${"peakRSS-MB".padStart(11)}`)
console.log(`  ${"-".repeat(28)} ${"-".repeat(7)} ${"-".repeat(7)} ${"-".repeat(11)}`)

for (const table of ["logs", "traces", "metrics_sum", "metrics_gauge", "metrics_histogram"]) {
	const out = join(DATA, `${table}.parquet`)
	const r = spawnSync("/usr/bin/time", ["-l", "bun", "run", "export-one.ts", DATA, CFG, out, WIN, table], {
		cwd: import.meta.dir, encoding: "utf8", timeout: 180000,
	})
	const stdout = r.stdout || "", stderr = r.stderr || ""
	const jsonLine = stdout.split("\n").find((l) => l.trim().startsWith("{")) || "{}"
	const peakMatch = stderr.match(/(\d+)\s+maximum resident set size/)
	const peakMB = peakMatch ? Number(peakMatch[1]) / 1024 / 1024 : 0
	let parsed: { ms?: number; outMB?: number } = {}
	try { parsed = JSON.parse(jsonLine) } catch {}
	console.log(`  ${table.padEnd(28)} ${String(parsed.ms ?? "?").padStart(7)} ${(parsed.outMB ?? 0).toFixed(1).padStart(7)} ${peakMB.toFixed(0).padStart(11)}`)
}

// DuckDB roundtrip: verify complex values survive
console.log(`\n  ── DuckDB roundtrip (complex values) ──`)
const logP = join(DATA, "logs.parquet")
console.log(`  logs body LIKE '%timeout%' (substring in wide body):`)
spawnSync(DUCK, ["-c", `SELECT count() FROM read_parquet('${logP}') WHERE Body LIKE '%timeout%';`, "-noheader"], { encoding: "utf8", stdio: ["ignore","pipe","pipe"] }).stdout.split("\n").filter(Boolean).forEach((l: string) => console.log(`    ${l.trim()}`))

const trP = join(DATA, "traces.parquet")
console.log(`  traces SpanAttributes map access:`)
spawnSync(DUCK, ["-c", `SELECT count() FROM read_parquet('${trP}') WHERE SpanAttributes['http.route'] IS NOT NULL;`, "-noheader"], { encoding: "utf8", stdio: ["ignore","pipe","pipe"] }).stdout.split("\n").filter(Boolean).forEach((l: string) => console.log(`    ${l.trim()}`))

const histP = join(DATA, "metrics_histogram.parquet")
console.log(`  metrics_histogram Bounds array:`)
spawnSync(DUCK, ["-c", `SELECT Bounds, BucketCounts FROM read_parquet('${histP}') LIMIT 2;`, "-noheader"], { encoding: "utf8", stdio: ["ignore","pipe","pipe"] }).stdout.split("\n").filter(Boolean).forEach((l: string) => console.log(`    ${l.trim()}`))

console.log(`\nstore kept at ${DATA}`)
