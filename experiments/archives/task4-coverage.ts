/**
 * Task 4: raw table coverage round-trip. Export all 6 archive tables, verify complex
 * values survive (large bodies, maps, arrays, exemplars, nullable fields, ns timestamps),
 * and demonstrate useful DuckDB investigations per signal type.
 */
import { spawnSync } from "node:child_process"
import { existsSync, mkdirSync, mkdtempSync, readdirSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { openChdb, query, closeChdb, writeBackupsConfig, bootstrapSchema } from "./harness"

const DATA = mkdtempSync(join(tmpdir(), "task4-coverage-"))
const CFG = writeBackupsConfig(DATA)
const OUT = join(DATA, "archive")
const DUCK = "/tmp/duckdb"
const WIN = "SETTINGS max_threads=1, output_format_parquet_row_group_size=10000"
const run = (cmd: string[], opts: any = {}) => spawnSync(cmd[0], cmd.slice(1), { encoding: "utf8", timeout: 60000, ...opts })
const duck = (sql: string) => run([DUCK, "-csv", "-noheader", "-c", sql], { stdio: ["ignore","pipe","pipe"] }).stdout.trim()

console.log(`=== Task 4: raw table coverage round-trip ===`)
console.log(`store: ${DATA}\n`)

// Build representative data for all 6 tables
console.log(`building representative data...`)
{
	const c = openChdb(DATA, CFG); bootstrapSchema(c.conn)
	// logs: large bodies + severity + maps
	const lv: string[] = []
	for (let i = 0; i < 50000; i++) {
		lv.push(`('local','2026-06-25 ${(10+i%14)}:00:00.000','2026-06-25 ${(10+i%14)}:00:00','${i%2===0?"ERROR":"INFO"}','svc${i%3}','request ${i} to upstream-${i%4} failed: connection refused to db-${i%8}.internal:5432 trace=${i.toString(16).padStart(16,"0")}')`)
	}
	query(c.conn, `INSERT INTO logs (OrgId, Timestamp, TimestampTime, SeverityText, ServiceName, Body) VALUES ${lv.join(",")}`)
	// traces: wide attributes + error events + ns timestamps
	const tv: string[] = []
	for (let i = 0; i < 50000; i++) {
		const id = i.toString(16).padStart(16, "0")
		tv.push(`('local','2026-06-25 ${(10+i%14)}:00:00.000','${id}','${id}','svc${i%3}','${i%3===0?"Error":"Ok"}','${i%3===0?"request failed":"ok"}','Server',map('http.route','/users/${i%50}','db.statement','SELECT * FROM t WHERE id=${i}'),${1000000+i})`)
	}
	query(c.conn, `INSERT INTO traces (OrgId, Timestamp, TraceId, SpanId, ServiceName, StatusCode, StatusMessage, SpanKind, SpanAttributes, Duration) VALUES ${tv.join(",")}`)
	// metrics: all 4 types with arrays + exemplars
	for (const [table, valExpr] of [["metrics_sum", `'${(0.42).toString()}'`],["metrics_gauge", `'${(1024.5).toString()}'`]] as [string,string][]) {
		const mv: string[] = []
		for (let i = 0; i < 20000; i++) mv.push(`('local','svc${i%3}','metric.${table.split("_")[1]}',${valExpr},'2026-06-25 10:00:00.000',map('host','h${i%4}'))`)
		query(c.conn, `INSERT INTO ${table} (OrgId, ServiceName, MetricName, Value, TimeUnix, Attributes) VALUES ${mv.join(",")}`)
	}
	// histogram: BucketCounts + ExplicitBounds arrays
	{
		const hv: string[] = []
		for (let i = 0; i < 20000; i++) {
			hv.push(`('local','svc${i%3}','latency','2026-06-25 10:00:00.000',map('host','h${i%4}'),${i%100},${(i*0.5).toFixed(1)},[10,20,30,40],[${i%10},${(i+1)%10},${(i+2)%10},${(i+3)%10}])`)
		}
		query(c.conn, `INSERT INTO metrics_histogram (OrgId, ServiceName, MetricName, TimeUnix, Attributes, Count, Sum, ExplicitBounds, BucketCounts) VALUES ${hv.join(",")}`)
	}
	// exponential_histogram: PositiveBucketCounts + Scale/ZeroCount/PositiveOffset
	{
		const ev: string[] = []
		for (let i = 0; i < 20000; i++) {
			ev.push(`('local','svc${i%3}','exp.latency','2026-06-25 10:00:00.000',map('host','h${i%4}'),${i%100},${(i*0.5).toFixed(1)},${i%20-10},${i%5},${i%30},[${i%10},${(i+1)%10},${(i+2)%10},${(i+3)%10}])`)
		}
		query(c.conn, `INSERT INTO metrics_exponential_histogram (OrgId, ServiceName, MetricName, TimeUnix, Attributes, Count, Sum, Scale, ZeroCount, PositiveOffset, PositiveBucketCounts) VALUES ${ev.join(",")}`)
	}
	console.log(`  logs: ${query(c.conn, "SELECT count() FROM logs")}`)
	console.log(`  traces: ${query(c.conn, "SELECT count() FROM traces")}`)
	console.log(`  metrics_sum: ${query(c.conn, "SELECT count() FROM metrics_sum")}`)
	console.log(`  metrics_gauge: ${query(c.conn, "SELECT count() FROM metrics_gauge")}`)
	console.log(`  metrics_histogram: ${query(c.conn, "SELECT count() FROM metrics_histogram")}`)
	console.log(`  metrics_exponential_histogram: ${query(c.conn, "SELECT count() FROM metrics_exponential_histogram")}`)
	closeChdb(c)
}

// Export all 6
console.log(`\nexporting all 6 tables...`)
mkdirSync(OUT, { recursive: true })
{
	const c = openChdb(DATA, CFG)
	for (const t of ["logs", "traces", "metrics_sum", "metrics_gauge", "metrics_histogram", "metrics_exponential_histogram"]) {
		const out = join(OUT, `${t}.parquet`)
		query(c.conn, `SELECT * FROM ${t} INTO OUTFILE '${out}' FORMAT Parquet ${WIN}`)
	}
	closeChdb(c)
}

// ── Round-trip verification + useful investigations per signal ──
console.log(`\n── DuckDB round-trip + investigations ──`)

console.log(`\n[logs] body substring search + severity breakdown:`)
console.log(`  count: ${duck(`SELECT count() FROM read_parquet('${OUT}/logs.parquet');`)}`)
console.log(`  body LIKE '%connection refused%': ${duck(`SELECT count() FROM read_parquet('${OUT}/logs.parquet') WHERE Body LIKE '%connection refused%';`)}`)
console.log(`  severity×service:\n${duck(`SELECT ServiceName, SeverityText, count() FROM read_parquet('${OUT}/logs.parquet') GROUP BY 1,2 ORDER BY 1,2;`).split("\n").map((l:string)=>`    ${l}`).join("\n")}`)

console.log(`\n[traces] error investigation + duration analysis:`)
console.log(`  count: ${duck(`SELECT count() FROM read_parquet('${OUT}/traces.parquet');`)}`)
console.log(`  error traces by service: ${duck(`SELECT ServiceName, count() FROM read_parquet('${OUT}/traces.parquet') WHERE StatusCode='Error' GROUP BY 1 ORDER BY 2 DESC LIMIT 3;`).replace(/\n/g," | ")}`)
console.log(`  p99 Duration (ns): ${duck(`SELECT quantile_cont(Duration, 0.99) FROM read_parquet('${OUT}/traces.parquet');`)}`)
console.log(`  map attr access: ${duck(`SELECT count() FROM read_parquet('${OUT}/traces.parquet') WHERE SpanAttributes['db.statement'] IS NOT NULL;`)}`)

console.log(`\n[metrics_sum/gauge] threshold investigation:`)
console.log(`  sum max per service: ${duck(`SELECT ServiceName, max(Value) FROM read_parquet('${OUT}/metrics_sum.parquet') GROUP BY 1 ORDER BY 2 DESC LIMIT 3;`).replace(/\n/g," | ")}`)
console.log(`  gauge > 1000: ${duck(`SELECT count() FROM read_parquet('${OUT}/metrics_gauge.parquet') WHERE Value > 1000;`)}`)

console.log(`\n[metrics_histogram] bucket aggregation:`)
console.log(`  total BucketCounts sum: ${duck(`SELECT sum(c) FROM (SELECT unnest(BucketCounts) AS c FROM read_parquet('${OUT}/metrics_histogram.parquet'));`)}`)
console.log(`  ExplicitBounds sample: ${duck(`SELECT ExplicitBounds FROM read_parquet('${OUT}/metrics_histogram.parquet') LIMIT 1;`)}`)

console.log(`\n[metrics_exponential_histogram] (same shape):`)
console.log(`  count: ${duck(`SELECT count() FROM read_parquet('${OUT}/metrics_exponential_histogram.parquet');`)}`)

console.log(`\n── nullable fields + ns timestamps survive? ──`)
console.log(`  traces.Duration (UInt64 ns) min/max: ${duck(`SELECT min(Duration), max(Duration) FROM read_parquet('${OUT}/traces.parquet');`)}`)
console.log(`  logs.Timestamp (DateTime64(9)) sample: ${duck(`SELECT min(Timestamp)::VARCHAR, max(Timestamp)::VARCHAR FROM read_parquet('${OUT}/logs.parquet');`)}`)

console.log(`\n── tables out of scope (session replay, derived MVs) ──`)
console.log(`  Derived MV targets (error_events, trace_list_mv, logs_aggregates_hourly,`)
console.log(`  service_overview_spans, etc.) are NOT in archive scope — they're derivable`)
console.log(`  from raw tables and have intentionally different TTLs. Document their`)
console.log(`  exclusion in the manifest but don't archive them.`)
console.log(`  Session replay data: no such table in the current local schema.`)

console.log(`\nstore + archive kept at ${DATA}`)
