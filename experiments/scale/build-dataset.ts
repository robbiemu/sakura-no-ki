/**
 * Scale dataset builder v3 — fast (array.join, no O(n²) string concat).
 * Builds a ~4 GiB hot store across 6 tables. Writes to the dedicated scale-test root.
 */
import { spawnSync } from "node:child_process"
import { mkdirSync, readFileSync } from "node:fs"
import { join } from "node:path"
import { openChdb, query, closeChdb, bootstrapSchema, writeBackupsConfig } from "../archives/harness"

const ROOT = readFileSync("/tmp/maple-scale-root.txt", "utf8").trim()
const LIVE = join(ROOT, "live")
mkdirSync(LIVE, { recursive: true })
const CFG = writeBackupsConfig(LIVE)
const sh = (c: string) => { try { return spawnSync("sh", ["-c", c], { encoding: "utf8", timeout: 60000 }).stdout.trim() } catch { return "?" } }

console.log(`=== Scale dataset builder v3 ===`)
console.log(`live: ${LIVE}\n`)
const c = openChdb(LIVE, CFG); bootstrapSchema(c.conn)
const BATCH = 50000
const t0 = performance.now()
const ins = (header: string, tuples: string[]) => query(c.conn, header + " VALUES " + tuples.join(","))

// logs: 4M rows, wide bodies (~1.5 GiB)
console.log(`logs (4M rows)...`)
for (let s = 0; s < 4_000_000; s += BATCH) {
	const t: string[] = []
	for (let i = s; i < s + BATCH; i++) {
		const hh = (10+i%14).toString().padStart(2,"0"), mm = (i%60).toString().padStart(2,"0")
		t.push(`('local','2026-06-25 ${hh}:${mm}:00.000','2026-06-25 ${hh}:${mm}:00','${i%4===0?"ERROR":"INFO"}','svc${i%6}','req ${i} timeout db-${i%8}:5432 user=${i%5000} region=us-${i%3}',map('h','h${i%8}'))`)
	}
	ins("INSERT INTO logs (OrgId, Timestamp, TimestampTime, SeverityText, ServiceName, Body, ResourceAttributes)", t)
	if ((s/BATCH)%8===0) process.stderr.write(`  logs ${s+BATCH}/4000000 (${((performance.now()-t0)/1000).toFixed(0)}s)\r`)
}
// traces: 3M rows
console.log(`\ntraces (3M rows)...`)
for (let s = 0; s < 3_000_000; s += BATCH) {
	const t: string[] = []
	for (let i = s; i < s + BATCH; i++) { const id = i.toString(16).padStart(16,"0"); t.push(`('local','2026-06-25 ${(10+i%14)}:00:00.000','${id}','${id}','svc${i%5}','${i%5===0?"Error":"Ok"}','${i%5===0?"err":""}','Server',map('r','/u${i%100}'),${1e6+i})`) }
	ins("INSERT INTO traces (OrgId, Timestamp, TraceId, SpanId, ServiceName, StatusCode, StatusMessage, SpanKind, SpanAttributes, Duration)", t)
	if ((s/BATCH)%8===0) process.stderr.write(`  traces ${s+BATCH}/3000000 (${((performance.now()-t0)/1000).toFixed(0)}s)\r`)
}
// metrics_sum + gauge: 1.5M each
for (const [table, base] of [["metrics_sum","dur"],["metrics_gauge","mem"]] as [string,string][]) {
	console.log(`\n${table} (1.5M rows)...`)
	for (let s = 0; s < 1_500_000; s += BATCH) {
		const t: string[] = []
		for (let i = s; i < s + BATCH; i++) t.push(`('local','svc${i%4}','${base}',${(i%1000*0.01).toFixed(3)},'2026-06-25 ${(10+i%14)}:00:00.000',map('h','h${i%8}'))`)
		ins(`INSERT INTO ${table} (OrgId, ServiceName, MetricName, Value, TimeUnix, Attributes)`, t)
		if ((s/BATCH)%4===0) process.stderr.write(`  ${table} ${s+BATCH}/1500000\r`)
	}
}
// metrics_histogram: 800k
console.log(`\nmetrics_histogram (800k)...`)
for (let s = 0; s < 800_000; s += BATCH) {
	const t: string[] = []
	for (let i = s; i < s + BATCH; i++) t.push(`('local','svc${i%4}','lat','2026-06-25 ${(10+i%14)}:00:00.000',map('h','h${i%8}'),${i%100},${(i*0.5).toFixed(1)},[0.1,0.5,1,5,10],[${i%20},${(i+1)%20},${(i+2)%20},${(i+3)%20},${(i+4)%20}])`)
	ins("INSERT INTO metrics_histogram (OrgId, ServiceName, MetricName, TimeUnix, Attributes, Count, Sum, ExplicitBounds, BucketCounts)", t)
}
// metrics_exponential_histogram: 800k
console.log(`\nmetrics_exponential_histogram (800k)...`)
for (let s = 0; s < 800_000; s += BATCH) {
	const t: string[] = []
	for (let i = s; i < s + BATCH; i++) t.push(`('local','svc${i%4}','elat','2026-06-25 ${(10+i%14)}:00:00.000',map('h','h${i%8}'),${i%100},${(i*0.5).toFixed(1)},${i%20-10},${i%5},${i%30},[${i%20},${(i+1)%20},${(i+2)%20},${(i+3)%20},${(i+4)%20}])`)
	ins("INSERT INTO metrics_exponential_histogram (OrgId, ServiceName, MetricName, TimeUnix, Attributes, Count, Sum, Scale, ZeroCount, PositiveOffset, PositiveBucketCounts)", t)
}

const totalS = ((performance.now()-t0)/1000).toFixed(0)
closeChdb(c)

// Size breakdown
console.log(`\n=== Dataset built in ${totalS}s ===`)
const c2 = openChdb(LIVE, CFG)
console.log(`  ${"table".padEnd(32)} ${"rows".padStart(12)} ${"parts-size".padStart(12)} ${"uncompressed".padStart(14)}`)
console.log(`  ${"-".repeat(32)} ${"-".repeat(12)} ${"-".repeat(12)} ${"-".repeat(14)}`)
let totalRows = 0
for (const t of ["logs","traces","metrics_sum","metrics_gauge","metrics_histogram","metrics_exponential_histogram"]) {
	const rows = Number(query(c2.conn, `SELECT count() FROM ${t}`))
	const sz = query(c2.conn, `SELECT formatReadableSize(sum(bytes_on_disk)), formatReadableSize(sum(data_uncompressed_bytes)) FROM system.parts WHERE table='${t}' AND active`).split("\t")
	totalRows += rows
	console.log(`  ${t.padEnd(32)} ${String(rows).padStart(12)} ${(sz[0]||"?").padStart(12)} ${(sz[1]||"?").padStart(14)}`)
}
closeChdb(c2)
console.log(`  ${"(total rows)".padEnd(32)} ${String(totalRows).padStart(12)}`)
console.log(`\n  filesystem (du store/): ${sh(`du -sh ${LIVE}/store | cut -f1`)}`)
