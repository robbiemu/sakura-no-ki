/**
 * Task 1 runner: build a 10M-row trace store once, then export it with each
 * candidate setting combination, measuring peak RSS via /usr/bin/time -l.
 *
 * Each export runs in a FRESH subprocess (clean RSS baseline) so peak RSS is
 * attributable to that export only. macOS ru_maxrss is in BYTES.
 */
import { spawnSync } from "node:child_process"
import { mkdtempSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { openChdb, query, closeChdb, writeBackupsConfig, bootstrapSchema, insertTraces } from "./harness"

const N = 10_000_000
const DATA = mkdtempSync(join(tmpdir(), "task1-10m-"))
const CFG = writeBackupsConfig(DATA)

console.log(`=== Task 1: Parquet writer memory controls (10M rows) ===`)
console.log(`store: ${DATA}\n`)
console.log(`building ${N} traces...`)
{
	const c = openChdb(DATA, CFG)
	bootstrapSchema(c.conn)
	insertTraces(c.conn, N, { wide: false })
	console.log(`  count: ${query(c.conn, "SELECT count() FROM traces")}`)
	closeChdb(c)
}

const COMBOS: Array<{ label: string; settings: Record<string, string> }> = [
	{ label: "baseline (no settings)", settings: {} },
	{ label: "max_threads=1", settings: { max_threads: "1" } },
	{ label: "max_block_size=8192", settings: { max_block_size: "8192" } },
	{ label: "max_memory_usage=1GiB", settings: { max_memory_usage: "1073741824" } },
	{ label: "row_group_size=100000", settings: { output_format_parquet_row_group_size: "100000" } },
	{ label: "row_group_size=10000", settings: { output_format_parquet_row_group_size: "10000" } },
	{ label: "row_group_size_bytes=8MiB", settings: { output_format_parquet_row_group_size_bytes: "8388608" } },
	{ label: "parallel_encoding=0", settings: { output_format_parquet_parallel_encoding: "0" } },
	{ label: "max_threads=1 + rg=10000", settings: { max_threads: "1", output_format_parquet_row_group_size: "10000" } },
	{ label: "kitchen sink (mt1,rg10k,mbs8k)", settings: { max_threads: "1", output_format_parquet_row_group_size: "10000", max_block_size: "8192" } },
]

console.log(`\n  ${"setting".padEnd(40)} ${"ms".padStart(7)} ${"outMB".padStart(7)} ${"peakRSS-MB".padStart(11)}`)
console.log(`  ${"-".repeat(40)} ${"-".repeat(7)} ${"-".repeat(7)} ${"-".repeat(11)}`)

for (let i = 0; i < COMBOS.length; i++) {
	const { label, settings } = COMBOS[i]
	const outFile = join(DATA, `exp-${i}.parquet`)
	const settingsJson = JSON.stringify(settings)
	const r = spawnSync("/usr/bin/time", ["-l", "bun", "run", "export-one.ts", DATA, CFG, outFile, settingsJson], {
		cwd: import.meta.dir, encoding: "utf8", timeout: 180000,
	})
	const stdout = r.stdout || ""
	const stderr = r.stderr || ""
	const jsonLine = stdout.split("\n").find((l) => l.trim().startsWith("{")) || "{}"
	const peakMatch = stderr.match(/(\d+)\s+maximum resident set size/)
	const peakMB = peakMatch ? Number(peakMatch[1]) / 1024 / 1024 : 0
	let parsed: { ms?: number; outMB?: number } = {}
	try { parsed = JSON.parse(jsonLine) } catch {}
	const ms = parsed.ms ?? "?"
	const outMB = (parsed.outMB ?? 0).toFixed(1)
	const ok = r.status === 0
	const note = !ok ? `  ERR: ${stderr.split("\n")[0].slice(0, 50)}` : ""
	console.log(`  ${label.padEnd(40)} ${String(ms).padStart(7)} ${outMB.padStart(7)} ${peakMB.toFixed(0).padStart(11)}${note}`)
}

console.log(`\nstore kept at ${DATA} for inspection`)
