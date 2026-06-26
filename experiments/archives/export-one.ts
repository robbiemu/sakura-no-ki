/**
 * Task 1 subprocess: open a pre-built store, run ONE export with given SETTINGS,
 * print JSON {ms, outMB}. Peak RSS measured externally via /usr/bin/time -l.
 *
 * Usage: bun run export-one.ts <dataDir> <cfg> <out> <settingsJson> [table]
 *   settingsJson: JSON object of SET-key=value pairs, e.g. '{"max_threads":"1"}'
 *   table: defaults to "traces"
 */
import { existsSync, statSync } from "node:fs"
import { openChdb, query, closeChdb, writeBackupsConfig } from "./harness"

const [dataDir, cfg, outPath, settingsJson, table = "traces"] = process.argv.slice(2)
const settings: Record<string, string> = settingsJson ? JSON.parse(settingsJson) : {}
const setClause = Object.keys(settings).length
	? " SETTINGS " + Object.entries(settings).map(([k, v]) => `${k}=${v}`).join(", ")
	: ""

const c = openChdb(dataDir, cfg)
const t0 = performance.now()
try {
	query(c.conn, `SELECT * FROM ${table} INTO OUTFILE '${outPath}' FORMAT Parquet${setClause}`)
} finally {
	closeChdb(c)
}
const ms = Math.round(performance.now() - t0)
const outMB = existsSync(outPath) ? +(statSync(outPath).size / 1024 / 1024).toFixed(1) : 0
console.log(JSON.stringify({ ms, outMB }))
