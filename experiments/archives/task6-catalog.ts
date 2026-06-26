/**
 * Task 6: durability and catalog options. Compare:
 *   1. manifests as source of truth, rebuildable catalog
 *   2. append-only catalog.jsonl
 *   3. small transactional catalog (SQLite/libSQL)
 *
 * Simulate interruption at each archive phase and identify the minimum state
 * machine that never advertises an incomplete chunk.
 */
import { spawnSync } from "node:child_process"
import { existsSync, mkdirSync, mkdtempSync, readdirSync, rmSync, writeFileSync, appendFileSync, readFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { openChdb, query, closeChdb, writeBackupsConfig, bootstrapSchema } from "./harness"

const DATA = mkdtempSync(join(tmpdir(), "task6-catalog-"))
const CFG = writeBackupsConfig(DATA)
const ARCH = join(DATA, "archive")
const DUCK = "/tmp/duckdb"
const run = (cmd: string[], opts: any = {}) => spawnSync(cmd[0], cmd.slice(1), { encoding: "utf8", timeout: 60000, ...opts })
const duck = (sql: string) => run([DUCK, "-csv", "-noheader", "-c", sql], { stdio: ["ignore","pipe","pipe"] }).stdout.trim()

console.log(`=== Task 6: durability and catalog options ===`)
console.log(`store: ${DATA}\n`)

// Build a store
console.log(`building store...`)
{
	const c = openChdb(DATA, CFG); bootstrapSchema(c.conn)
	const v: string[] = []
	for (let i = 0; i < 10000; i++) v.push(`('local','2026-06-25 10:00:00.000','${i.toString(16).padStart(16,"0")}','${i.toString(16).padStart(16,"0")}','svc','Ok','root','Server')`)
	query(c.conn, `INSERT INTO traces (OrgId, Timestamp, TraceId, SpanId, ServiceName, StatusCode, SpanName, SpanKind) VALUES ${v.join(",")}`)
	closeChdb(c)
}

// ── Crash-state simulation: interrupt at each phase ──
console.log(`\n── crash-state simulation ──`)
const BUILDING = join(ARCH, "chunks", "building"); mkdirSync(BUILDING, { recursive: true })
const CURRENT = join(ARCH, "chunks", "current"); mkdirSync(CURRENT, { recursive: true })
const CATALOG = join(ARCH, "catalog.jsonl")

const phases = [
	{ name: "Phase 1: crash during file write (partial .parquet)",
		setup: () => { writeFileSync(join(BUILDING, "traces.parquet"), "PAR1\0\0\0") /* truncated */ },
		expect: "building/ has a partial file, no manifest. Reconciler: delete building/ + re-export." },
	{ name: "Phase 2: crash after files but before manifest",
		setup: () => { /* leave valid parquet but no manifest.json */
			const c = openChdb(DATA, CFG)
			query(c.conn, `SELECT * FROM traces INTO OUTFILE '${join(BUILDING,"traces.parquet")}' FORMAT Parquet SETTINGS max_threads=1, output_format_parquet_row_group_size=10000`)
			closeChdb(c)
		},
		expect: "building/ has valid parquet, no manifest. Reconciler: validate parquet, generate manifest, promote — OR delete + re-export." },
	{ name: "Phase 3: crash after manifest but before promotion",
		setup: () => { writeFileSync(join(BUILDING, "manifest.json"), JSON.stringify({ id: "chunk-001", rows: 10000 })) },
		expect: "building/ has manifest, not promoted. Reconciler: validate checksums, promote building/→current/." },
	{ name: "Phase 4: crash after promotion but before catalog append",
		setup: () => {
			// simulate: building contents already promoted to current, but catalog.jsonl has no entry
			mkdirSync(join(CURRENT, "chunk-001"), { recursive: true })
			writeFileSync(join(CURRENT, "chunk-001", "manifest.json"), JSON.stringify({ id: "chunk-001", rows: 10000, status: "promoted" }))
		},
		expect: "current/ has a chunk with no catalog entry. Reconciler: scan current/, append missing catalog line." },
]

for (const phase of phases) {
	// clean slate
	rmSync(join(BUILDING, "."), { recursive: true, force: true }); mkdirSync(BUILDING, { recursive: true })
	rmSync(join(CURRENT, "."), { recursive: true, force: true }); mkdirSync(CURRENT, { recursive: true })
	rmSync(CATALOG, { force: true })
	console.log(`\n  ${phase.name}`)
	phase.setup()
	// What state exists?
	const bFiles = readdirSync(BUILDING)
	const cFiles = existsSync(CATALOG) ? readFileSync(CATALOG, "utf8").trim().split("\n") : []
	console.log(`    building/: ${bFiles.join(", ") || "(empty)"}`)
	console.log(`    current/: ${readdirSync(CURRENT).join(", ") || "(empty)"}`)
	console.log(`    catalog: ${cFiles.length} entries`)
	console.log(`    → ${phase.expect}`)
}

// ── Catalog option comparison ──
console.log(`\n── catalog option comparison ──`)
console.log(`
  Option 1: manifests as source of truth (rebuildable catalog)
    - Each chunk's manifest.json IS the record. catalog.jsonl is a convenience index.
    - Rebuild: scan current/*/manifest.json → regenerate catalog.jsonl.
    - Pro: simplest, always reconstructible, no separate state to keep consistent.
    - Con: O(chunks) scan to list; no indexes for time-range queries without reading manifests.

  Option 2: append-only catalog.jsonl (primary)
    - One line per chunk: {id, range, signals, rows, checksums, path, status}.
    - Append on promotion; never rewrite. Supersession via new lines with supersede=id.
    - Pro: simple, human-readable, atomic appends (single write() call).
    - Con: grows unboundedly; listing requires full scan; superseded entries linger.
    - Crash safety: a truncated last line is skipped (incomplete write). Reconciler appends.

  Option 3: small transactional catalog (SQLite/libSQL)
    - Indexed by time range, service, signal. Transactional promotion (chunk + catalog row atomic).
    - Pro: fast range queries, clean supersession, ACID.
    - Con: binary format (not human-readable without a tool), adds a dependency.
    - Crash safety: SQLite WAL is crash-safe; a transaction either commits or doesn't.

  RECOMMENDED for v1: Option 1 (manifests as truth) + Option 2 (catalog.jsonl as index).
  Both are text, both are rebuildable, and they compose: catalog is derived from manifests.
  Option 3 is a future optimization if catalog scan latency becomes a problem.
`)

// ── Minimum state machine ──
console.log(`── minimum state machine (never advertises an incomplete chunk) ──`)
console.log(`
  State                  | Transition to "advertised" (catalog entry written)
  ───────────────────────┼────────────────────────────────────────────────────
  building/ writing      | NEVER advertised. On crash: delete, re-export.
  building/ + files      | NEVER advertised until manifest + checksums written.
  building/ + manifest   | NEVER advertised until promoted (rename to current/).
  current/ (promoted)    | ELIGIBLE: reconciler appends catalog entry if missing.
  current/ + catalog     | ADVERTISED. Queries may read it.

  Key invariant: a catalog entry is appended ONLY AFTER the chunk is in current/
  AND its checksums are verified. A crash between promotion and catalog-append
  leaves a promotable-but-unadvertised chunk, which the reconciler discovers.
  No partial state is ever visible to queries (they read catalog, not current/).
`)

// ── Derived tables: intentionally separate TTLs ──
console.log(`── derived tables (separate TTL policy) ──`)
console.log(`  Derived MV targets (error_events, trace_list_mv, logs_aggregates_hourly,`)
console.log(`  service_overview_spans) have intentionally SHORTER or different TTLs than`)
console.log(`  raw tables. They are derived from raw → they should NOT be pruned alongside`)
console.log(`  raw. Their archival is out of scope (Task 4). Pruning them is a separate`)
console.log(`  policy decision tied to the UI's working-set needs, not the archive.`)

console.log(`\nstore + archive kept at ${DATA}`)
