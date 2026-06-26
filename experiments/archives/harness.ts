/**
 * Shared chDB experiment harness for archive research.
 *
 * Provides:
 *   - openChdb(dataDir, cfg?): open a fresh chDB connection (Maple's exact startup args)
 *   - query(conn, sql, format?): run one statement, return string result
 *   - bootstrapSchema(conn): apply local-schema.sql per-statement
 *   - withStore<T>(fn): create a temp store, bootstrap, run fn, clean up
 *
 * This is a library module (no auto-run). Experiment scripts import it.
 */
import { CString, dlopen, FFIType, ptr, read, toArrayBuffer } from "bun:ffi"
import { existsSync, mkdtempSync, writeFileSync, rmSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"
import schemaSql from "../../apps/cli/src/server/schema/local-schema.sql" with { type: "text" }

export const LIBCHDB = process.env.MAPLE_LIBCHDB ?? "/tmp/libchdb/libchdb.so"

if (!existsSync(LIBCHDB)) {
	throw new Error(`libchdb not found at ${LIBCHDB}. Download v26.1.0: ` +
		`curl -fsSL https://github.com/chdb-io/chdb-core/releases/download/v26.1.0/macos-arm64-libchdb.tar.gz | tar -xz -C /tmp && mkdir -p /tmp/libchdb && mv /tmp/libchdb.so /tmp/libchdb/`)
}

const lib = dlopen(LIBCHDB, {
	chdb_connect: { args: [FFIType.int, FFIType.ptr], returns: FFIType.ptr },
	chdb_close_conn: { args: [FFIType.ptr], returns: FFIType.void },
	chdb_query: { args: [FFIType.ptr, FFIType.ptr, FFIType.ptr], returns: FFIType.ptr },
	chdb_result_buffer: { args: [FFIType.ptr], returns: FFIType.ptr },
	chdb_result_length: { args: [FFIType.ptr], returns: FFIType.u64 },
	chdb_result_error: { args: [FFIType.ptr], returns: FFIType.ptr },
	chdb_destroy_query_result: { args: [FFIType.ptr], returns: FFIType.void },
})

const enc = new TextEncoder()
const cstr = (s: string) => enc.encode(s + "\0")

export interface ChdbConn { connPtrPtr: number; conn: number }

/** Open chDB with Maple's exact startup args, optionally with a backups config file. */
export function openChdb(dataDir: string, cfgPath?: string): ChdbConn {
	const args = [
		"clickhouse",
		"--async_load_databases=0",
		"--async_load_system_database=0",
		`--path=${dataDir}`,
		...(cfgPath ? [`--config-file=${cfgPath}`] : []),
	]
	const argBufs = args.map(cstr)
	const argv = new BigUint64Array(args.length)
	argBufs.forEach((b, i) => { argv[i] = BigInt(ptr(b)) })
	const connPtrPtr = lib.symbols.chdb_connect(args.length, ptr(argv))
	if (!connPtrPtr) throw new Error(`chdb_connect returned NULL (dataDir=${dataDir})`)
	const conn = read.ptr(connPtrPtr, 0) as number
	if (!conn) throw new Error("chdb_connect produced NULL connection")
	return { connPtrPtr, conn }
}

/** Run one SQL statement. Throws on chDB error (error message in the throw). */
export function query(conn: number, sql: string, format = "TabSeparated"): string {
	const res = lib.symbols.chdb_query(conn, ptr(cstr(sql)), ptr(cstr(format)))
	try {
		const errPtr = lib.symbols.chdb_result_error(res)
		const errMsg = errPtr ? new CString(errPtr).toString() : ""
		if (errMsg) throw new Error(errMsg)
		const len = Number(lib.symbols.chdb_result_length(res))
		if (len === 0) return ""
		const bufPtr = lib.symbols.chdb_result_buffer(res)
		if (!bufPtr) return ""
		return new TextDecoder().decode(toArrayBuffer(bufPtr, 0, len).slice(0))
	} finally {
		lib.symbols.chdb_destroy_query_result(res)
	}
}

/** Close the connection (release chDB). */
export function closeChdb(c: ChdbConn): void {
	if (c.connPtrPtr !== 0) {
		lib.symbols.chdb_close_conn(c.connPtrPtr)
		c.connPtrPtr = 0
	}
}

/** Apply Maple's schema, per-statement (the whole-script apply is unreliable). */
export function bootstrapSchema(conn: number): void {
	const stmts = schemaSql
		.split(/\n\s*\n/)
		.map((s) => s.trim().replace(/;\s*$/, ""))
		.filter((s) => s.length > 0 && !s.startsWith("--"))
	for (const s of stmts) {
		try { query(conn, s) } catch { /* some statements may fail if already applied */ }
	}
}

/** Write the standard backups config to <dataDir>/backups.xml and return its path. */
export function writeBackupsConfig(dataDir: string): string {
	const path = join(dataDir, "backups.xml")
	writeFileSync(path,
		`<clickhouse><backups><allowed_disk>default</allowed_disk><allowed_path>backups</allowed_path></backups></clickhouse>`)
	return path
}

/** Create a temp data dir, bootstrap schema, run fn(conn), close + clean up. */
export async function withStore<T>(
	fn: (conn: number, dataDir: string, cfg: string) => T | Promise<T>,
	opts: { keep?: boolean } = {},
): Promise<{ result: T; dataDir: string }> {
	const dataDir = mkdtempSync(join(tmpdir(), "arch-"))
	const cfg = writeBackupsConfig(dataDir)
	const c = openChdb(dataDir, cfg)
	bootstrapSchema(c.conn)
	try {
		const result = await fn(c.conn, dataDir, cfg)
		return { result, dataDir }
	} finally {
		closeChdb(c)
		if (!opts.keep) rmSync(dataDir, { recursive: true, force: true })
	}
}

/** Insert N synthetic traces (configurable width) via batched VALUES. */
export function insertTraces(conn: number, n: number, opts: { wide?: boolean } = {}): void {
	const wide = opts.wide ?? false
	const BATCH = 10000
	for (let s = 0; s < n; s += BATCH) {
		const vals: string[] = []
		for (let i = s; i < Math.min(s + BATCH, n); i++) {
			const id = i.toString(16).padStart(16, "0")
			// ClickHouse Map syntax: map('key','value',...) — NOT JSON.
			const attrs = wide
				? `,map('http.route','/api/v1/users/${i % 100}/detail','db.statement','SELECT * FROM orders WHERE id = ${i}','peer.service','upstream-${i % 8}')`
				: ""
			vals.push(`('local','2026-06-25 10:00:00.000','${id}','${id}','svc${i % 5}','${i % 5 === 0 ? "Error" : "Ok"}','root','Server'${attrs})`)
		}
		const cols = wide
			? "OrgId, Timestamp, TraceId, SpanId, ServiceName, StatusCode, SpanName, SpanKind, SpanAttributes"
			: "OrgId, Timestamp, TraceId, SpanId, ServiceName, StatusCode, SpanName, SpanKind"
		query(conn, `INSERT INTO traces (${cols}) VALUES ${vals.join(",")}`)
	}
}
