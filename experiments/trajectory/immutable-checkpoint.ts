/**
 * Immutable-checkpoint source prototype (Trajectory C).
 *
 * Implements the proposed layout from the research task:
 *   backups/
 *     snapshots/
 *       <checkpoint-id>/
 *         backup/          ← native BACKUP TO Disk output
 *         manifest.json    ← provenance + validation
 *         pins/            ← one file per active restore (anti-GC)
 *     current.json         ← {"checkpointId": "<id>"} atomic pointer
 *
 * This is a PROTOTYPE — disposable, not production code. It reuses the existing
 * BACKUP/RESTORE primitives (proven in prior research) but layers the immutable
 * snapshot-per-id + pin + atomic-pointer design on top.
 *
 * The existing checkpoint branch uses building/current/previous (mutable, overwritten).
 * Trajectory C replaces that with immutable, addressable-by-id snapshots. This module
 * proves the mechanism; the production wiring would live in checkpoints.ts.
 */
import { existsSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync, readdirSync, renameSync } from "node:fs"
import { cp, readFile, rename, rm, stat, writeFile } from "node:fs/promises"
import { join, resolve, sep } from "node:path"
import { randomUUID } from "node:crypto"
import { openChdb, query, closeChdb, bootstrapSchema } from "../archives/harness"

/** Write a backups config. If sourceDataDir is given, declares a 'src' disk pointing at it
 *  (so a scratch store can RESTORE a backup that lives under sourceDataDir in place). */
export function writeBackupConfig(path: string, sourceDataDir?: string): void {
	const dataDirWithSlash = (d: string) => { const a = resolve(d); return a.endsWith(sep) ? a : `${a}${sep}` }
	const sourceDisk = sourceDataDir
		? `\n  <storage_configuration>\n    <disks>\n      <src>\n        <path>${dataDirWithSlash(sourceDataDir)}</path>\n      </src>\n    </disks>\n  </storage_configuration>`
		: ""
	writeFileSync(path,
		`<clickhouse>\n  <backups>\n    <allowed_disk>${sourceDataDir ? "src" : "default"}</allowed_disk>\n    <allowed_path>backups</allowed_path>\n  </backups>${sourceDisk}\n</clickhouse>\n`)
}

export interface SnapshotManifest {
	readonly checkpointId: string
	readonly mapleVersion: string
	readonly chdbVersion: string
	readonly schemaFingerprint: string
	readonly createdAt: string
	readonly sourceDataDir: string
	readonly backupBytes: number
	readonly validation: {
		readonly validatedAt: string
		readonly traces: number
		readonly logs: number
		readonly materializedViews: number
	}
}

export const snapshotsRoot = (dataDir: string) => join(dataDir, "backups", "snapshots")
export const currentJsonPath = (dataDir: string) => join(dataDir, "backups", "current.json")
export const snapshotDir = (dataDir: string, id: string) => join(snapshotsRoot(dataDir), id)
export const pinsDir = (dataDir: string, id: string) => join(snapshotDir(dataDir, id), "pins")

/** Read the current pointer. Returns null if unset. */
export async function readCurrent(dataDir: string): Promise<string | null> {
	try {
		const raw = await readFile(currentJsonPath(dataDir), "utf8")
		const parsed = JSON.parse(raw) as { checkpointId: string }
		return parsed.checkpointId ?? null
	} catch { return null }
}

/** Atomically write the current pointer (write-temp + rename). */
export async function writeCurrent(dataDir: string, checkpointId: string): Promise<void> {
	const tmp = currentJsonPath(dataDir) + ".tmp"
	await writeFile(tmp, JSON.stringify({ checkpointId }, null, 2) + "\n")
	await rename(tmp, currentJsonPath(dataDir))
}

/** Validate a backup by RESTORE-ing into a scratch store and smoke-querying. */
export async function validateSnapshot(
	sourceDataDir: string,
	checkpointId: string,
	scratchParent: string,
): Promise<SnapshotManifest["validation"]> {
	const scratchData = join(scratchParent, "data")
	const scratchConfig = join(scratchParent, "config.xml")
	// The scratch needs a 'src' disk pointing at sourceDataDir to read the backup in place.
	writeBackupConfig(scratchConfig, sourceDataDir)
	const c = openChdb(scratchData, scratchConfig)
	try {
		query(c.conn, "CREATE DATABASE IF NOT EXISTS default")
		query(c.conn, `RESTORE DATABASE default FROM Disk('src', 'backups/snapshots/${checkpointId}/backup') SETTINGS allow_different_database_def=1`)
		return {
			validatedAt: new Date().toISOString(),
			traces: Number(query(c.conn, "SELECT count() FROM traces")),
			logs: Number(query(c.conn, "SELECT count() FROM logs")),
			materializedViews: Number(query(c.conn, "SELECT count() FROM system.tables WHERE database='default' AND engine='MaterializedView'")),
		}
	} finally {
		closeChdb(c)
		rmSync(scratchParent, { recursive: true, force: true })
	}
}

/**
 * Create + validate + promote an immutable snapshot under a unique ID.
 * Returns the manifest. The previous snapshot remains addressable.
 *
 * crashPoint (optional, for fault injection): one of
 *   "after-backup" | "after-validate" | "after-manifest" | "after-pointer" | null
 */
export async function createSnapshot(
	dataDir: string,
	cfg: string,
	crashPoint: string | null = null,
): Promise<SnapshotManifest> {
	const id = new Date().toISOString().replace(/[:.]/g, "-") + "-" + randomUUID().slice(0, 8)
	const snapDir = snapshotDir(dataDir, id)
	const backupDir = join(snapDir, "backup")
	mkdirSync(backupDir, { recursive: true })

	// 1. Native BACKUP into the snapshot's backup/ dir.
	//    Open the live store, BACKUP, close.
	const c = openChdb(dataDir, cfg)
	try {
		query(c.conn, `BACKUP DATABASE default TO Disk('default', 'backups/snapshots/${id}/backup')`)
	} finally {
		closeChdb(c)
	}
	if (crashPoint === "after-backup") throw new Error("INJECTED: after-backup")

	// 2. Validate by RESTORE into scratch.
	const scratchParent = mkdtempSync(join("/tmp", "traj-validate-"))
	const validation = await validateSnapshot(dataDir, id, scratchParent)
	if (crashPoint === "after-validate") throw new Error("INJECTED: after-validate")

	// 3. Write manifest.
	const manifest: SnapshotManifest = {
		checkpointId: id,
		mapleVersion: "dev",
		chdbVersion: "26.1.0",
		schemaFingerprint: "dev-fp",
		createdAt: new Date().toISOString(),
		sourceDataDir: dataDir,
		backupBytes: await dirSize(backupDir),
		validation,
	}
	await writeFile(join(snapDir, "manifest.json"), JSON.stringify(manifest, null, 2) + "\n")
	if (crashPoint === "after-manifest") throw new Error("INJECTED: after-manifest")

	// 4. Atomically promote: point current.json at this snapshot.
	await writeCurrent(dataDir, id)
	if (crashPoint === "after-pointer") throw new Error("INJECTED: after-pointer")

	return manifest
}

/** Pin a checkpoint against GC (one file per active restore). Returns the pin id. */
export async function pinSnapshot(dataDir: string, checkpointId: string): Promise<string> {
	const pDir = pinsDir(dataDir, checkpointId)
	mkdirSync(pDir, { recursive: true })
	const pinId = randomUUID()
	await writeFile(join(pDir, pinId), new Date().toISOString())
	return pinId
}

/** Release a pin. If no pins remain, the snapshot is GC-eligible. */
export async function releasePin(dataDir: string, checkpointId: string, pinId: string): Promise<void> {
	try { await rm(join(pinsDir(dataDir, checkpointId), pinId)) } catch {}
}

/** Is this snapshot pinned (has any pin files)? */
export function isPinned(dataDir: string, checkpointId: string): boolean {
	const pDir = pinsDir(dataDir, checkpointId)
	if (!existsSync(pDir)) return false
	return readdirSync(pDir).length > 0
}

/** GC: remove snapshots that are neither current nor pinned. Returns removed IDs. */
export async function gcSnapshots(dataDir: string): Promise<string[]> {
	const current = await readCurrent(dataDir)
	const root = snapshotsRoot(dataDir)
	if (!existsSync(root)) return []
	const removed: string[] = []
	for (const id of readdirSync(root)) {
		if (id === current) continue
		if (isPinned(dataDir, id)) continue
		await rm(join(root, id), { recursive: true, force: true })
		removed.push(id)
	}
	return removed
}

/**
 * Restore a checkpoint by ID into an EXTERNAL scratch data dir.
 * Does NOT touch the live store. Returns the scratch dir path.
 * crashPoint (fault injection): "during-restore" | "after-restore" | null
 */
export async function restoreSnapshotToScratch(
	liveDataDir: string,
	checkpointId: string,
	scratchDataDir: string,
	crashPoint: string | null = null,
): Promise<SnapshotManifest["validation"]> {
	mkdirSync(scratchDataDir, { recursive: true })
	const scratchParent = mkdtempSync(join("/tmp", "traj-restore-"))
	const config = join(scratchParent, "config.xml")
	writeBackupConfig(config, liveDataDir) // 'src' disk → liveDataDir, where the backup lives
	const c = openChdb(scratchDataDir, config)
	try {
		query(c.conn, "CREATE DATABASE IF NOT EXISTS default")
		query(c.conn, `RESTORE DATABASE default FROM Disk('src', 'backups/snapshots/${checkpointId}/backup') SETTINGS allow_different_database_def=1`)
		const validation = {
			validatedAt: new Date().toISOString(),
			traces: Number(query(c.conn, "SELECT count() FROM traces")),
			logs: Number(query(c.conn, "SELECT count() FROM logs")),
			materializedViews: Number(query(c.conn, "SELECT count() FROM system.tables WHERE database='default' AND engine='MaterializedView'")),
		}
		if (crashPoint === "during-restore") throw new Error("INJECTED: during-restore")
		return validation
	} finally {
		closeChdb(c)
		rmSync(scratchParent, { recursive: true, force: true })
	}
}

const dirSize = async (path: string): Promise<number> => {
	let total = 0
	const entries = readdirSync(path)
	for (const entry of entries) {
		const child = join(path, entry)
		const s = await stat(child)
		if (s.isDirectory()) total += await dirSize(child)
		else if (s.isFile()) total += s.size
	}
	return total
}
