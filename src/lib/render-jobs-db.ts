/**
 * HD render queue — one ordered queue of frozen part snapshots that any number
 * of render machines pull from, one job at a time.
 *
 * Dual backend, same as projects: SQLite on self-hosted/LAN, Postgres on the
 * published build.
 */
import path from "node:path";
import { mkdirSync } from "node:fs";
import { randomUUID } from "node:crypto";
import Database from "better-sqlite3";
import { hostProjectsDbPath } from "@/lib/host-storage";
import { usePostgres } from "@/lib/runtime-backends";
import { pgQuery } from "@/lib/pg";

export type RenderJobStatus =
  | "queued"
  | "rendering"
  | "done"
  | "failed"
  | "cancelled";

/** No heartbeat for this long → the job goes back into the queue. */
export const HEARTBEAT_TIMEOUT_MS = 3 * 60 * 1000;

export interface RenderJobRow {
  id: string;
  course_id: string | null;
  project_id: string;
  part_id: string;
  episode_title: string;
  part_title: string;
  requested_by_user_id: string;
  requested_by_email: string;
  duration_ms: number;
  scene_count: number;
  status: RenderJobStatus;
  progress: number;
  stage: string;
  machine: string | null;
  created_at: string;
  claimed_at: string | null;
  heartbeat_at: string | null;
  finished_at: string | null;
  output_url: string | null;
  error: string | null;
  payload: unknown;
}

export interface RenderJobItem {
  id: string;
  courseId: string | null;
  projectId: string;
  partId: string;
  episodeTitle: string;
  partTitle: string;
  requestedByUserId: string;
  requestedByEmail: string;
  durationMs: number;
  sceneCount: number;
  status: RenderJobStatus;
  progress: number;
  stage: string;
  machine: string | null;
  createdAt: string;
  claimedAt: string | null;
  heartbeatAt: string | null;
  finishedAt: string | null;
  outputUrl: string | null;
  error: string | null;
}

export function toJobItem(row: RenderJobRow): RenderJobItem {
  return {
    id: row.id,
    courseId: row.course_id,
    projectId: row.project_id,
    partId: row.part_id,
    episodeTitle: row.episode_title,
    partTitle: row.part_title,
    requestedByUserId: row.requested_by_user_id,
    requestedByEmail: row.requested_by_email,
    durationMs: row.duration_ms,
    sceneCount: row.scene_count,
    status: row.status,
    progress: row.progress,
    stage: row.stage,
    machine: row.machine,
    createdAt: row.created_at,
    claimedAt: row.claimed_at,
    heartbeatAt: row.heartbeat_at,
    finishedAt: row.finished_at,
    outputUrl: row.output_url,
    error: row.error,
  };
}

export interface CreateRenderJobInput {
  courseId: string | null;
  projectId: string;
  partId: string;
  episodeTitle: string;
  partTitle: string;
  requestedByUserId: string;
  requestedByEmail: string;
  durationMs: number;
  sceneCount: number;
  payload: unknown;
}

/* ------------------------------ SQLite ------------------------------ */

let db: Database.Database | null = null;

function getDb(): Database.Database {
  if (db) return db;
  mkdirSync(path.dirname(hostProjectsDbPath()), { recursive: true });
  db = new Database(hostProjectsDbPath());
  db.pragma("journal_mode = WAL");
  db.exec(`
    CREATE TABLE IF NOT EXISTS render_jobs (
      id TEXT PRIMARY KEY,
      course_id TEXT,
      project_id TEXT NOT NULL,
      part_id TEXT NOT NULL,
      episode_title TEXT NOT NULL DEFAULT '',
      part_title TEXT NOT NULL DEFAULT '',
      requested_by_user_id TEXT NOT NULL DEFAULT '',
      requested_by_email TEXT NOT NULL DEFAULT '',
      duration_ms INTEGER NOT NULL DEFAULT 0,
      scene_count INTEGER NOT NULL DEFAULT 0,
      status TEXT NOT NULL DEFAULT 'queued',
      progress REAL NOT NULL DEFAULT 0,
      stage TEXT NOT NULL DEFAULT '',
      machine TEXT,
      created_at TEXT NOT NULL,
      claimed_at TEXT,
      heartbeat_at TEXT,
      finished_at TEXT,
      output_url TEXT,
      error TEXT,
      payload TEXT NOT NULL DEFAULT '{}'
    );
    CREATE INDEX IF NOT EXISTS render_jobs_status_idx
      ON render_jobs (status, created_at);
  `);
  return db;
}

function sqliteRow(row: Record<string, unknown>): RenderJobRow {
  let payload: unknown = {};
  try {
    payload = JSON.parse(String(row.payload ?? "{}"));
  } catch {
    payload = {};
  }
  const str = (v: unknown): string => (v == null ? "" : String(v));
  const orNull = (v: unknown): string | null => (v == null ? null : String(v));
  return {
    id: str(row.id),
    course_id: orNull(row.course_id),
    project_id: str(row.project_id),
    part_id: str(row.part_id),
    episode_title: str(row.episode_title),
    part_title: str(row.part_title),
    requested_by_user_id: str(row.requested_by_user_id),
    requested_by_email: str(row.requested_by_email),
    duration_ms: Number(row.duration_ms ?? 0),
    scene_count: Number(row.scene_count ?? 0),
    status: (str(row.status) || "queued") as RenderJobStatus,
    progress: Number(row.progress ?? 0),
    stage: str(row.stage),
    machine: orNull(row.machine),
    created_at: str(row.created_at),
    claimed_at: orNull(row.claimed_at),
    heartbeat_at: orNull(row.heartbeat_at),
    finished_at: orNull(row.finished_at),
    output_url: orNull(row.output_url),
    error: orNull(row.error),
    payload,
  };
}

/* ----------------------------- Postgres ----------------------------- */

const PG_DDL = `
CREATE TABLE IF NOT EXISTS render_jobs (
  id TEXT PRIMARY KEY,
  course_id TEXT,
  project_id TEXT NOT NULL,
  part_id TEXT NOT NULL,
  episode_title TEXT NOT NULL DEFAULT '',
  part_title TEXT NOT NULL DEFAULT '',
  requested_by_user_id TEXT NOT NULL DEFAULT '',
  requested_by_email TEXT NOT NULL DEFAULT '',
  duration_ms BIGINT NOT NULL DEFAULT 0,
  scene_count INTEGER NOT NULL DEFAULT 0,
  status TEXT NOT NULL DEFAULT 'queued',
  progress DOUBLE PRECISION NOT NULL DEFAULT 0,
  stage TEXT NOT NULL DEFAULT '',
  machine TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  claimed_at TIMESTAMPTZ,
  heartbeat_at TIMESTAMPTZ,
  finished_at TIMESTAMPTZ,
  output_url TEXT,
  error TEXT,
  payload JSONB NOT NULL DEFAULT '{}'::jsonb
);
CREATE INDEX IF NOT EXISTS render_jobs_status_idx ON render_jobs (status, created_at);
CREATE INDEX IF NOT EXISTS render_jobs_queue_idx ON render_jobs (created_at) WHERE status = 'queued';
`;

let pgReady: Promise<void> | null = null;

async function ensurePgTable(): Promise<void> {
  if (!pgReady) {
    pgReady = (async () => {
      for (const stmt of PG_DDL.split(";").map((s) => s.trim()).filter(Boolean)) {
        await pgQuery(stmt);
      }
      // Carry-over of parts frozen with the old "Ready for HD" flow. It must run
      // exactly once for the whole install — otherwise every fresh server
      // process resurrects jobs the user deleted.
      await pgQuery(
        `CREATE TABLE IF NOT EXISTS render_jobs_migrations (key TEXT PRIMARY KEY, done_at TIMESTAMPTZ NOT NULL DEFAULT now())`,
      );
      const flag = await pgQuery(
        `INSERT INTO render_jobs_migrations (key) VALUES ('bundles_carryover')
         ON CONFLICT (key) DO NOTHING RETURNING key`,
      );
      // Only an install that never had a queue needs the carry-over; running it
      // on an existing queue would bring deleted jobs back.
      const existing = await pgQuery(`SELECT 1 FROM render_jobs LIMIT 1`).catch(
        () => ({ rows: [1] }) as { rows: unknown[] },
      );
      if (flag.rows.length > 0 && existing.rows.length === 0) {
        await pgQuery(
          `INSERT INTO render_jobs
             (id, project_id, part_id, episode_title, part_title,
              requested_by_user_id, requested_by_email, duration_ms, scene_count,
              status, created_at, output_url, payload)
           SELECT b.id, b.project_id, b.part_id, b.episode_title, b.part_title,
                  b.owner_user_id, b.owner_email, b.duration_ms, b.scene_count,
                  CASE WHEN b.status = 'done' THEN 'done'
                       WHEN b.status = 'failed' THEN 'failed'
                       ELSE 'queued' END,
                  b.ready_at, b.output_url, b.payload
           FROM render_bundles b
           WHERE NOT EXISTS (SELECT 1 FROM render_jobs j WHERE j.id = b.id)`,
        ).catch(() => undefined);
      }
      // Older jobs were stored without a course, so the course filter hid them.
      await pgQuery(
        `UPDATE render_jobs j SET course_id = p.course_id
           FROM projects p
          WHERE j.project_id = p.id
            AND j.course_id IS NULL
            AND p.course_id IS NOT NULL`,
      ).catch(() => undefined);
    })().catch((err) => {
      pgReady = null;
      throw err;
    });
  }
  return pgReady;
}

const PG_SELECT = `
  id, course_id, project_id, part_id, episode_title, part_title,
  requested_by_user_id, requested_by_email, duration_ms, scene_count, status,
  progress, stage, machine, created_at::text AS created_at,
  claimed_at::text AS claimed_at, heartbeat_at::text AS heartbeat_at,
  finished_at::text AS finished_at, output_url, error, payload
`;

/** Same columns without the frozen payload (can be megabytes per job); list views never read it. */
const PG_SELECT_LIST = `
  id, course_id, project_id, part_id, episode_title, part_title,
  requested_by_user_id, requested_by_email, duration_ms, scene_count, status,
  progress, stage, machine, created_at::text AS created_at,
  claimed_at::text AS claimed_at, heartbeat_at::text AS heartbeat_at,
  finished_at::text AS finished_at, output_url, error, '{}'::jsonb AS payload
`;

const SQLITE_SELECT_LIST = `
  id, course_id, project_id, part_id, episode_title, part_title,
  requested_by_user_id, requested_by_email, duration_ms, scene_count, status,
  progress, stage, machine, created_at, claimed_at, heartbeat_at, finished_at,
  output_url, error, '{}' AS payload
`;

function pgRow(row: Record<string, unknown>): RenderJobRow {
  const raw = row.payload;
  let payload: unknown = raw ?? {};
  if (typeof raw === "string") {
    try {
      payload = JSON.parse(raw);
    } catch {
      payload = {};
    }
  }
  return { ...sqliteRow({ ...row, payload: "{}" }), payload };
}

/* ------------------------------- API -------------------------------- */

export async function createRenderJob(
  input: CreateRenderJobInput,
): Promise<RenderJobRow> {
  const id = randomUUID();
  const now = new Date().toISOString();
  const payloadJson = JSON.stringify(input.payload ?? {});

  if (usePostgres()) {
    await ensurePgTable();
    // Re-queuing a part supersedes an earlier job that has not started.
    await pgQuery(`DELETE FROM render_jobs WHERE part_id = $1 AND status = 'queued'`, [
      input.partId,
    ]);
    await pgQuery(
      `INSERT INTO render_jobs
        (id, course_id, project_id, part_id, episode_title, part_title,
         requested_by_user_id, requested_by_email, duration_ms, scene_count,
         status, created_at, payload)
       VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,'queued',$11,$12::jsonb)`,
      [
        id,
        input.courseId,
        input.projectId,
        input.partId,
        input.episodeTitle,
        input.partTitle,
        input.requestedByUserId,
        input.requestedByEmail,
        Math.round(input.durationMs),
        input.sceneCount,
        now,
        payloadJson,
      ],
    );
  } else {
    const d = getDb();
    d.prepare(`DELETE FROM render_jobs WHERE part_id = ? AND status = 'queued'`).run(
      input.partId,
    );
    d.prepare(
      `INSERT INTO render_jobs
        (id, course_id, project_id, part_id, episode_title, part_title,
         requested_by_user_id, requested_by_email, duration_ms, scene_count,
         status, created_at, payload)
       VALUES (?,?,?,?,?,?,?,?,?,?,'queued',?,?)`,
    ).run(
      id,
      input.courseId,
      input.projectId,
      input.partId,
      input.episodeTitle,
      input.partTitle,
      input.requestedByUserId,
      input.requestedByEmail,
      Math.round(input.durationMs),
      input.sceneCount,
      now,
      payloadJson,
    );
  }

  const created = await getRenderJob(id);
  if (!created) throw new Error("Render job insert failed");
  return created;
}

export async function getRenderJob(id: string): Promise<RenderJobRow | null> {
  if (usePostgres()) {
    await ensurePgTable();
    const res = await pgQuery(`SELECT ${PG_SELECT} FROM render_jobs WHERE id = $1`, [id]);
    const row = res.rows[0];
    return row ? pgRow(row as Record<string, unknown>) : null;
  }
  const row = getDb().prepare(`SELECT * FROM render_jobs WHERE id = ?`).get(id) as
    | Record<string, unknown>
    | undefined;
  return row ? sqliteRow(row) : null;
}

/** The sweep is an UPDATE; the Export page polls every few seconds from every open tab. */
const STALE_SWEEP_MIN_INTERVAL_MS = 10_000;
let lastStaleSweepAt = 0;

/** Put timed-out renders back into the queue so another machine can take them. */
export async function requeueStaleJobs(opts?: { force?: boolean }): Promise<void> {
  // Timeouts are minutes long, so sweeping more than every few seconds finds nothing new.
  const now = Date.now();
  if (!opts?.force && now - lastStaleSweepAt < STALE_SWEEP_MIN_INTERVAL_MS) return;
  lastStaleSweepAt = now;
  const cutoff = new Date(Date.now() - HEARTBEAT_TIMEOUT_MS).toISOString();
  if (usePostgres()) {
    await ensurePgTable();
    await pgQuery(
      `UPDATE render_jobs
         SET status = 'queued', machine = NULL, progress = 0, stage = '',
             claimed_at = NULL, heartbeat_at = NULL
       WHERE status = 'rendering'
         AND COALESCE(heartbeat_at, claimed_at) < $1`,
      [cutoff],
    );
    return;
  }
  getDb()
    .prepare(
      `UPDATE render_jobs
         SET status = 'queued', machine = NULL, progress = 0, stage = '',
             claimed_at = NULL, heartbeat_at = NULL
       WHERE status = 'rendering'
         AND COALESCE(heartbeat_at, claimed_at) < ?`,
    )
    .run(cutoff);
}

export async function listRenderJobs(opts?: {
  status?: RenderJobStatus | "all" | "active";
  courseId?: string;
  limit?: number;
}): Promise<RenderJobRow[]> {
  await requeueStaleJobs();
  const status = opts?.status ?? "all";
  const limit = Math.min(Math.max(opts?.limit ?? 500, 1), 1000);

  const where: string[] = [];
  const params: unknown[] = [];
  const ph = () => (usePostgres() ? `$${params.length}` : "?");

  if (status === "active") {
    where.push(`status IN ('queued','rendering')`);
  } else if (status !== "all") {
    params.push(status);
    where.push(`status = ${ph()}`);
  }
  if (opts?.courseId) {
    // Older jobs never stored a course, so fall back to the episode's course.
    params.push(opts.courseId);
    const a = ph();
    params.push(opts.courseId);
    const b = ph();
    where.push(
      `(course_id = ${a} OR (course_id IS NULL AND project_id IN (SELECT id FROM projects WHERE course_id = ${b})))`,
    );
  }
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

  if (usePostgres()) {
    await ensurePgTable();
    const res = await pgQuery(
      `SELECT ${PG_SELECT_LIST} FROM render_jobs ${whereSql}
       ORDER BY created_at DESC LIMIT ${limit}`,
      params,
    );
    return res.rows.map((r) => pgRow(r as Record<string, unknown>));
  }
  const rows = getDb()
    .prepare(
      `SELECT ${SQLITE_SELECT_LIST} FROM render_jobs ${whereSql} ORDER BY created_at DESC LIMIT ${limit}`,
    )
    .all(...params) as Record<string, unknown>[];
  return rows.map(sqliteRow);
}

/** Atomically take the oldest waiting job. Safe with many machines polling. */
export async function claimNextRenderJob(machine: string): Promise<RenderJobRow | null> {
  await requeueStaleJobs();
  const now = new Date().toISOString();

  if (usePostgres()) {
    await ensurePgTable();
    const res = await pgQuery(
      `UPDATE render_jobs SET status = 'rendering', machine = $1,
              claimed_at = $2, heartbeat_at = $2, progress = 0, stage = 'claimed',
              error = NULL
       WHERE id = (
         SELECT id FROM render_jobs WHERE status = 'queued'
         ORDER BY created_at ASC LIMIT 1 FOR UPDATE SKIP LOCKED
       )
       AND NOT EXISTS (
         SELECT 1 FROM render_jobs WHERE status = 'rendering' AND machine = $1
       )
       RETURNING ${PG_SELECT}`,
      [machine, now],
    );
    const row = res.rows[0];
    return row ? pgRow(row as Record<string, unknown>) : null;
  }

  const d = getDb();
  const busy = d
    .prepare(`SELECT 1 FROM render_jobs WHERE status = 'rendering' AND machine = ? LIMIT 1`)
    .get(machine);
  if (busy) return null;
  const next = d
    .prepare(`SELECT id FROM render_jobs WHERE status = 'queued' ORDER BY created_at ASC LIMIT 1`)
    .get() as { id?: string } | undefined;
  if (!next?.id) return null;
  const res = d
    .prepare(
      `UPDATE render_jobs SET status = 'rendering', machine = ?, claimed_at = ?,
              heartbeat_at = ?, progress = 0, stage = 'claimed', error = NULL
       WHERE id = ? AND status = 'queued'`,
    )
    .run(machine, now, now, next.id);
  if (res.changes === 0) return null;
  return getRenderJob(next.id);
}

export async function heartbeatRenderJob(
  id: string,
  patch: { progress?: number; stage?: string; machine?: string },
): Promise<RenderJobRow | null> {
  const current = await getRenderJob(id);
  if (!current) return null;
  if (current.status === "cancelled") return current;
  // A finished render stays finished — late heartbeats must not reopen it.
  if (current.status === "done") return current;
  // Same for a failed one: a progress report still in flight when the machine
  // reported the failure used to flip it back to "rendering" with nobody on it.
  if (current.status === "failed") return current;
  // If another machine has taken this job over, a late heartbeat from the first
  // one must not rename it or overwrite the new machine's progress.
  if (
    current.status === "rendering" &&
    current.machine &&
    patch.machine &&
    current.machine !== patch.machine
  ) {
    return current;
  }
  const now = new Date().toISOString();
  const progress = Math.max(0, Math.min(1, patch.progress ?? current.progress));
  const stage = patch.stage ?? current.stage;
  const machine = patch.machine ?? current.machine;

  if (usePostgres()) {
    await pgQuery(
      `UPDATE render_jobs SET status = 'rendering', progress = $2, stage = $3,
              machine = $4, heartbeat_at = $5 WHERE id = $1`,
      [id, progress, stage, machine, now],
    );
  } else {
    getDb()
      .prepare(
        `UPDATE render_jobs SET status = 'rendering', progress = ?, stage = ?,
                machine = ?, heartbeat_at = ? WHERE id = ?`,
      )
      .run(progress, stage, machine, now, id);
  }
  return getRenderJob(id);
}

export async function updateRenderJob(
  id: string,
  patch: {
    status?: RenderJobStatus;
    progress?: number;
    stage?: string;
    outputUrl?: string | null;
    error?: string | null;
    machine?: string | null;
    finished?: boolean;
  },
): Promise<RenderJobRow | null> {
  const current = await getRenderJob(id);
  if (!current) return null;
  // The uploaded video wins: a late "failed" report from the render machine
  // must not overwrite a job that already delivered its MP4.
  if (current.status === "done" && current.output_url && patch.status === "failed") {
    return current;
  }
  const status = patch.status ?? current.status;
  const progress = patch.progress ?? current.progress;
  const stage = patch.stage ?? current.stage;
  const outputUrl = patch.outputUrl === undefined ? current.output_url : patch.outputUrl;
  const error = patch.error === undefined ? current.error : patch.error;
  const machine = patch.machine === undefined ? current.machine : patch.machine;
  const finishedAt =
    patch.finished || status === "done" || status === "failed" || status === "cancelled"
      ? new Date().toISOString()
      : current.finished_at;

  if (usePostgres()) {
    await pgQuery(
      `UPDATE render_jobs SET status = $2, progress = $3, stage = $4,
              output_url = $5, error = $6, machine = $7, finished_at = $8
       WHERE id = $1`,
      [id, status, progress, stage, outputUrl, error, machine, finishedAt],
    );
  } else {
    getDb()
      .prepare(
        `UPDATE render_jobs SET status = ?, progress = ?, stage = ?, output_url = ?,
                error = ?, machine = ?, finished_at = ? WHERE id = ?`,
      )
      .run(status, progress, stage, outputUrl, error, machine, finishedAt, id);
  }
  return getRenderJob(id);
}

export async function deleteRenderJob(id: string): Promise<void> {
  if (usePostgres()) {
    await ensurePgTable();
    await pgQuery(`DELETE FROM render_jobs WHERE id = $1`, [id]);
    // Legacy "Ready for HD" rows would otherwise be carried over again.
    await pgQuery(`DELETE FROM render_bundles WHERE id = $1`, [id]).catch(
      () => undefined,
    );
    return;
  }
  getDb().prepare(`DELETE FROM render_jobs WHERE id = ?`).run(id);
}
