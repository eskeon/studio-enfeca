import { createWriteStream, mkdirSync, rmSync } from "node:fs";
import path from "node:path";
import { randomUUID } from "node:crypto";
import { Readable, Transform } from "node:stream";
import { pipeline } from "node:stream/promises";
import { createFileRoute } from "@tanstack/react-router";
import { hasValidRenderKey, renderKeyError } from "@/lib/render-key";
import { putAssetFromFile } from "@/lib/object-storage";
import { scratchRoot } from "@/lib/runtime-backends";
import {
  claimNextRenderJob,
  getRenderJob,
  heartbeatRenderJob,
  listRenderJobs,
  toJobItem,
  updateRenderJob,
} from "@/lib/render-jobs-db";
import { localGetCourseById } from "@/lib/local-courses-db";
import { normalizeCourseSettings, resolveCourseTemplateTheme } from "@/lib/course-settings";

/**
 * Render-machine API (all render-key protected):
 *   GET  /api/public/render/jobs            → read-only queue view
 *   GET  /api/public/render/:id             → the frozen HD job payload
 *   POST /api/public/render/claim           → atomically take the oldest waiting job
 *   POST /api/public/render/:id/progress    → heartbeat { progress, stage }; may reply { abort: true }
 *   POST /api/public/render/:id/fail        → { error }
 *   PUT  /api/public/render/:id/output      → raw MP4 body; stores and links the render
 */
export const Route = createFileRoute("/api/public/render/$")({
  server: {
    handlers: {
      GET: async ({ params, request }) => {
        if (!hasValidRenderKey(request)) return renderKeyError();
        const { id } = splat(params._splat);
        if (!id || id === "jobs") {
          const status = new URL(request.url).searchParams.get("status") ?? "active";
          const rows = await listRenderJobs({
            status: status as "all",
          });
          return Response.json({ jobs: rows.map(toJobItem) });
        }
        const job = await getRenderJob(id);
        if (!job) return json({ error: "Render job not found" }, 404);
        return Response.json(await jobPayload(job));
      },

      POST: async ({ params, request }) => {
        if (!hasValidRenderKey(request)) return renderKeyError();
        const { id, sub } = splat(params._splat);

        if (id === "claim") {
          let body: { machine?: string } = {};
          try {
            body = (await request.json()) as typeof body;
          } catch {
            body = {};
          }
          const machine = (body.machine ?? "").trim().slice(0, 80) || "unnamed-machine";
          const job = await claimNextRenderJob(machine);
          if (!job) return Response.json({ job: null });
          return Response.json({ job: await jobPayload(job) });
        }

        if (!id) return json({ error: "Not found" }, 404);

        if (sub === "progress") {
          let body: { progress?: number; stage?: string; machine?: string } = {};
          try {
            body = (await request.json()) as typeof body;
          } catch {
            body = {};
          }
          const job = await heartbeatRenderJob(id, {
            progress:
              typeof body.progress === "number"
                ? body.progress > 1
                  ? body.progress / 100
                  : body.progress
                : undefined,
            stage: typeof body.stage === "string" ? body.stage.slice(0, 120) : undefined,
            machine: typeof body.machine === "string" ? body.machine.slice(0, 80) : undefined,
          });
          if (!job) return json({ error: "Render job not found" }, 404);
          return Response.json({
            ok: true,
            status: job.status,
            abort: job.status === "cancelled",
          });
        }

        if (sub === "fail") {
          let body: { error?: string } = {};
          try {
            body = (await request.json()) as typeof body;
          } catch {
            body = {};
          }
          const message = (body.error ?? "Render failed").slice(0, 2000);
          // A machine reporting a failure for a job that was stopped (its upload
          // was refused, say) must not relabel it as failed.
          const existing = await getRenderJob(id);
          if (!existing) return json({ error: "Render job not found" }, 404);
          if (existing.status === "cancelled") return Response.json({ ok: true });
          // "Another job is ahead in the queue" means the machine skipped this
          // one on purpose — it belongs back in the queue, not in failures.
          const skipped = /ahead in the queue/i.test(message);
          const job = await updateRenderJob(
            id,
            skipped
              ? { status: "queued", stage: "queued", progress: 0, machine: null, error: null }
              : { status: "failed", stage: "failed", error: message },
          );
          if (!job) return json({ error: "Render job not found" }, 404);
          return Response.json({ ok: true });
        }

        return json({ error: "Not found" }, 404);
      },

      PUT: async ({ params, request }) => {
        if (!hasValidRenderKey(request)) return renderKeyError();
        const { id, sub } = splat(params._splat);
        if (!id || sub !== "output") return json({ error: "Not found" }, 404);
        const job = await getRenderJob(id);
        if (!job) return json({ error: "Render job not found" }, 404);
        // A stopped render must not come back as "done" because its upload
        // arrived late, and there is no point receiving the body.
        if (job.status === "cancelled") return json({ error: "This render was stopped" }, 409);

        const limit = maxUploadBytes();
        const declared = Number(request.headers.get("content-length") ?? 0);
        if (declared > limit) return json({ error: tooLargeMessage(limit) }, 413);
        if (!request.body) return json({ error: "Empty body" }, 400);

        // The MP4 can be over a gigabyte and the server has ~2 GB of RAM, so it
        // goes to a scratch file on disk, never into memory.
        const tmpDir = path.join(scratchRoot(), "render-uploads");
        mkdirSync(tmpDir, { recursive: true });
        const tmp = path.join(tmpDir, `${job.id}-${randomUUID()}.part`);
        let received = 0;
        let tooLarge = false;
        try {
          // Past the limit, stop writing but keep reading to the end. Aborting the
          // request body mid-stream makes Node throw "Controller is already
          // closed" from the still-flowing socket.
          const counter = new Transform({
            transform(chunk: Buffer, _enc, cb) {
              if (tooLarge) return cb();
              received += chunk.length;
              if (received > limit) {
                tooLarge = true;
                return cb();
              }
              cb(null, chunk);
            },
          });
          await pipeline(
            Readable.fromWeb(request.body as Parameters<typeof Readable.fromWeb>[0]),
            counter,
            createWriteStream(tmp),
          );
          if (tooLarge) return json({ error: tooLargeMessage(limit) }, 413);
          if (received === 0) return json({ error: "Empty body" }, 400);

          const payload = job.payload as { filename?: string };
          const filename = (payload.filename ?? `${job.id}.mp4`).replace(/[^\w.\- ]+/g, "");
          const url = await putAssetFromFile({
            kind: "project",
            relPath: `renders/${job.id}/${filename}`,
            filePath: tmp,
            contentType: "video/mp4",
          });

          // It may have been stopped while the file was uploading.
          const latest = await getRenderJob(id);
          if (!latest || latest.status === "cancelled") {
            return json({ error: "This render was stopped" }, 409);
          }
          const updated = await updateRenderJob(id, {
            status: "done",
            progress: 1,
            stage: "done",
            outputUrl: url,
            error: null,
          });
          return Response.json({ ok: true, outputUrl: updated?.output_url ?? url });
        } finally {
          rmSync(tmp, { force: true });
        }
      },
    },
  },
});

/**
 * Resolve the course's current template theme (blue/orange) at download time,
 * so jobs queued before the theme existed (or before it was changed) still
 * render with the right colours. Also stamped onto every scene so renderers
 * that read per-scene fields pick it up.
 */
async function jobPayload(job: Awaited<ReturnType<typeof getRenderJob>>) {
  if (!job) return null;
  const payload = { ...((job.payload as Record<string, unknown>) ?? {}) };
  let theme = (payload.templateTheme as string | undefined) ?? "orange";
  if (job.course_id) {
    try {
      const course = await localGetCourseById(job.course_id);
      theme = resolveCourseTemplateTheme(normalizeCourseSettings(course?.settings), course?.title);
    } catch {
      // keep frozen theme
    }
  }
  payload.templateTheme = theme;
  if (Array.isArray(payload.scenes)) {
    payload.scenes = payload.scenes.map((s) =>
      s && typeof s === "object" ? { ...(s as Record<string, unknown>), templateTheme: theme } : s,
    );
  }
  return {
    id: job.id,
    status: job.status,
    episodeTitle: job.episode_title,
    partTitle: job.part_title,
    requestedBy: job.requested_by_email,
    durationMs: job.duration_ms,
    sceneCount: job.scene_count,
    createdAt: job.created_at,
    outputUrl: job.output_url,
    ...payload,
  };
}

/** Largest accepted render upload (RENDER_MAX_UPLOAD_MB, default 4 GB). */
function maxUploadBytes(): number {
  const mb = Number(process.env.RENDER_MAX_UPLOAD_MB);
  return (Number.isFinite(mb) && mb > 0 ? mb : 4096) * 1024 * 1024;
}

function tooLargeMessage(limit: number): string {
  return `Upload too large (limit ${Math.round(limit / 1024 / 1024)} MB)`;
}

function splat(value: string | undefined): { id: string | null; sub: string | null } {
  const parts = (value ?? "").split("/").filter(Boolean);
  return { id: parts[0] ?? null, sub: parts[1] ?? null };
}

function json(data: unknown, status: number): Response {
  return new Response(JSON.stringify(data), {
    status,
    headers: { "Content-Type": "application/json" },
  });
}
