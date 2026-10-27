import { existsSync } from "node:fs";
import path from "node:path";
import { spawn, spawnSync } from "node:child_process";
import { createRequire } from "node:module";

const require = createRequire(import.meta.url);

export const NO_LOCAL_VIDEO_TOOLS_MESSAGE =
  "Voice replacement for screen recordings runs only on the studio machines (Mac app or the self-hosted server). This hosted site can't process video files, so open this episode in the studio app to swap the voice.";

/** Serverless/edge runtimes stub child_process — detect that once. */
let processSpawnSupported: boolean | null = null;
function canSpawnProcesses(): boolean {
  if (processSpawnSupported !== null) return processSpawnSupported;
  try {
    spawnSync(process.execPath ?? "node", ["-e", ""], { timeout: 5_000 });
    processSpawnSupported = true;
  } catch {
    processSpawnSupported = false;
  }
  return processSpawnSupported;
}

/**
 * `ffmpeg -version` blocks the whole server while it runs, and every ffmpeg or
 * ffprobe call used to repeat it. A binary that works keeps working, so remember
 * it for the life of the process; a failure is only remembered briefly so that
 * installing ffmpeg later is still picked up.
 */
const FFMPEG_OK = new Set<string>();
const FFMPEG_FAILED_AT = new Map<string, number>();
const FFMPEG_RETRY_MS = 60_000;

/** True when this path is a runnable ffmpeg for the current CPU. */
function canRunFfmpeg(bin: string): boolean {
  if (!bin || (bin !== "ffmpeg" && !existsSync(bin))) return false;
  if (FFMPEG_OK.has(bin)) return true;
  const failedAt = FFMPEG_FAILED_AT.get(bin);
  if (failedAt !== undefined && Date.now() - failedAt < FFMPEG_RETRY_MS) return false;
  let ok = false;
  try {
    const r = spawnSync(bin, ["-version"], {
      encoding: "utf8",
      timeout: 8_000,
    });
    // macOS Apple Silicon + Intel binary → errno -86 (EBADARCH)
    ok = !r.error && r.status === 0;
  } catch {
    ok = false;
  }
  if (ok) {
    FFMPEG_OK.add(bin);
    FFMPEG_FAILED_AT.delete(bin);
  } else {
    FFMPEG_FAILED_AT.set(bin, Date.now());
  }
  return ok;
}

function ffmpegStaticPath(): string | null {
  try {
    const p = require("ffmpeg-static") as string | null;
    if (p && existsSync(p)) return p;
  } catch {
    /* optional */
  }
  return null;
}

function candidateBins(): string[] {
  const fromEnv = process.env.FFMPEG_PATH?.trim();
  const staticPath = ffmpegStaticPath();
  return [
    fromEnv,
    staticPath,
    // Prefer the known-good package binary over a possibly-wrong .tools copy
    path.join(process.cwd(), "node_modules", "ffmpeg-static", "ffmpeg"),
    path.join(process.cwd(), ".tools", "ffmpeg"),
    "/opt/homebrew/bin/ffmpeg",
    "/usr/local/bin/ffmpeg",
    `${process.env.HOME}/.local/bin/ffmpeg`,
    "/usr/bin/ffmpeg",
    "ffmpeg",
  ].filter((p): p is string => !!p);
}

/** Resolve a working ffmpeg binary (throws if none can run on this CPU). */
export async function resolveFfmpegBin(): Promise<string> {
  if (!canSpawnProcesses()) throw new Error(NO_LOCAL_VIDEO_TOOLS_MESSAGE);
  const tried: string[] = [];
  for (const bin of candidateBins()) {
    if (tried.includes(bin)) continue;
    tried.push(bin);
    if (canRunFfmpeg(bin)) return bin;
  }
  throw new Error(
    "No working ffmpeg for this Mac (wrong CPU arch or missing). Install with: brew install ffmpeg  (or set FFMPEG_PATH to an arm64 build)",
  );
}

export function runFfmpeg(bin: string, args: string[]): Promise<void> {
  return new Promise((resolve, reject) => {
    let p: ReturnType<typeof spawn>;
    try {
      p = spawn(bin, args, { stdio: ["ignore", "pipe", "pipe"] });
    } catch {
      reject(new Error(NO_LOCAL_VIDEO_TOOLS_MESSAGE));
      return;
    }
    let err = "";
    p.stderr?.on("data", (d) => {
      err += String(d);
    });
    p.on("error", (e) => {
      const msg = e instanceof Error ? e.message : String(e);
      if (msg.includes("-86") || (e as NodeJS.ErrnoException).errno === -86) {
        reject(
          new Error(
            `ffmpeg binary is the wrong CPU type for this Mac (${bin}). Use the arm64 build from ffmpeg-static or: brew install ffmpeg`,
          ),
        );
        return;
      }
      reject(e);
    });
    p.on("close", (code) => {
      if (code === 0) resolve();
      else reject(new Error(`ffmpeg failed (${code}): ${err.slice(-800)}`));
    });
  });
}

export async function probeMediaDurationMs(
  ffmpegBin: string,
  filePath: string,
): Promise<number> {
  const ffprobe = ffmpegBin.replace(/ffmpeg$/, "ffprobe");
  if (ffprobe !== ffmpegBin && existsSync(ffprobe) && canRunFfmpeg(ffprobe)) {
    const ms = await new Promise<number>((resolve) => {
      const p = spawn(
        ffprobe,
        [
          "-v",
          "error",
          "-show_entries",
          "format=duration",
          "-of",
          "default=noprint_wrappers=1:nokey=1",
          filePath,
        ],
        { stdio: ["ignore", "pipe", "pipe"] },
      );
      let out = "";
      p.stdout?.on("data", (d) => {
        out += String(d);
      });
      p.on("close", (code) => {
        const sec = Number.parseFloat(out.trim());
        if (code === 0 && Number.isFinite(sec) && sec > 0) {
          resolve(Math.max(80, Math.round(sec * 1000)));
        } else resolve(0);
      });
      p.on("error", () => resolve(0));
    });
    if (ms > 0) return ms;
  }

  return new Promise((resolve) => {
    const p = spawn(ffmpegBin, ["-i", filePath], {
      stdio: ["ignore", "pipe", "pipe"],
    });
    let err = "";
    p.stderr?.on("data", (d) => {
      err += String(d);
    });
    p.on("close", () => {
      const m = /Duration:\s*(\d+):(\d+):(\d+(?:\.\d+)?)/.exec(err);
      if (!m) {
        resolve(0);
        return;
      }
      const h = Number(m[1]);
      const min = Number(m[2]);
      const sec = Number(m[3]);
      const total = ((h * 60 + min) * 60 + sec) * 1000;
      resolve(Number.isFinite(total) && total > 0 ? Math.max(80, Math.round(total)) : 0);
    });
    p.on("error", () => resolve(0));
  });
}

/** True if the media file has at least one audio stream. */
export async function probeHasAudioStream(
  ffmpegBin: string,
  filePath: string,
): Promise<boolean> {
  const ffprobe = ffmpegBin.replace(/ffmpeg$/, "ffprobe");
  if (ffprobe !== ffmpegBin && existsSync(ffprobe) && canRunFfmpeg(ffprobe)) {
    const count = await new Promise<number>((resolve) => {
      const p = spawn(
        ffprobe,
        [
          "-v",
          "error",
          "-select_streams",
          "a",
          "-show_entries",
          "stream=index",
          "-of",
          "csv=p=0",
          filePath,
        ],
        { stdio: ["ignore", "pipe", "pipe"] },
      );
      let out = "";
      p.stdout?.on("data", (d) => {
        out += String(d);
      });
      p.on("close", () => {
        const lines = out
          .split(/\r?\n/)
          .map((l) => l.trim())
          .filter(Boolean);
        resolve(lines.length);
      });
      p.on("error", () => resolve(0));
    });
    if (count > 0) return true;
  }

  return new Promise((resolve) => {
    const p = spawn(ffmpegBin, ["-i", filePath], {
      stdio: ["ignore", "pipe", "pipe"],
    });
    let err = "";
    p.stderr?.on("data", (d) => {
      err += String(d);
    });
    p.on("close", () => {
      resolve(/Stream #\d+:\d+.*Audio:/i.test(err));
    });
    p.on("error", () => resolve(false));
  });
}
