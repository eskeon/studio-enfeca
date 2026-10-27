import {
  copyFileSync,
  createReadStream,
  existsSync,
  mkdirSync,
  readFileSync,
  statSync,
  writeFileSync,
} from "node:fs";
import { request as httpRequest } from "node:http";
import { request as httpsRequest } from "node:https";
import { Readable } from "node:stream";
import path from "node:path";
import { hostAppAssetsRoot, hostProjectAssetsRoot } from "@/lib/host-storage";
import {
  hasCloudStorage,
  isEdgeRuntime,
  useCloudStorage,
  useSpaces,
} from "@/lib/runtime-backends";
import { presignS3Url } from "@/lib/s3-sigv4";

export type AssetKind = "project" | "app";

/** Cloud bucket layout mirrors the disk layout: `<kind>/<relPath>`. */
const CLOUD_BUCKET = "project-assets";

function cloudObjectUrl(kind: AssetKind, rel: string): string {
  const base = process.env.SUPABASE_URL!.trim().replace(/\/+$/, "");
  const prefix = kind === "app" ? "app-assets" : "project-assets";
  return `${base}/storage/v1/object/${CLOUD_BUCKET}/${prefix}/${rel}`;
}

function cloudHeaders(): Record<string, string> {
  const key = process.env.SUPABASE_SERVICE_ROLE_KEY!.trim();
  return { apikey: key, Authorization: `Bearer ${key}` };
}

type S3Like = {
  send: (command: unknown) => Promise<any>;
};

let spacesClientPromise: Promise<S3Like> | null = null;

function spacesEndpoint(): string {
  const raw = process.env.SPACES_ENDPOINT!.trim();
  if (/^https?:\/\//i.test(raw)) return raw;
  return `https://${raw}`;
}

function spacesBasePrefix(): string {
  const base = (process.env.SPACES_BASE_PATH ?? process.env.SPACES_PREFIX ?? "")
    .trim()
    .replace(/^\/+|\/+$/g, "");
  return base ? `${base}/` : "";
}

async function spacesClient(): Promise<any> {
  if (!spacesClientPromise) {
    spacesClientPromise = (async () => {
      const { S3Client } = await import("@aws-sdk/client-s3");
      const region = process.env.SPACES_REGION?.trim() || "blr1";
      return new S3Client({
        endpoint: spacesEndpoint(),
        region,
        credentials: {
          accessKeyId: process.env.SPACES_KEY!.trim(),
          secretAccessKey: process.env.SPACES_SECRET!.trim(),
        },
        forcePathStyle: false,
      }) as any;
    })();
  }
  return spacesClientPromise!;
}

function bucket(): string {
  return process.env.SPACES_BUCKET!.trim();
}

function spacesKey(kind: AssetKind, rel: string): string {
  const clean = rel.replace(/^\/+/, "");
  const kindPrefix = kind === "app" ? "app-assets" : "project-assets";
  return `${spacesBasePrefix()}${kindPrefix}/${clean}`;
}

/**
 * Presign with WebCrypto instead of the AWS SDK. The SDK's S3 client does not
 * work in the published edge worker, which made every stored asset 404 there.
 */
function spacesPresign(
  method: "GET" | "PUT" | "HEAD",
  kind: AssetKind,
  rel: string,
  expiresIn = 3600,
  _contentType?: string,
): Promise<string> {
  return presignS3Url({
    method,
    endpoint: spacesEndpoint(),
    region: process.env.SPACES_REGION?.trim() || "blr1",
    bucket: bucket(),
    key: spacesKey(kind, rel),
    accessKeyId: process.env.SPACES_KEY!.trim(),
    secretAccessKey: process.env.SPACES_SECRET!.trim(),
    expiresIn,
  });
}

function localRoot(kind: AssetKind): string {
  return kind === "app" ? hostAppAssetsRoot() : hostProjectAssetsRoot();
}

/** Write bytes and return the public app path (`/api/assets/...` or `/api/app-assets/...`). */
export async function putAsset(opts: {
  kind: AssetKind;
  /** Relative path under the kind root, e.g. `{userId}/{projectId}/{file}`. */
  relPath: string;
  body: Buffer;
  contentType?: string;
}): Promise<string> {
  const rel = opts.relPath.replace(/^\/+/, "");
  if (!rel || rel.includes("..")) throw new Error("Invalid asset path");

  if (useCloudStorage()) {
    const res = await fetch(cloudObjectUrl(opts.kind, rel), {
      method: "POST",
      headers: {
        ...cloudHeaders(),
        "Content-Type": opts.contentType ?? "application/octet-stream",
        "x-upsert": "true",
      },
      body: new Uint8Array(opts.body),
    });
    if (!res.ok) {
      throw new Error(`Asset upload failed [${res.status}]: ${await res.text()}`);
    }
  } else if (useSpaces() && isEdgeRuntime()) {
    const url = await spacesPresign("PUT", opts.kind, rel, 900);
    const res = await fetch(url, {
      method: "PUT",
      headers: { "Content-Type": opts.contentType ?? "application/octet-stream" },
      body: new Uint8Array(opts.body),
    });
    if (!res.ok) {
      throw new Error(`Asset upload failed [${res.status}]: ${await res.text()}`);
    }
  } else if (useSpaces()) {
    const { PutObjectCommand } = await import("@aws-sdk/client-s3");
    const client = await spacesClient();
    await client.send(
      new PutObjectCommand({
        Bucket: bucket(),
        Key: spacesKey(opts.kind, rel),
        Body: opts.body,
        ContentType: opts.contentType,
        ACL: "private",
      }),
    );
  } else {
    const full = path.join(localRoot(opts.kind), rel);
    mkdirSync(path.dirname(full), { recursive: true });
    writeFileSync(full, opts.body);
  }

  const prefix = opts.kind === "app" ? "/api/app-assets" : "/api/assets";
  return `${prefix}/${rel}`;
}

/**
 * PUT a file to an already-signed URL without loading it into memory. Needs an
 * explicit Content-Length: S3/Spaces reject chunked PUT bodies.
 *
 * This uses node:http(s) with a piped read stream rather than fetch(): Node's
 * fetch reads a streamed request body as fast as it can with no backpressure, so
 * memory grew with the size of the file (measured: ~1 GB for a 1.1 GB upload).
 */
export async function putFileToSignedUrl(
  url: string,
  filePath: string,
  contentType: string,
  timeoutMs = 30 * 60 * 1000,
): Promise<void> {
  const size = statSync(filePath).size;
  const target = new URL(url);
  const request = target.protocol === "http:" ? httpRequest : httpsRequest;

  await new Promise<void>((resolve, reject) => {
    const req = request(
      target,
      {
        method: "PUT",
        headers: { "Content-Type": contentType, "Content-Length": String(size) },
        timeout: timeoutMs,
      },
      (res) => {
        const chunks: Buffer[] = [];
        res.on("data", (c: Buffer) => chunks.push(c));
        res.on("end", () => {
          if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) {
            resolve();
          } else {
            const text = Buffer.concat(chunks).toString("utf8").slice(0, 500);
            reject(new Error(`Asset upload failed [${res.statusCode}]: ${text}`));
          }
        });
        res.on("error", reject);
      },
    );
    req.on("timeout", () => req.destroy(new Error("Asset upload timed out")));
    req.on("error", reject);
    const file = createReadStream(filePath);
    file.on("error", (e) => req.destroy(e));
    file.pipe(req);
  });
}

/**
 * Store a file that is already on disk (e.g. a finished render) without holding
 * it in memory. On Spaces it streams to a presigned URL; if that fails for any
 * reason it falls back to the buffered upload putAsset() has always done, so the
 * worst case is the previous behaviour.
 */
export async function putAssetFromFile(opts: {
  kind: AssetKind;
  relPath: string;
  filePath: string;
  contentType?: string;
}): Promise<string> {
  const rel = opts.relPath.replace(/^\/+/, "");
  if (!rel || rel.includes("..")) throw new Error("Invalid asset path");
  const prefix = opts.kind === "app" ? "/api/app-assets" : "/api/assets";
  const contentType = opts.contentType ?? "application/octet-stream";

  const buffered = () =>
    putAsset({
      kind: opts.kind,
      relPath: rel,
      body: readFileSync(opts.filePath),
      contentType: opts.contentType,
    });

  if (useCloudStorage() || (useSpaces() && isEdgeRuntime())) return buffered();

  if (useSpaces()) {
    try {
      const url = await spacesPresign("PUT", opts.kind, rel, 3600);
      await putFileToSignedUrl(url, opts.filePath, contentType);
      return `${prefix}/${rel}`;
    } catch (e) {
      console.warn(
        "[assets] streamed upload failed, retrying buffered:",
        e instanceof Error ? e.message : e,
      );
      return buffered();
    }
  }

  const full = path.join(localRoot(opts.kind), rel);
  mkdirSync(path.dirname(full), { recursive: true });
  copyFileSync(opts.filePath, full);
  return `${prefix}/${rel}`;
}

/**
 * Direct, time-limited download URL for an object, so large media (e.g. .mov
 * screen recordings) never streams through the app server / edge worker.
 * Returns null when assets live on local disk (dev), where proxying is fine.
 */
/**
 * Presigned PUT URL so the browser can upload a large file straight to Spaces
 * without proxying bytes through the app worker. Returns null when Spaces is
 * not the active storage backend.
 */
export async function signedUploadUrl(
  kind: AssetKind,
  relPath: string,
  contentType?: string,
  expiresInSeconds = 60 * 30,
): Promise<{ uploadUrl: string; appUrl: string } | null> {
  if (!useSpaces()) return null;
  const rel = relPath.replace(/^\/+/, "");
  if (!rel || rel.includes("..")) return null;
  let uploadUrl: string;
  if (isEdgeRuntime()) {
    uploadUrl = await spacesPresign("PUT", kind, rel, expiresInSeconds, contentType);
  } else {
    const { PutObjectCommand } = await import("@aws-sdk/client-s3");
    const { getSignedUrl } = await import("@aws-sdk/s3-request-presigner");
    const client = await spacesClient();
    uploadUrl = await getSignedUrl(
      client,
      new PutObjectCommand({
        Bucket: bucket(),
        Key: spacesKey(kind, rel),
        ...(contentType ? { ContentType: contentType } : {}),
      }),
      { expiresIn: expiresInSeconds },
    );
  }
  const prefix = kind === "app" ? "/api/app-assets" : "/api/assets";
  return { uploadUrl, appUrl: `${prefix}/${rel}` };
}

/** Signed cloud-bucket URL, or null when the object is not there. */
async function cloudSignedUrl(
  kind: AssetKind,
  rel: string,
  expiresInSeconds: number,
): Promise<string | null> {
  const base = process.env.SUPABASE_URL!.trim().replace(/\/+$/, "");
  const prefix = kind === "app" ? "app-assets" : "project-assets";
  const res = await fetch(`${base}/storage/v1/object/sign/${CLOUD_BUCKET}/${prefix}/${rel}`, {
    method: "POST",
    headers: { ...cloudHeaders(), "Content-Type": "application/json" },
    body: JSON.stringify({ expiresIn: expiresInSeconds }),
  });
  if (!res.ok) return null;
  const json = (await res.json()) as { signedURL?: string; signedUrl?: string };
  const signed = json.signedURL ?? json.signedUrl;
  if (!signed) return null;
  return `${base}/storage/v1${signed.startsWith("/") ? signed : `/${signed}`}`;
}

export async function signedAssetUrl(
  kind: AssetKind,
  relPath: string,
  expiresInSeconds = 60 * 60 * 6,
): Promise<string | null> {
  const rel = relPath.replace(/^\/+/, "");
  if (!rel || rel.includes("..")) return null;

  if (useCloudStorage()) {
    return cloudSignedUrl(kind, rel, expiresInSeconds);
  }

  if (useSpaces()) {
    // Objects uploaded before the Spaces migration still live in the cloud
    // bucket, so only sign a Spaces URL when the object is actually there.
    if (await spacesExists(kind, rel)) {
      if (isEdgeRuntime()) {
        return spacesPresign("GET", kind, rel, expiresInSeconds);
      }
      const { GetObjectCommand } = await import("@aws-sdk/client-s3");
      const { getSignedUrl } = await import("@aws-sdk/s3-request-presigner");
      const client = await spacesClient();
      return getSignedUrl(
        client,
        new GetObjectCommand({ Bucket: bucket(), Key: spacesKey(kind, rel) }),
        { expiresIn: expiresInSeconds },
      );
    }
    if (hasCloudStorage()) return cloudSignedUrl(kind, rel, expiresInSeconds);
    return null;
  }

  return null;
}

export type AssetReadResult = {
  status: 200 | 206;
  body: ReadableStream;
  headers: Record<string, string>;
};

function parseByteRange(
  header: string | null,
  size: number,
): { start: number; end: number } | null {
  if (!header || !header.startsWith("bytes=")) return null;
  const m = /^bytes=(\d*)-(\d*)$/.exec(header.trim());
  if (!m) return null;
  const startRaw = m[1];
  const endRaw = m[2];
  if (!startRaw && !endRaw) return null;

  let start: number;
  let end: number;
  if (!startRaw) {
    const suffix = Number(endRaw);
    if (!Number.isFinite(suffix) || suffix <= 0) return null;
    start = Math.max(0, size - suffix);
    end = size - 1;
  } else {
    start = Number(startRaw);
    end = endRaw ? Number(endRaw) : size - 1;
  }
  if (!Number.isFinite(start) || !Number.isFinite(end)) return null;
  if (start < 0 || end < start || start >= size) return null;
  end = Math.min(end, size - 1);
  return { start, end };
}

async function cloudExists(kind: AssetKind, rel: string): Promise<boolean> {
  try {
    const res = await fetch(cloudObjectUrl(kind, rel), {
      method: "HEAD",
      headers: cloudHeaders(),
    });
    return res.ok;
  } catch {
    return false;
  }
}

async function spacesExists(kind: AssetKind, rel: string): Promise<boolean> {
  if (isEdgeRuntime()) {
    try {
      const res = await fetch(await spacesPresign("HEAD", kind, rel, 300), { method: "HEAD" });
      return res.ok;
    } catch {
      return false;
    }
  }
  try {
    const { HeadObjectCommand } = await import("@aws-sdk/client-s3");
    const client = await spacesClient();
    await client.send(new HeadObjectCommand({ Bucket: bucket(), Key: spacesKey(kind, rel) }));
    return true;
  } catch {
    return false;
  }
}

/** Serve a stored asset with optional HTTP Range (local disk or Spaces). */
/** True when the object already exists in the active storage backend. */
export async function assetExists(kind: AssetKind, relPath: string): Promise<boolean> {
  const rel = relPath.replace(/^\/+/, "");
  if (!rel || rel.includes("..")) return false;

  if (useCloudStorage()) return cloudExists(kind, rel);

  if (useSpaces()) {
    if (await spacesExists(kind, rel)) return true;
    return hasCloudStorage() ? cloudExists(kind, rel) : false;
  }

  return existsSync(path.join(localRoot(kind), rel));
}

export async function readAsset(opts: {
  kind: AssetKind;
  relPath: string;
  contentType: string;
  rangeHeader: string | null;
}): Promise<AssetReadResult | null> {
  const rel = opts.relPath.replace(/^\/+/, "");
  if (!rel || rel.includes("..")) return null;

  const common = {
    "Content-Type": opts.contentType,
    "Accept-Ranges": "bytes",
    "Cache-Control": "public, max-age=31536000, immutable",
  };

  const cloudRead = async (): Promise<AssetReadResult | null> => {
    const res = await fetch(cloudObjectUrl(opts.kind, rel), {
      headers: {
        ...cloudHeaders(),
        ...(opts.rangeHeader ? { Range: opts.rangeHeader } : {}),
      },
    });
    if (!res.ok || !res.body) return null;
    const len = res.headers.get("content-length");
    const contentRange = res.headers.get("content-range");
    return {
      status: res.status === 206 ? 206 : 200,
      body: res.body,
      headers: {
        ...common,
        ...(len ? { "Content-Length": len } : {}),
        ...(contentRange ? { "Content-Range": contentRange } : {}),
      },
    };
  };

  if (useCloudStorage()) return cloudRead();

  if (useSpaces() && isEdgeRuntime()) {
    // Edge worker: sign with WebCrypto and stream the object straight through.
    const url = await spacesPresign("GET", opts.kind, rel, 900);
    const res = await fetch(url, {
      headers: opts.rangeHeader ? { Range: opts.rangeHeader } : {},
    });
    if (!res.ok || !res.body) {
      // Pre-migration object: still served from the cloud bucket.
      return hasCloudStorage() ? cloudRead() : null;
    }
    const len = res.headers.get("content-length");
    const contentRange = res.headers.get("content-range");
    return {
      status: res.status === 206 ? 206 : 200,
      body: res.body,
      headers: {
        ...common,
        ...(len ? { "Content-Length": len } : {}),
        ...(contentRange ? { "Content-Range": contentRange } : {}),
      },
    };
  }

  if (useSpaces()) {
    const { GetObjectCommand, HeadObjectCommand } = await import("@aws-sdk/client-s3");
    const key = spacesKey(opts.kind, rel);
    const client = await spacesClient();
    let size: number;
    try {
      const head = await client.send(
        new HeadObjectCommand({ Bucket: bucket(), Key: key }),
      );
      size = Number(head.ContentLength ?? 0);
    } catch {
      return hasCloudStorage() ? cloudRead() : null;
    }
    if (!Number.isFinite(size) || size < 0) return null;

    const range = parseByteRange(opts.rangeHeader, size);
    const get = await client.send(
      new GetObjectCommand({
        Bucket: bucket(),
        Key: key,
        ...(range ? { Range: `bytes=${range.start}-${range.end}` } : {}),
      }),
    );
    const body = get.Body;
    if (!body) return null;
    const webStream =
      typeof body.transformToWebStream === "function"
        ? body.transformToWebStream()
        : (Readable.toWeb(body as any) as ReadableStream);

    if (range) {
      const { start, end } = range;
      return {
        status: 206,
        body: webStream as ReadableStream,
        headers: {
          ...common,
          "Content-Length": String(end - start + 1),
          "Content-Range": `bytes ${start}-${end}/${size}`,
        },
      };
    }
    return {
      status: 200,
      body: webStream as ReadableStream,
      headers: {
        ...common,
        "Content-Length": String(size),
      },
    };
  }

  const full = path.join(localRoot(opts.kind), rel);
  const resolved = path.resolve(full);
  if (!resolved.startsWith(path.resolve(localRoot(opts.kind)))) return null;
  if (!existsSync(resolved)) return null;

  const { size } = statSync(resolved);
  const range = parseByteRange(opts.rangeHeader, size);
  if (range) {
    const { start, end } = range;
    const stream = createReadStream(resolved, { start, end });
    return {
      status: 206,
      body: Readable.toWeb(stream) as ReadableStream,
      headers: {
        ...common,
        "Content-Length": String(end - start + 1),
        "Content-Range": `bytes ${start}-${end}/${size}`,
      },
    };
  }
  const stream = createReadStream(resolved);
  return {
    status: 200,
    body: Readable.toWeb(stream) as ReadableStream,
    headers: {
      ...common,
      "Content-Length": String(size),
    },
  };
}

/** Download asset bytes to a local file (for ffmpeg / export scratch). */
export async function materializeAssetToFile(
  kind: AssetKind,
  relPath: string,
  destPath: string,
): Promise<void> {
  const rel = relPath.replace(/^\/+/, "");
  mkdirSync(path.dirname(destPath), { recursive: true });

  const fromCloud = async () => {
    const res = await fetch(cloudObjectUrl(kind, rel), { headers: cloudHeaders() });
    if (!res.ok) throw new Error(`Missing cloud object: ${rel} (${res.status})`);
    writeFileSync(destPath, Buffer.from(new Uint8Array(await res.arrayBuffer())));
  };

  if (useCloudStorage()) return fromCloud();

  if (!useSpaces()) {
    const { copyFileSync } = await import("node:fs");
    const src = path.join(localRoot(kind), rel);
    copyFileSync(src, destPath);
    return;
  }

  try {
    const { GetObjectCommand } = await import("@aws-sdk/client-s3");
    const get = await (await spacesClient()).send(
      new GetObjectCommand({
        Bucket: bucket(),
        Key: spacesKey(kind, rel),
      }),
    );
    const bytes = await get.Body?.transformToByteArray();
    if (!bytes) throw new Error(`Missing Spaces object: ${rel}`);
    writeFileSync(destPath, Buffer.from(bytes));
  } catch (err) {
    // Pre-migration object: still in the cloud bucket.
    if (hasCloudStorage()) return fromCloud();
    throw err;
  }
}
