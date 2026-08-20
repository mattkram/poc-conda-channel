import { AwsClient } from "aws4fetch";
import type { Env } from "./types.js";
import { CHANNEL_NAME_RE, checkChannelAccess } from "./channel.js";
import { verifyUploadToken, signUploadToken } from "./auth.js";

export const PRESIGN_TTL_SECONDS = 900;

export function r2Client(env: Env): AwsClient {
  return new AwsClient({
    service: "s3",
    region: "auto",
    accessKeyId: env.R2_ACCESS_KEY_ID,
    secretAccessKey: env.R2_SECRET_ACCESS_KEY,
  });
}

export function stagingKeyFor(channel: string, filename: string): string {
  return `${channel}/_incoming/${filename}`;
}

export function validateChannelAndFilename(channel: string, filename: string): string | null {
  if (!CHANNEL_NAME_RE.test(channel)) return "invalid channel name";
  if (filename.includes("/")) return "invalid filename";
  if (!filename.endsWith(".conda") && !filename.endsWith(".tar.bz2")) {
    return "only .conda or .tar.bz2 packages are accepted";
  }
  return null;
}

// ---------------------------------------------------------------------------
// Token scope helpers
// ---------------------------------------------------------------------------

export function checkTokenScope(
  claims: { login: string; channel?: string; pkg?: string },
  channel: string,
  filename: string,
): string | null {
  if (claims.channel && claims.channel !== channel) {
    return `token is scoped to channel '${claims.channel}', cannot upload to '${channel}'`;
  }
  if (claims.pkg) {
    const pkgName = filename.replace(/[-_][\d].*$/, "");
    if (pkgName !== claims.pkg) {
      return `token is scoped to package '${claims.pkg}', cannot upload '${filename}'`;
    }
  }
  return null;
}

export async function isTrustedPublishingRequired(channel: string, env: Env): Promise<boolean> {
  const row = await env.DB.prepare(
    `SELECT COUNT(*) as total, SUM(require_trusted) as req FROM trusted_publishers WHERE channel = ?`,
  )
    .bind(channel)
    .first<{ total: number; req: number | null }>();
  if (!row || row.total === 0) return false;
  return (row.req ?? 0) >= row.total;
}

// ---------------------------------------------------------------------------
// Upload handlers
// ---------------------------------------------------------------------------

export async function handleUploadInit(request: Request, env: Env): Promise<Response> {
  const auth = request.headers.get("authorization") ?? "";
  const token = auth.replace(/^Bearer\s+/i, "");
  const claims = await verifyUploadToken(token, env.UPLOAD_TOKEN_SECRET);
  if (!claims) return new Response("unauthorized", { status: 401 });

  const { channel, filename } = await request.json<{ channel: string; filename: string }>();
  const invalid = validateChannelAndFilename(channel, filename);
  if (invalid) return new Response(invalid, { status: 400 });

  const scopeErr = checkTokenScope(claims, channel, filename);
  if (scopeErr) return new Response(scopeErr, { status: 403 });

  if (!claims.channel) {
    const trusted = await isTrustedPublishingRequired(channel, env);
    if (trusted)
      return new Response(
        `channel '${channel}' requires trusted publishing — use POST /upload/exchange-oidc to get an upload token`,
        { status: 403 },
      );
  }

  const denied = await checkChannelAccess(channel, claims.login, env);
  if (denied) return denied;

  const key = stagingKeyFor(channel, filename);
  const objectUrl = new URL(
    `https://${env.R2_ACCOUNT_ID}.r2.cloudflarestorage.com/${env.R2_BUCKET_NAME}/${key}`,
  );
  objectUrl.searchParams.set("X-Amz-Expires", String(PRESIGN_TTL_SECONDS));

  const signed = await r2Client(env).sign(new Request(objectUrl, { method: "PUT" }), {
    aws: { signQuery: true },
  });

  return Response.json({
    upload_url: signed.url,
    method: "PUT",
    expires_in: PRESIGN_TTL_SECONDS,
  });
}

export async function handleUploadComplete(request: Request, env: Env): Promise<Response> {
  const auth = request.headers.get("authorization") ?? "";
  const token = auth.replace(/^Bearer\s+/i, "");
  const claims = await verifyUploadToken(token, env.UPLOAD_TOKEN_SECRET);
  if (!claims) return new Response("unauthorized", { status: 401 });

  const { channel, filename } = await request.json<{ channel: string; filename: string }>();
  const invalid = validateChannelAndFilename(channel, filename);
  if (invalid) return new Response(invalid, { status: 400 });

  const scopeErr = checkTokenScope(claims, channel, filename);
  if (scopeErr) return new Response(scopeErr, { status: 403 });

  const key = stagingKeyFor(channel, filename);
  const head = await env.CHANNEL_BUCKET.head(key);
  if (!head) {
    return new Response(
      `no object staged at ${key} — did the upload PUT succeed?`,
      { status: 409 },
    );
  }

  const queueId = env.QUEUE.idFromName(channel);
  const queue = env.QUEUE.get(queueId);
  await queue.fetch("http://queue/enqueue", {
    method: "POST",
    body: JSON.stringify({
      channel,
      filename,
      uploadedAt: Date.now(),
      uploadedBy: claims.login,
    }),
    headers: { "content-type": "application/json" },
  });

  return new Response(`queued ${filename} for channel ${channel}`, { status: 202 });
}
