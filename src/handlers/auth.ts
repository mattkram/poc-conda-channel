import type { Env } from "../types.js";
import { b64url, hmac } from "../utils.js";

export { b64url, hmac };

// ---------------------------------------------------------------------------
// HMAC-signed upload tokens
// ---------------------------------------------------------------------------

export async function signUploadToken(
  payload: { login: string; channel?: string; pkg?: string },
  secret: string,
  ttl = 3600,
): Promise<string> {
  const body = { ...payload, exp: Math.floor(Date.now() / 1000) + ttl };
  const encoded = b64url(JSON.stringify(body));
  const sig = await hmac(encoded, secret);
  return `${encoded}.${sig}`;
}

export async function verifyUploadToken(
  token: string,
  secret: string,
): Promise<{ login: string; channel?: string; pkg?: string } | null> {
  const [encoded, sig] = token.split(".");
  if (!encoded || !sig) return null;
  const expected = await hmac(encoded, secret);
  if (expected !== sig) return null;
  const payload = JSON.parse(atob(encoded));
  if (payload.exp < Math.floor(Date.now() / 1000)) return null;
  return payload;
}

// ---------------------------------------------------------------------------
// Session cookie helpers
// ---------------------------------------------------------------------------

export const SESSION_COOKIE = "__session";
export const SESSION_TTL = 8 * 3600; // seconds

export function getSessionToken(request: Request): string | null {
  const cookie = request.headers.get("cookie") ?? "";
  for (const part of cookie.split(";")) {
    const [k, ...v] = part.trim().split("=");
    if (k.trim() === SESSION_COOKIE) return decodeURIComponent(v.join("="));
  }
  return null;
}

export async function getSessionLogin(request: Request, secret: string): Promise<string | null> {
  const token = getSessionToken(request);
  if (!token) return null;
  const claims = await verifyUploadToken(token, secret);
  return claims?.login ?? null;
}

export function sessionCookieHeader(token: string, maxAge: number): string {
  return `${SESSION_COOKIE}=${encodeURIComponent(token)}; Path=/; Max-Age=${maxAge}; HttpOnly; Secure; SameSite=Lax`;
}

// ---------------------------------------------------------------------------
// GitHub Device Flow
// ---------------------------------------------------------------------------

export async function startDeviceFlow(env: Env): Promise<Response> {
  const resp = await fetch("https://github.com/login/device/code", {
    method: "POST",
    headers: { accept: "application/json", "content-type": "application/json" },
    body: JSON.stringify({ client_id: env.GITHUB_CLIENT_ID, scope: "read:user" }),
  });
  const data = await resp.json();
  return Response.json(data);
}

export async function pollDeviceFlow(request: Request, env: Env): Promise<Response> {
  const { device_code } = await request.json<{ device_code: string }>();

  const resp = await fetch("https://github.com/login/oauth/access_token", {
    method: "POST",
    headers: { accept: "application/json", "content-type": "application/json" },
    body: JSON.stringify({
      client_id: env.GITHUB_CLIENT_ID,
      device_code,
      grant_type: "urn:ietf:params:oauth:grant-type:device_code",
    }),
  });
  const data = await resp.json<{ access_token?: string; error?: string }>();
  if (!data.access_token) {
    return Response.json(data, { status: 202 });
  }

  const ghUser = await fetch("https://api.github.com/user", {
    headers: { authorization: `Bearer ${data.access_token}`, "user-agent": "conda-wit" },
  }).then((r) => r.json<{ login: string }>());

  const uploadToken = await signUploadToken({ login: ghUser.login }, env.UPLOAD_TOKEN_SECRET);
  return Response.json({ upload_token: uploadToken, expires_in: 3600 });
}

// ---------------------------------------------------------------------------
// Browser OAuth (GitHub web application flow)
// ---------------------------------------------------------------------------

export async function handleBrowserLoginStart(request: Request, env: Env): Promise<Response> {
  const nonce = b64url(String.fromCharCode(...crypto.getRandomValues(new Uint8Array(16))));
  const state = `${nonce}.${await hmac(nonce, env.UPLOAD_TOKEN_SECRET)}`;

  const redirectUri = new URL("/auth/callback", new URL(request.url).origin).toString();
  const params = new URLSearchParams({
    client_id: env.GITHUB_CLIENT_ID,
    redirect_uri: redirectUri,
    scope: "read:user",
    state,
  });
  const githubUrl = `https://github.com/login/oauth/authorize?${params}`;

  return new Response(null, {
    status: 302,
    headers: {
      location: githubUrl,
      "set-cookie": `__oauth_state=${encodeURIComponent(state)}; Path=/auth/callback; Max-Age=600; HttpOnly; Secure; SameSite=Lax`,
    },
  });
}

export async function handleBrowserLoginCallback(
  request: Request,
  url: URL,
  env: Env,
): Promise<Response> {
  const code = url.searchParams.get("code");
  const state = url.searchParams.get("state");

  const errorPage = (msg: string) =>
    new Response(
      `<!DOCTYPE html><html><head><meta charset="utf-8"><title>Login error</title>` +
        `<style>body{font-family:sans-serif;padding:40px;color:#c0392b}</style></head>` +
        `<body><h2>Login failed</h2><p>${msg}</p><a href="/">Back</a></body></html>`,
      { status: 400, headers: { "content-type": "text/html;charset=utf-8" } },
    );

  if (!code || !state) return errorPage("Missing code or state from GitHub.");

  const cookieHeader = request.headers.get("cookie") ?? "";
  const stateCookie = cookieHeader
    .split(";")
    .map((p) => p.trim())
    .find((p) => p.startsWith("__oauth_state="))
    ?.slice("__oauth_state=".length);
  if (!stateCookie || decodeURIComponent(stateCookie) !== state) {
    return errorPage("State mismatch — possible CSRF. Please try logging in again.");
  }
  const [nonce, sig] = state.split(".");
  if (!nonce || !sig || sig !== (await hmac(nonce, env.UPLOAD_TOKEN_SECRET))) {
    return errorPage("Invalid state signature.");
  }

  const origin = new URL(request.url).origin;
  const tokenResp = await fetch("https://github.com/login/oauth/access_token", {
    method: "POST",
    headers: { accept: "application/json", "content-type": "application/json" },
    body: JSON.stringify({
      client_id: env.GITHUB_CLIENT_ID,
      client_secret: env.GITHUB_CLIENT_SECRET,
      code,
      redirect_uri: `${origin}/auth/callback`,
    }),
  });
  const tokenData = await tokenResp.json<{
    access_token?: string;
    error?: string;
    error_description?: string;
  }>();
  if (!tokenData.access_token) {
    return errorPage(
      `GitHub error: ${tokenData.error_description ?? tokenData.error ?? "unknown"}`,
    );
  }

  const ghUser = await fetch("https://api.github.com/user", {
    headers: {
      authorization: `Bearer ${tokenData.access_token}`,
      "user-agent": "conda-wit",
    },
  }).then((r) => r.json<{ login: string }>());

  const sessionToken = await signUploadToken(
    { login: ghUser.login },
    env.UPLOAD_TOKEN_SECRET,
    SESSION_TTL,
  );

  return new Response(null, {
    status: 302,
    headers: new Headers([
      ["location", "/channels"],
      ["set-cookie", sessionCookieHeader(sessionToken, SESSION_TTL)],
      [
        "set-cookie",
        `__oauth_state=; Path=/auth/callback; Max-Age=0; HttpOnly; Secure; SameSite=Lax`,
      ],
    ]),
  });
}

export function handleBrowserLogout(_url: URL): Response {
  return new Response(null, {
    status: 302,
    headers: new Headers([
      ["location", "/channels"],
      ["set-cookie", sessionCookieHeader("", 0)],
    ]),
  });
}

// ---------------------------------------------------------------------------
// resolveLogin — used by browse and admin handlers
// ---------------------------------------------------------------------------

export async function resolveLogin(request: Request, secret: string): Promise<string | null> {
  const auth = request.headers.get("authorization") ?? "";
  const bearer = auth.replace(/^Bearer\s+/i, "");
  if (bearer) {
    const claims = await verifyUploadToken(bearer, secret);
    if (claims) return claims.login;
  }
  return getSessionLogin(request, secret);
}
