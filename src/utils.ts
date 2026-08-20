// ---------------------------------------------------------------------------
// Pure utility functions — no Env/IO dependencies, fully unit-testable.
// ---------------------------------------------------------------------------

/** HTML-escape a string for safe template interpolation. */
export function esc(s: string): string {
  return (s ?? "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

/** Extract namespace prefix from a channel name (e.g. "mattkram" from "mattkram/main"), or null. */
export function channelNamespace(channel: string): string | null {
  const idx = channel.indexOf("/");
  return idx === -1 ? null : channel.slice(0, idx);
}

/** Format a byte count as a human-readable string. */
export function fmtBytes(n: number): string {
  if (n < 1024) return `${n} B`;
  if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KB`;
  return `${(n / 1024 / 1024).toFixed(1)} MB`;
}

/** Format a conda timestamp (ms since epoch) as an ISO date string (YYYY-MM-DD). */
export function fmtDate(ts?: number): string {
  if (!ts) return "";
  return new Date(ts).toISOString().slice(0, 10);
}

/** Encode a string as base64url. */
export function b64url(s: string): string {
  return btoa(s).replace(/\+/g, "-").replace(/\//g, "_").replace(/=+$/, "");
}

/** Compute HMAC-SHA256 of data with secret, returning base64url. */
export async function hmac(secret: string, data: string): Promise<string> {
  const key = await crypto.subtle.importKey(
    "raw",
    new TextEncoder().encode(secret),
    { name: "HMAC", hash: "SHA-256" },
    false,
    ["sign"],
  );
  const sig = await crypto.subtle.sign("HMAC", key, new TextEncoder().encode(data));
  return b64url(String.fromCharCode(...new Uint8Array(sig)));
}

/** Render the logged-in/out user widget for page headers. */
export function userWidget(login: string | null): string {
  if (login) {
    return `<div class="header-user">
      <span>👤 ${esc(login)}</span>
      <a class="logout-btn" href="/auth/logout">Log out</a>
    </div>`;
  }
  return `<div class="header-user">
    <a class="login-btn" href="/auth/login">Log in with GitHub</a>
  </div>`;
}
