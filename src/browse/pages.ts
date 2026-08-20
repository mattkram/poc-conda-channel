import { getContainer } from "@cloudflare/containers";
import type { Env, TrustedPublisherRow } from "../types.js";
import { esc, channelNamespace, fmtBytes, fmtDate, userWidget } from "../utils.js";
import { resolveLogin } from "../auth.js";
import {
  CHANNEL_NAME_RE,
  checkReadAccess,
  checkChannelAccess,
  requireChannelOwner,
  browseAuth,
} from "../channel.js";
import { verifyUploadToken } from "../auth.js";
import { validateChannelAndFilename } from "../upload.js";
import { BROWSE_CSS, PKG_DETAIL_CSS, HERO_CSS } from "./ui.js";
import {
  loadBrowseIndex,
  loadChannelSubdirs,
  loadBuildsFromRepodata,
  renderResults,
} from "./render.js";

// ---------------------------------------------------------------------------
// Homepage
// ---------------------------------------------------------------------------

export async function handleSearchResults(
  request: Request,
  url: URL,
  env: Env,
): Promise<Response> {
  const login = await resolveLogin(request, env.UPLOAD_TOKEN_SECRET);
  const q = url.searchParams.get("q")?.trim() ?? "";

  if (!q) {
    return new Response("", {
      headers: { "content-type": "text/html;charset=utf-8", "HX-Push-Url": "/" },
    });
  }

  const { results: channels } = await env.DB.prepare(
    `SELECT name, owner, visibility FROM channels`,
  ).all<{ name: string; owner: string | null; visibility: string }>();
  const visible = new Set(
    channels.filter((c) => c.visibility === "public" || c.owner === login).map((c) => c.name),
  );

  const { results } = await env.DB.prepare(
    `SELECT p.channel, p.name, p.version, p.summary, p.subdirs
     FROM packages_fts f
     JOIN packages p ON p.rowid = f.rowid
     WHERE packages_fts MATCH ?
     ORDER BY p.name ASC
     LIMIT 50`,
  )
    .bind(`"${q.replace(/"/g, '""')}"*`)
    .all<{
      channel: string;
      name: string;
      version: string;
      summary: string;
      subdirs: string;
    }>();

  const rows = results.filter((r) => visible.has(r.channel));

  if (rows.length === 0) {
    return new Response(
      `<div class="results-count">No packages match &ldquo;${esc(q)}&rdquo;.</div>`,
      { headers: { "content-type": "text/html;charset=utf-8" } },
    );
  }

  const tableRows = rows
    .map((r) => {
      const subdirs: string[] = JSON.parse(r.subdirs ?? "[]");
      const ns = channelNamespace(r.channel);
      const chanDisplay = ns
        ? `${esc(ns)}/${esc(r.channel.slice(ns.length + 1))}`
        : esc(r.channel);
      return `<tr>
      <td><a class="pkg-link" href="/channels/${esc(r.channel)}/package/${encodeURIComponent(r.name)}">${esc(r.name)}</a></td>
      <td class="pkg-summary">${esc(r.summary ?? "")}</td>
      <td>${subdirs.map((s) => `<span class="badge">${esc(s)}</span>`).join(" ")}</td>
      <td><a class="chan-link" href="/channels/${esc(r.channel)}">${chanDisplay}</a></td>
      <td style="color:#52606d;font-size:13px">${esc(r.version)}</td>
    </tr>`;
    })
    .join("");

  const html = `
    <div class="results-count">${rows.length} result${rows.length === 1 ? "" : "s"}</div>
    <table class="results-table">
      <thead><tr><th>Package</th><th>Summary</th><th>Platforms</th><th>Channel</th><th>Version</th></tr></thead>
      <tbody>${tableRows}</tbody>
    </table>`;

  return new Response(html, {
    headers: {
      "content-type": "text/html;charset=utf-8",
      "HX-Push-Url": q ? `/search?q=${encodeURIComponent(q)}` : "/",
    },
  });
}

export async function handleHomepage(request: Request, url: URL, env: Env): Promise<Response> {
  const login = await resolveLogin(request, env.UPLOAD_TOKEN_SECRET);
  const q = url.searchParams.get("q")?.trim() ?? "";

  const { results } = await env.DB.prepare(
    `SELECT name, owner, visibility FROM channels ORDER BY name`,
  ).all<{ name: string; owner: string | null; visibility: string }>();
  const visible = results.filter((c) => c.visibility === "public" || c.owner === login);

  const chanCards = visible
    .map((ch) => {
      const ns = channelNamespace(ch.name);
      const displayName = ns
        ? `<span style="color:#9aacb8">${esc(ns)}/</span>${esc(ch.name.slice(ns.length + 1))}`
        : esc(ch.name);
      const lock =
        ch.visibility === "private" ? ' <span class="lock-badge">🔒 private</span>' : "";
      return `<div class="pkg">
      <a class="name" href="/channels/${ch.name}">${displayName}</a>${lock}
      <div class="meta">${ch.owner ? `<span>owner: ${esc(ch.owner)}</span>` : ""}<span>conda channel</span></div>
    </div>`;
    })
    .join("");

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="description" content="Search and browse conda packages across all channels.">
<title>conda-wit</title>
<script src="https://unpkg.com/htmx.org@1.9.12" defer></script>
<style>${BROWSE_CSS}${HERO_CSS}</style>
</head>
<body>
<header>
  <a class="brand" href="/">conda-wit</a>
  <span class="chan-sep">/</span><a class="chan" href="/channels" style="text-decoration:none">channels</a>
  ${userWidget(login)}
</header>

<div class="hero">
  <h1>Find conda packages</h1>
  <p>Search across all public channels hosted on this server.</p>
  <form class="hero-search-wrap"
        hx-get="/search/results"
        hx-trigger="input changed delay:300ms from:input[name='q'], submit"
        hx-target="#search-results"
        action="/search" method="GET">
    <input type="search" name="q" value="${esc(q)}"
           placeholder="Search packages&hellip;"
           autocomplete="off" autofocus
           aria-label="Search packages">
    <button type="submit">Search</button>
  </form>
</div>

<div id="search-results">${q ? `<div style="text-align:center;padding:32px;color:#9aacb8">Loading…</div>
<script>htmx.ajax('GET','/search/results?q=${encodeURIComponent(q)}',{target:'#search-results'})</script>` : ""}</div>

<div class="channels-section" id="channel-list"${q ? ' style="display:none"' : ""}>
  <h2>${visible.length} channel${visible.length === 1 ? "" : "s"}</h2>
  ${chanCards || `<div class="empty">No channels yet.</div>`}
</div>

<script>
document.addEventListener('htmx:afterSettle', () => {
  const q = document.querySelector('input[name="q"]')?.value?.trim();
  const cl = document.getElementById('channel-list');
  if (cl) cl.style.display = q ? 'none' : '';
});
document.querySelector('input[name="q"]')?.addEventListener('input', function() {
  const cl = document.getElementById('channel-list');
  if (cl) cl.style.display = this.value.trim() ? 'none' : '';
});
</script>
</body>
</html>`;
  return new Response(html, { headers: { "content-type": "text/html;charset=utf-8" } });
}

export async function handleGlobalSearch(request: Request, url: URL, env: Env): Promise<Response> {
  const login = await resolveLogin(request, env.UPLOAD_TOKEN_SECRET);
  const q = url.searchParams.get("q")?.trim() ?? "";

  let rows: Array<{
    channel: string;
    name: string;
    version: string;
    summary: string;
    subdirs: string;
  }> = [];
  if (q) {
    const { results: channels } = await env.DB.prepare(
      `SELECT name, owner, visibility FROM channels`,
    ).all<{ name: string; owner: string | null; visibility: string }>();
    const visible = new Set(
      channels.filter((c) => c.visibility === "public" || c.owner === login).map((c) => c.name),
    );
    const { results } = await env.DB.prepare(
      `SELECT p.channel, p.name, p.version, p.summary, p.subdirs
       FROM packages_fts f
       JOIN packages p ON p.rowid = f.rowid
       WHERE packages_fts MATCH ?
       ORDER BY p.name ASC
       LIMIT 200`,
    )
      .bind(`"${q.replace(/"/g, '""')}"*`)
      .all<{
        channel: string;
        name: string;
        version: string;
        summary: string;
        subdirs: string;
      }>();
    rows = results.filter((r) => visible.has(r.channel));
  }

  const resultCards = rows
    .map((r) => {
      const subdirs: string[] = JSON.parse(r.subdirs ?? "[]");
      const ns = channelNamespace(r.channel);
      const chanDisplay = ns
        ? `<span style="color:#9aacb8">${esc(ns)}/</span>${esc(r.channel.slice(ns.length + 1))}`
        : esc(r.channel);
      return `
      <div class="pkg">
        <a class="name" href="/channels/${esc(r.channel)}/package/${encodeURIComponent(r.name)}">${esc(r.name)}</a>
        <span class="ver">${esc(r.version)}</span>
        <span class="ver" style="margin-left:6px">in <a href="/channels/${esc(r.channel)}" style="color:#52606d">${chanDisplay}</a></span>
        ${r.summary ? `<div class="summary">${esc(r.summary)}</div>` : ""}
        <div class="meta">${subdirs.map((s) => `<span class="badge">${esc(s)}</span>`).join(" ")}</div>
      </div>`;
    })
    .join("");

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${q ? `${esc(q)} &middot; ` : ""}Search &middot; conda-wit</title>
<style>${BROWSE_CSS}</style>
</head>
<body>
<header>
  <a class="brand" href="/">conda-wit</a>
  <span class="chan-sep">/</span><span class="chan">search</span>
  ${userWidget(login)}
</header>
<main>
<div class="wrap">
  <form method="GET" action="/search" class="controls" style="margin-bottom:20px">
    <label class="sr-only" for="global-search">Search all packages</label>
    <input id="global-search" type="search" name="q" placeholder="Search all packages&hellip;" value="${esc(q)}" autocomplete="off" autofocus style="flex:1 1 400px">
    <button type="submit" style="padding:10px 20px;background:#2d7a1f;color:#fff;border:none;border-radius:6px;font-size:14px;font-weight:600;cursor:pointer">Search</button>
  </form>
  ${q ? `<div class="count">${rows.length} result${rows.length === 1 ? "" : "s"} for &ldquo;${esc(q)}&rdquo;</div>
  ${resultCards || `<div class="empty">No packages match &ldquo;${esc(q)}&rdquo; across any public channel.</div>`}` : `<div class="empty" style="padding-top:60px">Enter a package name to search across all public channels.</div>`}
</div>
</main>
</body>
</html>`;
  return new Response(html, { headers: { "content-type": "text/html;charset=utf-8" } });
}

// ---------------------------------------------------------------------------
// Channels index
// ---------------------------------------------------------------------------

export async function handleChannelsIndex(request: Request, env: Env): Promise<Response> {
  const login = await resolveLogin(request, env.UPLOAD_TOKEN_SECRET);
  const { results } = await env.DB.prepare(
    `SELECT name, owner, visibility FROM channels ORDER BY name`,
  ).all<{ name: string; owner: string | null; visibility: string }>();

  const visible = results.filter((ch) => ch.visibility === "public" || ch.owner === login);

  const subdirsByChannel = new Map<string, Set<string>>();
  if (visible.length > 0) {
    const placeholders = visible.map(() => "?").join(",");
    const { results: pkgRows } = await env.DB.prepare(
      `SELECT channel, subdirs FROM packages WHERE channel IN (${placeholders})`,
    )
      .bind(...visible.map((c) => c.name))
      .all<{ channel: string; subdirs: string }>();
    for (const r of pkgRows) {
      if (!subdirsByChannel.has(r.channel)) subdirsByChannel.set(r.channel, new Set());
      for (const s of JSON.parse(r.subdirs ?? "[]") as string[]) {
        subdirsByChannel.get(r.channel)!.add(s);
      }
    }
  }

  const cards = visible.map((ch) => {
    const isPrivate = ch.visibility === "private";
    const lock = isPrivate ? ' <span class="lock-badge">🔒 private</span>' : "";
    const ns = channelNamespace(ch.name);
    const displayName = ns
      ? `<span style="color:#9aacb8">${esc(ns)}/</span>${esc(ch.name.slice(ns.length + 1))}`
      : esc(ch.name);
    const subdirs = [...(subdirsByChannel.get(ch.name) ?? [])].sort();
    const subdirBadges = subdirs.map((s) => `<span class="badge">${esc(s)}</span>`).join(" ");
    return `
      <div class="pkg">
        <a class="name" href="/channels/${ch.name}">${displayName}</a>${lock}
        <div class="meta">
          ${ch.owner ? `<span>owner: ${esc(ch.owner)}</span>` : ""}
          <span>conda channel</span>
          ${subdirBadges}
        </div>
      </div>`;
  });

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="description" content="Browse all conda channels hosted on this server.">
<title>Channels &middot; conda-wit</title>
<style>${BROWSE_CSS}</style>
</head>
<body>
<header>
  <a class="brand" href="/">conda-wit</a>
  <span class="chan-sep">/</span><span class="chan">channels</span>
  ${userWidget(login)}
</header>
<main>
<div class="wrap">
  <form method="GET" action="/search" class="controls">
    <label class="sr-only" for="global-search">Search all packages</label>
    <input id="global-search" type="search" name="q" placeholder="Search packages across all channels&hellip;" autocomplete="off">
    <button type="submit" style="padding:10px 20px;background:#2d7a1f;color:#fff;border:none;border-radius:6px;font-size:14px;font-weight:600;cursor:pointer">Search</button>
  </form>
  <div class="count">${visible.length} channel${visible.length === 1 ? "" : "s"}</div>
  ${cards.join("") || `<div class="empty">No channels yet.</div>`}
</div>
</main>
</body>
</html>`;
  return new Response(html, { headers: { "content-type": "text/html;charset=utf-8" } });
}

export async function handleNamespacePage(
  request: Request,
  namespace: string,
  channels: Array<{ name: string; owner: string | null; visibility: string }>,
  env: Env,
): Promise<Response> {
  const login = await resolveLogin(request, env.UPLOAD_TOKEN_SECRET);
  const nsChannels = channels
    .filter((ch) => ch.name.startsWith(`${namespace}/`))
    .filter((ch) => ch.visibility === "public" || ch.owner === login);

  const cards = nsChannels.map((ch) => {
    const short = ch.name.slice(namespace.length + 1);
    const lock =
      ch.visibility === "private" ? ' <span class="lock-badge">🔒 private</span>' : "";
    return `
      <div class="pkg">
        <a class="name" href="/channels/${ch.name}">${esc(short)}</a>${lock}
        <div class="meta"><span>conda channel</span></div>
      </div>`;
  });

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="description" content="Channels owned by ${esc(namespace)}.">
<title>${esc(namespace)} &middot; channels</title>
<style>${BROWSE_CSS}</style>
</head>
<body>
<header>
  <a class="brand" href="/">conda-wit</a>
  <span class="chan-sep">/</span>
  <span class="chan">${esc(namespace)}</span>
  ${userWidget(login)}
</header>
<main>
<div class="wrap">
  <div class="count">${nsChannels.length} channel${nsChannels.length === 1 ? "" : "s"}</div>
  ${cards.join("") || `<div class="empty">No channels in this namespace.</div>`}
</div>
</main>
</body>
</html>`;
  return new Response(html, { headers: { "content-type": "text/html;charset=utf-8" } });
}

// ---------------------------------------------------------------------------
// Channel browse (package list)
// ---------------------------------------------------------------------------

export async function handleBrowseResults(
  request: Request,
  channel: string,
  url: URL,
  env: Env,
): Promise<Response> {
  const denied = await browseAuth(request, channel, env);
  if (denied) return denied;
  const q = url.searchParams.get("q") ?? "";
  const sort = url.searchParams.get("sort") ?? "name-asc";
  const page = parseInt(url.searchParams.get("page") ?? "1", 10) || 1;
  const subdir = url.searchParams.get("subdir") ?? undefined;
  const [records, allSubdirs] = await Promise.all([
    loadBrowseIndex(channel, env, q, sort, subdir),
    loadChannelSubdirs(channel, env),
  ]);

  const canonicalParams = new URLSearchParams();
  if (q) canonicalParams.set("q", q);
  if (sort !== "name-asc") canonicalParams.set("sort", sort);
  if (subdir) canonicalParams.set("subdir", subdir);
  if (page > 1) canonicalParams.set("page", String(page));
  const canonicalSearch = canonicalParams.toString();
  const pushUrl = `/channels/${channel}${canonicalSearch ? "?" + canonicalSearch : ""}`;

  return new Response(renderResults(channel, records, q, sort, page, subdir, allSubdirs), {
    headers: { "content-type": "text/html;charset=utf-8", "HX-Push-Url": pushUrl },
  });
}

export async function handleBrowsePage(
  request: Request,
  channel: string,
  url: URL,
  env: Env,
): Promise<Response> {
  const denied = await browseAuth(request, channel, env);
  if (denied) return denied;
  const login = await resolveLogin(request, env.UPLOAD_TOKEN_SECRET);
  const q = url.searchParams.get("q") ?? "";
  const sort = url.searchParams.get("sort") ?? "name-asc";
  const page = parseInt(url.searchParams.get("page") ?? "1", 10) || 1;
  const subdir = url.searchParams.get("subdir") ?? undefined;

  const [records, allSubdirs, chanRow] = await Promise.all([
    loadBrowseIndex(channel, env, q, sort, subdir),
    loadChannelSubdirs(channel, env),
    env.DB.prepare(`SELECT owner FROM channels WHERE name = ?`)
      .bind(channel)
      .first<{ owner: string | null }>(),
  ]);
  const results = renderResults(channel, records, q, sort, page, subdir, allSubdirs);
  const isOwner = !!login && login === chanRow?.owner;

  const ns = channelNamespace(channel);
  const channelHeader = ns
    ? `<a class="chan-ns" href="/channels/${esc(ns)}">${esc(ns)}</a><span class="chan-sep">/</span><span class="chan">${esc(channel.slice(ns.length + 1))}</span>`
    : `<span class="chan">${esc(channel)}</span>`;

  const adminLink = isOwner
    ? `<a class="admin-btn" href="/channels/${esc(channel)}/admin">&#9881; Settings</a>`
    : "";

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="description" content="Browse packages in the ${esc(channel)} conda channel. Search, filter, and install packages.">
<title>${esc(channel)} &middot; packages</title>
<script src="https://unpkg.com/htmx.org@1.9.12" defer></script>
<style>${BROWSE_CSS}
  .header-user a.admin-btn { color: #52606d; text-decoration: none; font-size: 13px; }
  .header-user a.admin-btn:hover { text-decoration: underline; }
</style>
</head>
<body>
<header>
  <a class="brand" href="/">conda-wit</a>
  ${channelHeader}
  <div class="header-user">
    ${adminLink}
    ${login ? `<span>👤 ${esc(login)}</span><a class="logout-btn" href="/auth/logout">Log out</a>` : `<a class="login-btn" href="/auth/login">Log in with GitHub</a>`}
  </div>
</header>
<main>
<div class="wrap">
  <form class="controls" hx-get="/channels/${channel}/results" hx-target="#results" hx-trigger="input changed delay:250ms from:input[name='q'], change from:select">
    <label class="sr-only" for="pkg-search">Search packages</label>
    <input id="pkg-search" type="search" name="q" placeholder="Search packages&hellip;" value="${esc(q)}" autocomplete="off">
    <label class="sr-only" for="pkg-sort">Sort by</label>
    <select id="pkg-sort" name="sort" aria-label="Sort packages">
      <option value="name-asc"${sort === "name-asc" ? " selected" : ""}>Name A&rarr;Z</option>
      <option value="name-desc"${sort === "name-desc" ? " selected" : ""}>Name Z&rarr;A</option>
    </select>
  </form>
  <div id="results">${results}</div>
</div>
</main>
</body>
</html>`;
  return new Response(html, { headers: { "content-type": "text/html;charset=utf-8" } });
}

// ---------------------------------------------------------------------------
// Package detail
// ---------------------------------------------------------------------------

export async function handleBrowsePackage(
  request: Request,
  channel: string,
  name: string,
  env: Env,
): Promise<Response> {
  const denied = await browseAuth(request, channel, env);
  if (denied) return denied;
  const login = await resolveLogin(request, env.UPLOAD_TOKEN_SECRET);
  name = decodeURIComponent(name);

  const [row, chanRow] = await Promise.all([
    env.DB.prepare(
      `SELECT name, version, summary, license, home, subdirs FROM packages WHERE channel = ? AND name = ?`,
    )
      .bind(channel, name)
      .first<{
        name: string;
        version: string;
        summary: string;
        license: string;
        home: string;
        subdirs: string;
      }>(),
    env.DB.prepare(`SELECT owner FROM channels WHERE name = ?`)
      .bind(channel)
      .first<{ owner: string | null }>(),
  ]);
  if (!row) return new Response("package not found", { status: 404 });
  const isOwner = !!login && login === chanRow?.owner;
  const rec = { ...row, subdirs: JSON.parse(row.subdirs ?? "[]") as string[] };

  const builds = await loadBuildsFromRepodata(channel, name, rec.subdirs ?? [], env);
  builds.sort(
    (a, b) =>
      b.version.localeCompare(a.version, undefined, { numeric: true }) ||
      a.subdir.localeCompare(b.subdir) ||
      a.filename.localeCompare(b.filename),
  );

  const byVersion = new Map<string, typeof builds>();
  for (const b of builds) {
    if (!byVersion.has(b.version)) byVersion.set(b.version, []);
    byVersion.get(b.version)!.push(b);
  }

  const latestVersion = builds[0]?.version ?? rec.version;
  const latestBuilds = byVersion.get(latestVersion) ?? [];
  const latestDeps = [...new Set(latestBuilds.flatMap((b) => b.depends ?? []))].sort();

  const origin = new URL(request.url).origin;
  const installCmd = `conda install -c ${origin}/repo/${channel} ${name}`;

  const fileRow = (b: (typeof builds)[0]) => `
    <tr>
      <td><a class="dl-link" href="/repo/${channel}/${b.subdir}/${encodeURIComponent(b.filename)}" title="Download">${esc(b.filename)}</a></td>
      <td><span class="badge">${esc(b.subdir)}</span></td>
      <td>${esc(b.build)}</td>
      <td class="num">${b.size != null ? fmtBytes(b.size) : ""}</td>
      <td class="num mono" title="${b.sha256 ? `SHA256: ${b.sha256}` : ""}">${b.md5 ? b.md5.slice(0, 8) + "…" : ""}</td>
      <td class="num">${fmtDate(b.timestamp)}</td>
      ${isOwner ? `<td><button class="del-file-btn" data-channel="${esc(channel)}" data-subdir="${esc(b.subdir)}" data-filename="${esc(b.filename)}">Delete</button></td>` : ""}
    </tr>`;

  const versionGroups = [...byVersion.entries()]
    .map(
      ([ver, vbuilds], i) => `
  <details${i === 0 ? " open" : ""}>
    <summary class="ver-summary">
      <span class="ver-num">${esc(ver)}</span>
      <span class="ver-count">${vbuilds.length} file${vbuilds.length === 1 ? "" : "s"}</span>
      <span class="ver-subdirs">${[...new Set(vbuilds.map((b) => b.subdir))].map((s) => `<span class="badge">${esc(s)}</span>`).join(" ")}</span>
    </summary>
    <table class="files-table">
      <thead><tr><th>Filename</th><th>Subdir</th><th>Build</th><th>Size</th><th>MD5</th><th>Date</th>${isOwner ? "<th></th>" : ""}</tr></thead>
      <tbody>${vbuilds.map(fileRow).join("")}</tbody>
    </table>
  </details>`,
    )
    .join("");

  const depsSection = latestDeps.length
    ? `
  <section class="detail-section">
    <h2>Dependencies <span class="ver-count">(${esc(latestVersion)})</span></h2>
    <ul class="deps-list">
      ${latestDeps.map((d) => `<li><code>${esc(d)}</code></li>`).join("")}
    </ul>
  </section>`
    : "";

  const html = `<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="description" content="${esc(rec.summary || `${name} package in the ${channel} conda channel`)}">
<title>${esc(name)} &middot; ${esc(channel)}</title>
<style>${BROWSE_CSS}${PKG_DETAIL_CSS}
  .del-file-btn { background:#fff; color:#b42318; border:1px solid #f5c2bf; padding:3px 10px; border-radius:5px; font-size:12px; cursor:pointer; white-space:nowrap; }
  .del-file-btn:hover { background:#fdecea; }
</style>
</head>
<body>
<header>
  <a class="brand" href="/">conda-wit</a>
  <span class="chan-sep">/</span>
  <a class="chan-ns" href="/channels/${channel.split("/")[0]}">${esc(channel.split("/")[0])}</a>
  <span class="chan-sep">/</span>
  <a class="chan" href="/channels/${channel}">${esc(channel.split("/").slice(1).join("/"))}</a>
  <span class="chan-sep">/</span>
  <span class="chan">${esc(name)}</span>
  ${userWidget(login)}
</header>
<main>
<div class="wrap">
  <div class="pkg-hero">
    <div class="pkg-title">
      <h1>${esc(name)}</h1>
      <span class="ver-badge">${esc(latestVersion)}</span>
    </div>
    ${rec.summary ? `<p class="pkg-summary">${esc(rec.summary)}</p>` : ""}
    <div class="pkg-attrs">
      ${rec.license ? `<span class="attr"><span class="attr-label">License</span>${esc(rec.license)}</span>` : ""}
      ${rec.home ? `<span class="attr"><span class="attr-label">Home</span><a href="${esc(rec.home)}" target="_blank" rel="noopener">${esc(rec.home)}</a></span>` : ""}
      <span class="attr"><span class="attr-label">Subdirs</span>${(rec.subdirs ?? []).map((s) => `<span class="badge">${esc(s)}</span>`).join(" ")}</span>
    </div>
  </div>
  <section class="detail-section">
    <h2>Install</h2>
    <div class="install-block">
      <code id="install-cmd">${esc(installCmd)}</code>
      <button class="copy-btn" onclick="navigator.clipboard.writeText(document.getElementById('install-cmd').textContent).then(()=>{this.textContent='Copied!';setTimeout(()=>this.textContent='Copy',1500)})">Copy</button>
    </div>
  </section>
  ${depsSection}
  <section class="detail-section">
    <h2>Files <span class="ver-count">${builds.length} total across ${byVersion.size} version${byVersion.size === 1 ? "" : "s"}</span></h2>
    ${versionGroups || `<div class="empty">No files.</div>`}
  </section>
</div>
</main>
${isOwner ? `<script>
document.addEventListener('click', async (e) => {
  const btn = e.target.closest('.del-file-btn');
  if (!btn) return;
  const { channel, subdir, filename } = btn.dataset;
  if (!confirm('Delete ' + filename + ' from ' + subdir + '?\\nThis will reindex the channel.')) return;
  btn.disabled = true; btn.textContent = 'Deleting…';
  const resp = await fetch('/channel/' + channel + '/' + subdir + '/' + filename, {
    method: 'DELETE', credentials: 'same-origin'
  });
  if (resp.ok) {
    const row = btn.closest('tr');
    row.style.opacity = '0.4';
    setTimeout(() => { row.remove(); }, 400);
  } else {
    alert('Delete failed: ' + await resp.text());
    btn.disabled = false; btn.textContent = 'Delete';
  }
});
</script>` : ""}
</body>
</html>`;
  return new Response(html, { headers: { "content-type": "text/html;charset=utf-8" } });
}

// ---------------------------------------------------------------------------
// R2 read path
// ---------------------------------------------------------------------------

export async function handleR2Get(
  request: Request,
  channel: string,
  key: string,
  env: Env,
): Promise<Response> {
  const auth = request.headers.get("authorization") ?? "";
  const token = auth.replace(/^Bearer\s+/i, "");
  const claims = token ? await verifyUploadToken(token, env.UPLOAD_TOKEN_SECRET) : null;
  const denied = await checkReadAccess(channel, claims?.login ?? null, env);
  if (denied) return denied;

  const obj = await env.CHANNEL_BUCKET.get(key);
  if (!obj) return new Response("not found", { status: 404 });
  const headers = new Headers();
  obj.writeHttpMetadata(headers);
  headers.set("etag", obj.httpEtag);

  const leaf = key.split("/").pop() ?? "";
  if (leaf.endsWith(".conda") || leaf.endsWith(".tar.bz2")) {
    headers.set("cache-control", "public, max-age=31536000, immutable");
  } else if (leaf.endsWith(".msgpack.zst") || leaf.endsWith(".zst")) {
    headers.set("cache-control", "public, max-age=31536000, immutable");
  } else if (leaf === "repodata.json" || leaf === "repodata_from_packages.json") {
    headers.set("cache-control", "public, max-age=300, must-revalidate");
  } else {
    headers.set("cache-control", "public, max-age=60");
  }
  return new Response(obj.body, { headers });
}

export async function handleChannelRoot(
  request: Request,
  channel: string,
  env: Env,
): Promise<Response> {
  if (!CHANNEL_NAME_RE.test(channel)) return new Response("not found", { status: 404 });

  const auth = request.headers.get("authorization") ?? "";
  const token = auth.replace(/^Bearer\s+/i, "");
  const claims = token ? await verifyUploadToken(token, env.UPLOAD_TOKEN_SECRET) : null;
  const denied = await checkReadAccess(channel, claims?.login ?? null, env);
  if (denied) return denied;

  const subdirs = new Set<string>();
  let cursor: string | undefined;
  do {
    const list = await env.CHANNEL_BUCKET.list({ prefix: `${channel}/`, cursor, delimiter: "/" });
    for (const prefix of (list as any).delimitedPrefixes ?? []) {
      const subdir = prefix.slice(channel.length + 1, -1);
      if (subdir && !subdir.startsWith("_")) subdirs.add(subdir);
    }
    cursor = list.truncated ? list.cursor : undefined;
  } while (cursor);

  const rows = [...subdirs]
    .sort()
    .map((s) => `<li><a href="/${channel}/${s}/">${s}/</a></li>`)
    .join("\n    ");

  const html = `<!DOCTYPE html>
<html>
<head><meta charset="utf-8"><title>${channel}</title></head>
<body>
<h1>${channel}</h1>
<ul>
    ${rows || "<li><em>(empty)</em></li>"}
</ul>
</body>
</html>`;
  return new Response(html, { headers: { "content-type": "text/html;charset=utf-8" } });
}

// ---------------------------------------------------------------------------
// Package delete
// ---------------------------------------------------------------------------

export async function handleDeletePackage(
  request: Request,
  channel: string,
  subdir: string,
  filename: string,
  env: Env,
): Promise<Response> {
  const login = await resolveLogin(request, env.UPLOAD_TOKEN_SECRET);
  if (!login) return new Response("unauthorized", { status: 401 });

  const invalid = validateChannelAndFilename(channel, filename);
  if (invalid) return new Response(invalid, { status: 400 });

  const denied = await checkChannelAccess(channel, login, env);
  if (denied) return denied;

  const key = `${channel}/${subdir}/${filename}`;
  const exists = await env.CHANNEL_BUCKET.head(key);
  if (!exists) return new Response(`${key} not found`, { status: 404 });

  await env.CHANNEL_BUCKET.delete(key);

  const container = getContainer(env.INDEXER, channel);
  const resp = await container.fetch("http://container/reindex", {
    method: "POST",
    body: JSON.stringify({ channel, subdir }),
    headers: { "content-type": "application/json" },
  });
  if (!resp.ok) {
    return new Response(`deleted ${filename} but reindex failed: ${await resp.text()}`, {
      status: 500,
    });
  }

  return new Response(`deleted ${filename} and reindexed ${channel}/${subdir}`, { status: 200 });
}
