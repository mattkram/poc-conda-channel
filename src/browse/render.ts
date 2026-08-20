import type { Env, BrowseRecord, BuildEntry } from "../types.js";
import { esc } from "../utils.js";

export const PAGE_SIZE = 25;

export async function loadChannelSubdirs(channel: string, env: Env): Promise<string[]> {
  const { results } = await env.DB.prepare(
    `SELECT subdirs FROM packages WHERE channel = ?`,
  )
    .bind(channel)
    .all<{ subdirs: string }>();
  const set = new Set<string>();
  for (const r of results) {
    for (const s of JSON.parse(r.subdirs ?? "[]") as string[]) set.add(s);
  }
  return [...set].sort();
}

export async function loadBrowseIndex(
  channel: string,
  env: Env,
  q?: string,
  sort?: string,
  subdir?: string,
): Promise<BrowseRecord[]> {
  const sortCol = sort === "name-desc" ? "name DESC" : "name ASC";
  let stmt: D1PreparedStatement;
  const subdirParam = subdir ? `%"${subdir}"%` : undefined;

  if (q) {
    stmt = env.DB.prepare(
      `SELECT p.name, p.version, p.summary, p.license, p.home, p.subdirs
       FROM packages_fts f
       JOIN packages p ON p.rowid = f.rowid
       WHERE p.channel = ? AND packages_fts MATCH ?${subdir ? " AND subdirs LIKE ?" : ""}
       ORDER BY p.${sortCol}`,
    ).bind(channel, `"${q.replace(/"/g, '""')}"*`, ...(subdirParam ? [subdirParam] : []));
  } else {
    stmt = env.DB.prepare(
      `SELECT name, version, summary, license, home, subdirs
       FROM packages WHERE channel = ?${subdir ? " AND subdirs LIKE ?" : ""} ORDER BY ${sortCol}`,
    ).bind(channel, ...(subdirParam ? [subdirParam] : []));
  }

  const { results } = await stmt.all<{
    name: string;
    version: string;
    summary: string;
    license: string;
    home: string;
    subdirs: string;
  }>();
  return results.map((r) => ({ ...r, subdirs: JSON.parse(r.subdirs ?? "[]") as string[] }));
}

export async function loadBuildsFromRepodata(
  channel: string,
  name: string,
  subdirs: string[],
  env: Env,
): Promise<BuildEntry[]> {
  const results = await Promise.all(
    subdirs.map(async (subdir) => {
      const obj = await env.CHANNEL_BUCKET.get(`${channel}/${subdir}/repodata.json`);
      if (!obj) return [];
      const rd = await obj.json<{
        packages?: Record<string, any>;
        "packages.conda"?: Record<string, any>;
      }>();
      const entries: BuildEntry[] = [];
      for (const [fn, meta] of Object.entries({
        ...(rd.packages ?? {}),
        ...(rd["packages.conda"] ?? {}),
      })) {
        if (meta.name === name) {
          entries.push({
            subdir,
            filename: fn,
            version: meta.version ?? "",
            build: meta.build ?? "",
            timestamp: meta.timestamp,
            size: meta.size,
            md5: meta.md5,
            sha256: meta.sha256,
            depends: meta.depends ?? [],
          });
        }
      }
      return entries;
    }),
  );
  return results.flat();
}

export function renderResults(
  channel: string,
  records: BrowseRecord[],
  q: string,
  sort: string,
  page: number,
  activeSubdir?: string,
  allSubdirs?: string[],
): string {
  const total = records.length;
  const pages = Math.max(1, Math.ceil(total / PAGE_SIZE));
  const cur = Math.min(Math.max(1, page), pages);
  const slice = records.slice((cur - 1) * PAGE_SIZE, cur * PAGE_SIZE);

  const rows = slice.length
    ? slice
        .map(
          (r) => `
      <div class="pkg">
        <a class="name" href="/channels/${channel}/package/${encodeURIComponent(r.name)}">${esc(r.name)}</a>
        <span class="ver">${esc(r.version)}</span>
        ${r.summary ? `<div class="summary">${esc(r.summary)}</div>` : ""}
        <div class="meta">
          ${r.license ? `<span>${esc(r.license)}</span>` : ""}
          ${(r.subdirs ?? []).map((s) => `<span class="badge">${esc(s)}</span>`).join(" ")}
        </div>
      </div>`,
        )
        .join("")
    : `<div class="empty">No packages${q ? ` match &ldquo;${esc(q)}&rdquo;` : ""}${activeSubdir ? ` in <strong>${esc(activeSubdir)}</strong>` : ""}.</div>`;

  const canonicalQs = (p: number, sd?: string) => {
    const params = new URLSearchParams();
    if (q) params.set("q", q);
    if (sort !== "name-asc") params.set("sort", sort);
    if (sd) params.set("subdir", sd);
    if (p > 1) params.set("page", String(p));
    const s = params.toString();
    return s ? "?" + s : "";
  };
  const resultsQs = (p: number) =>
    `?q=${encodeURIComponent(q)}&sort=${encodeURIComponent(sort)}&page=${p}${activeSubdir ? `&subdir=${encodeURIComponent(activeSubdir)}` : ""}`;

  const subdirBar =
    allSubdirs && allSubdirs.length > 1
      ? `
    <div class="subdir-bar">
      <a class="subdir-pill${!activeSubdir ? " active" : ""}" href="/channels/${channel}${canonicalQs(1)}">All</a>
      ${allSubdirs.map((s) => `<a class="subdir-pill${activeSubdir === s ? " active" : ""}" href="/channels/${channel}${canonicalQs(1, s)}">${esc(s)}</a>`).join("")}
    </div>`
      : "";

  const pager =
    pages > 1
      ? `
    <div class="pager">
      ${cur > 1 ? `<a href="/channels/${channel}${canonicalQs(cur - 1, activeSubdir)}" hx-get="/channels/${channel}/results${resultsQs(cur - 1)}" hx-target="#results" hx-push-url="/channels/${channel}${canonicalQs(cur - 1, activeSubdir)}">&lsaquo; Prev</a>` : ""}
      <span class="cur">${cur} / ${pages}</span>
      ${cur < pages ? `<a href="/channels/${channel}${canonicalQs(cur + 1, activeSubdir)}" hx-get="/channels/${channel}/results${resultsQs(cur + 1)}" hx-target="#results" hx-push-url="/channels/${channel}${canonicalQs(cur + 1, activeSubdir)}">Next &rsaquo;</a>` : ""}
    </div>`
      : "";

  return `${subdirBar}<div class="count">${total} package${total === 1 ? "" : "s"}${activeSubdir ? ` in <strong>${esc(activeSubdir)}</strong>` : ""}</div>${rows}${pager}`;
}
