// ---------------------------------------------------------------------------
// CSS constants and shared HTML rendering utilities.
// ---------------------------------------------------------------------------

export const PKG_DETAIL_CSS = `
  .pkg-hero { background:#fff; border:1px solid #e4e7eb; border-radius:10px; padding:24px 28px; margin-bottom:24px; }
  .pkg-title { display:flex; align-items:baseline; gap:12px; margin-bottom:8px; }
  .pkg-title h1 { margin:0; font-size:24px; }
  .ver-badge { background:#2d7a1f; color:#fff; border-radius:6px; padding:3px 10px; font-size:14px; font-weight:600; }
  .pkg-summary { color:#3d4f5c; font-size:15px; margin:8px 0 14px; }
  .pkg-attrs { display:flex; flex-direction:column; gap:6px; font-size:13px; }
  .attr { display:flex; gap:8px; align-items:baseline; flex-wrap:wrap; }
  .attr-label { font-weight:600; color:#52606d; min-width:60px; }
  .detail-section { margin-bottom:28px; }
  .detail-section h2 { font-size:16px; font-weight:700; margin:0 0 12px; color:#1f2933; display:flex; align-items:center; gap:8px; }
  .ver-count { font-size:12px; font-weight:400; color:#52606d; }
  .install-block { display:flex; align-items:center; gap:12px; background:#f0f2f5; border-radius:6px; padding:12px 16px; flex-wrap:wrap; }
  .install-block code { flex:1; font-size:13px; background:none; padding:0; user-select:all; }
  .copy-btn { padding:6px 14px; background:#2d7a1f; color:#fff; border:none; border-radius:6px; font-size:13px; cursor:pointer; white-space:nowrap; }
  .copy-btn:hover { background:#246018; }
  details { border:1px solid #e4e7eb; border-radius:8px; margin-bottom:8px; overflow:hidden; }
  summary.ver-summary { display:flex; align-items:center; gap:10px; padding:12px 16px; cursor:pointer; background:#fff; list-style:none; user-select:none; }
  summary.ver-summary::-webkit-details-marker { display:none; }
  details[open] summary.ver-summary { border-bottom:1px solid #e4e7eb; }
  summary.ver-summary::before { content:"▶"; font-size:10px; color:#9aacb8; transition:transform .15s; flex-shrink:0; }
  details[open] summary.ver-summary::before { transform:rotate(90deg); }
  .ver-num { font-size:15px; font-weight:700; color:#1f6f18; }
  .ver-subdirs { display:flex; gap:4px; flex-wrap:wrap; margin-left:auto; }
  .files-table { width:100%; border-collapse:collapse; font-size:13px; }
  .files-table th { text-align:left; padding:8px 12px; background:#f5f7fa; color:#52606d; font-weight:600; border-bottom:1px solid #e4e7eb; }
  .files-table td { padding:8px 12px; border-bottom:1px solid #f0f2f5; vertical-align:middle; }
  .files-table tr:last-child td { border-bottom:none; }
  .files-table tr:hover td { background:#fafbfc; }
  a.dl-link { color:#1f6f18; text-decoration:none; font-weight:500; }
  a.dl-link:hover { text-decoration:underline; }
  .num { color:#52606d; white-space:nowrap; }
  .mono { font-family:monospace; }
  .deps-list { list-style:none; padding:0; margin:0; display:flex; flex-wrap:wrap; gap:6px; }
  .deps-list li code { font-size:12px; }
`;

export const BROWSE_CSS = `
  * { box-sizing: border-box; }
  body { margin: 0; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; color: #1f2933; background: #f5f7fa; }
  header { background: #fff; border-bottom: 1px solid #e4e7eb; padding: 16px 24px; display: flex; align-items: center; gap: 8px; }
  header .brand { font-weight: 700; font-size: 18px; color: #2d7a1f; text-decoration: none; }
  header .chan-ns { color: #2d7a1f; font-size: 14px; font-weight: 600; text-decoration: none; }
  header .chan-ns:hover { text-decoration: underline; }
  header .chan-sep { color: #9aacb8; font-size: 14px; padding: 0 2px; }
  header .chan { color: #3d4f5c; font-size: 14px; font-weight: 600; }
  .header-user { margin-left: auto; display: flex; align-items: center; gap: 10px; font-size: 13px; color: #3d4f5c; }
  .header-user a.login-btn { padding: 5px 14px; background: #2d7a1f; color: #fff; border-radius: 6px; text-decoration: none; font-weight: 600; }
  .header-user a.login-btn:hover { background: #246018; }
  .header-user a.logout-btn { color: #52606d; text-decoration: none; }
  .header-user a.logout-btn:hover { text-decoration: underline; }
  .wrap { max-width: 960px; margin: 0 auto; padding: 24px; }
  .controls { display: flex; gap: 12px; margin-bottom: 20px; flex-wrap: wrap; align-items: center; }
  .controls label.sr-only { position:absolute; width:1px; height:1px; padding:0; margin:-1px; overflow:hidden; clip:rect(0,0,0,0); white-space:nowrap; border:0; }
  .controls input[type=search] { flex: 1 1 320px; padding: 10px 14px; border: 1px solid #cbd2d9; border-radius: 6px; font-size: 15px; }
  .controls select { padding: 10px 12px; border: 1px solid #cbd2d9; border-radius: 6px; font-size: 14px; background: #fff; }
  .count { color: #3d4f5c; font-size: 13px; margin-bottom: 12px; }
  .pkg { background: #fff; border: 1px solid #e4e7eb; border-radius: 8px; padding: 16px 18px; margin-bottom: 10px; }
  .pkg:hover { border-color: #2d7a1f; }
  .pkg a.name { font-size: 16px; font-weight: 600; color: #1f6f18; text-decoration: none; }
  .pkg .ver { color: #52606d; font-size: 13px; margin-left: 8px; }
  .pkg .summary { color: #3d4f5c; font-size: 14px; margin: 6px 0 8px; }
  .pkg .meta { display: flex; gap: 14px; flex-wrap: wrap; font-size: 12px; color: #3d4f5c; }
  .pkg .badge { background: #c8eac2; color: #1a5c12; border-radius: 4px; padding: 2px 8px; font-size: 12px; }
  .pager { display: flex; gap: 8px; align-items: center; margin-top: 20px; }
  .pager a, .pager span { padding: 6px 12px; border: 1px solid #cbd2d9; border-radius: 6px; text-decoration: none; color: #1f2933; font-size: 14px; cursor: pointer; }
  .pager .cur { background: #2d7a1f; color: #fff; border-color: #2d7a1f; }
  .empty { color: #3d4f5c; padding: 40px; text-align: center; }
  code { background: #f0f2f5; padding: 2px 6px; border-radius: 4px; font-size: 13px; }
  .lock-badge { background: #fdecea; color: #b42318; border-radius: 4px; padding: 2px 8px; font-size: 12px; margin-left: 4px; }
  .subdir-bar { display:flex; gap:6px; flex-wrap:wrap; margin-bottom:14px; }
  .subdir-pill { padding:4px 12px; border-radius:20px; border:1px solid #cbd2d9; font-size:12px; font-weight:600; color:#3d4f5c; text-decoration:none; background:#fff; }
  .subdir-pill:hover { border-color:#2d7a1f; color:#2d7a1f; }
  .subdir-pill.active { background:#2d7a1f; color:#fff; border-color:#2d7a1f; }
`;

export const HERO_CSS = `
  .hero { background:#fff; border-bottom:1px solid #e4e7eb; padding:64px 24px 48px; text-align:center; }
  .hero h1 { margin:0 0 8px; font-size:32px; font-weight:800; color:#1f2933; letter-spacing:-0.5px; }
  .hero p { margin:0 0 32px; color:#52606d; font-size:16px; }
  .hero-search-wrap { max-width:640px; margin:0 auto; display:flex; gap:0; box-shadow:0 2px 12px rgba(0,0,0,.1); border-radius:10px; overflow:hidden; }
  .hero-search-wrap input[type=search] { flex:1; padding:16px 20px; border:none; font-size:16px; outline:none; color:#1f2933; min-width:0; }
  .hero-search-wrap button { padding:0 28px; background:#2d7a1f; color:#fff; border:none; font-size:15px; font-weight:700; cursor:pointer; white-space:nowrap; }
  .hero-search-wrap button:hover { background:#246018; }
  #search-results { max-width:960px; margin:0 auto; padding:0 24px; }
  .results-table { width:100%; border-collapse:collapse; font-size:14px; margin-top:8px; }
  .results-table th { text-align:left; padding:8px 12px; color:#52606d; font-size:12px; font-weight:600; text-transform:uppercase; letter-spacing:.04em; border-bottom:2px solid #e4e7eb; }
  .results-table td { padding:10px 12px; border-bottom:1px solid #f0f2f5; vertical-align:middle; }
  .results-table tr:last-child td { border-bottom:none; }
  .results-table tr:hover td { background:#fafbfc; }
  .results-table a.pkg-link { font-weight:600; color:#1f6f18; text-decoration:none; font-size:15px; }
  .results-table a.pkg-link:hover { text-decoration:underline; }
  .results-table .pkg-summary { color:#52606d; font-size:13px; }
  .results-table .chan-link { color:#9aacb8; font-size:12px; text-decoration:none; }
  .results-table .chan-link:hover { color:#2d7a1f; text-decoration:underline; }
  .results-count { color:#52606d; font-size:13px; padding:12px 0 4px; }
  .channels-section { max-width:960px; margin:32px auto 0; padding:0 24px 48px; }
  .channels-section h2 { font-size:16px; font-weight:700; color:#52606d; text-transform:uppercase; letter-spacing:.06em; margin:0 0 14px; }
`;
