// ---------------------------------------------------------------------------
// CSS constants and shared HTML rendering utilities.
//
// Brand palette (from design sheet):
//   Primary teal:  #0B8275  (darkened from #0B8275 for WCAG AA contrast)
//   Gold accent:   #FBBF24
//   Navy:          #2563EB
//   Grey:          #6B7280
//   Font:          Quicksand (Google Fonts)
// ---------------------------------------------------------------------------

export const GOOGLE_FONTS = `<link rel="preconnect" href="https://fonts.googleapis.com">
<link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=Quicksand:wght@400;500;600;700&display=swap" rel="stylesheet" media="print" onload="this.media='all'">
<noscript><link href="https://fonts.googleapis.com/css2?family=Quicksand:wght@400;500;600;700&display=swap" rel="stylesheet"></noscript>`;

export const FAVICON_TAGS = `<link rel="icon" type="image/x-icon" href="/favicon.ico">
<link rel="icon" type="image/png" sizes="32x32" href="/favicon-32x32.png">
<link rel="icon" type="image/png" sizes="16x16" href="/favicon-16x16.png">
<link rel="apple-touch-icon" sizes="180x180" href="/apple-touch-icon.png">`;

export const PKG_DETAIL_CSS = `
  .pkg-hero { background:#fff; border:1px solid #e4e7eb; border-radius:10px; padding:24px 28px; margin-bottom:24px; }
  .pkg-title { display:flex; align-items:baseline; gap:12px; margin-bottom:8px; }
  .pkg-title h1 { margin:0; font-size:24px; color:#1e293b; }
  .ver-badge { background:#0B8275; color:#fff; border-radius:6px; padding:3px 10px; font-size:14px; font-weight:700; }
  .pkg-summary { color:#3d4f5c; font-size:15px; margin:8px 0 14px; }
  .pkg-attrs { display:flex; flex-direction:column; gap:6px; font-size:13px; }
  .attr { display:flex; gap:8px; align-items:baseline; flex-wrap:wrap; }
  .attr-label { font-weight:700; color:#6B7280; min-width:60px; }
  .detail-section { margin-bottom:28px; }
  .detail-section h2 { font-size:16px; font-weight:700; margin:0 0 12px; color:#1e293b; display:flex; align-items:center; gap:8px; }
  .ver-count { font-size:12px; font-weight:400; color:#6B7280; }
  .install-block { display:flex; align-items:center; gap:12px; background:#f0fdf9; border:1px solid #99f6e4; border-radius:6px; padding:12px 16px; flex-wrap:wrap; }
  .install-block code { flex:1; font-size:13px; background:none; padding:0; user-select:all; color:#0f766e; }
  .copy-btn { padding:6px 14px; background:#0B8275; color:#fff; border:none; border-radius:6px; font-size:13px; font-weight:700; cursor:pointer; white-space:nowrap; }
  .copy-btn:hover { background:#0f9a8a; }
  details { border:1px solid #e4e7eb; border-radius:8px; margin-bottom:8px; overflow:hidden; }
  summary.ver-summary { display:flex; align-items:center; gap:10px; padding:12px 16px; cursor:pointer; background:#fff; list-style:none; user-select:none; }
  summary.ver-summary::-webkit-details-marker { display:none; }
  details[open] summary.ver-summary { border-bottom:1px solid #e4e7eb; }
  summary.ver-summary::before { content:"▶"; font-size:10px; color:#9aacb8; transition:transform .15s; flex-shrink:0; }
  details[open] summary.ver-summary::before { transform:rotate(90deg); }
  .ver-num { font-size:15px; font-weight:700; color:#0B8275; }
  .ver-subdirs { display:flex; gap:4px; flex-wrap:wrap; margin-left:auto; }
  .ver-subdirs .badge { padding:2px 10px; border-radius:20px; border:1px solid #cbd2d9; font-size:12px; font-weight:700; color:#3d4f5c; background:#fff; }
  .files-table .badge { padding:2px 8px; border-radius:20px; border:1px solid #cbd2d9; font-size:11px; font-weight:700; color:#3d4f5c; background:#fff; }
  .files-table { width:100%; border-collapse:collapse; font-size:13px; }
  .files-table th { text-align:left; padding:8px 12px; background:#f5f7fa; color:#6B7280; font-weight:700; border-bottom:1px solid #e4e7eb; }
  .files-table td { padding:8px 12px; border-bottom:1px solid #f0f2f5; vertical-align:middle; }
  .files-table tr:last-child td { border-bottom:none; }
  .files-table tr:hover td { background:#f0fdf9; }
  a.dl-link { color:#0B8275; text-decoration:none; font-weight:600; }
  a.dl-link:hover { text-decoration:underline; }
  .num { color:#6B7280; white-space:nowrap; }
  .mono { font-family:monospace; }
  .deps-list { list-style:none; padding:0; margin:0; display:flex; flex-wrap:wrap; gap:6px; }
  .deps-list li code { font-size:12px; }
`;

export const BROWSE_CSS = `
  * { box-sizing: border-box; }
  body { margin: 0; font-family: "Quicksand", -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif; color: #1e293b; background: #f8fafc; }
  header { background: #fff; border-bottom: 1px solid #e4e7eb; padding: 12px 24px; display: flex; align-items: center; gap: 8px; }
  header .brand { font-weight: 700; font-size: 20px; color: #0B8275; text-decoration: none; letter-spacing: -0.3px; font-family: "Quicksand", sans-serif; }
  header .brand:hover { color: #0f9a8a; }
  header .chan-ns { color: #0B8275; font-size: 14px; font-weight: 600; text-decoration: none; }
  header .chan-ns:hover { text-decoration: underline; }
  header .chan-sep { color: #cbd2d9; font-size: 14px; padding: 0 2px; }
  header .chan { color: #3d4f5c; font-size: 14px; font-weight: 600; }
  .header-user { margin-left: auto; display: flex; align-items: center; gap: 10px; font-size: 13px; color: #3d4f5c; }
  .header-user a.login-btn { padding: 6px 16px; background: #0B8275; color: #fff; border-radius: 20px; text-decoration: none; font-weight: 700; font-size: 13px; transition: background .15s; }
  .header-user a.login-btn:hover { background: #0f9a8a; }
  .header-user a.logout-btn { color: #6B7280; text-decoration: none; font-size: 13px; }
  .header-user a.logout-btn:hover { color: #0B8275; text-decoration: underline; }
  .wrap { max-width: 960px; margin: 0 auto; padding: 24px; }
  .controls { display: flex; gap: 12px; margin-bottom: 20px; flex-wrap: wrap; align-items: center; }
  .controls label.sr-only { position:absolute; width:1px; height:1px; padding:0; margin:-1px; overflow:hidden; clip:rect(0,0,0,0); white-space:nowrap; border:0; }
  .controls input[type=search] { flex: 1 1 320px; padding: 10px 14px; border: 1px solid #cbd2d9; border-radius: 8px; font-size: 15px; font-family: inherit; outline: none; transition: border-color .15s; }
  .controls input[type=search]:focus { border-color: #0B8275; box-shadow: 0 0 0 3px rgba(20,187,166,.12); }
  .controls select { padding: 10px 12px; border: 1px solid #cbd2d9; border-radius: 8px; font-size: 14px; background: #fff; font-family: inherit; }
  .count { color: #6B7280; font-size: 13px; margin-bottom: 12px; font-weight: 500; }
  .pkg { background: #fff; border: 1px solid #e4e7eb; border-radius: 10px; padding: 16px 18px; margin-bottom: 10px; transition: border-color .15s, box-shadow .15s; }
  .pkg:hover { border-color: #0B8275; box-shadow: 0 2px 8px rgba(20,187,166,.1); }
  .pkg a.name { font-size: 16px; font-weight: 700; color: #0B8275; text-decoration: none; }
  .pkg a.name:hover { color: #0f9a8a; }
  .pkg .ver { color: #6B7280; font-size: 13px; margin-left: 8px; font-weight: 500; }
  .pkg .summary { color: #3d4f5c; font-size: 14px; margin: 6px 0 8px; }
  .pkg .meta { display: flex; gap: 14px; flex-wrap: wrap; font-size: 12px; color: #3d4f5c; }
  .pkg .badge { background: #ccfbf1; color: #0f766e; border-radius: 20px; padding: 2px 10px; font-size: 12px; font-weight: 600; }
  .pager { display: flex; gap: 8px; align-items: center; margin-top: 20px; }
  .pager a, .pager span { padding: 6px 12px; border: 1px solid #cbd2d9; border-radius: 8px; text-decoration: none; color: #1e293b; font-size: 14px; cursor: pointer; font-weight: 600; }
  .pager a:hover { border-color: #0B8275; color: #0B8275; }
  .pager .cur { background: #0B8275; color: #fff; border-color: #0B8275; }
  .empty { color: #6B7280; padding: 40px; text-align: center; font-weight: 500; }
  code { background: #f0fdf9; color: #0f766e; padding: 2px 6px; border-radius: 4px; font-size: 13px; }
  .lock-badge { background: #fef3c7; color: #b45309; border-radius: 20px; padding: 2px 10px; font-size: 12px; font-weight: 600; margin-left: 4px; }
  .subdir-bar { display:flex; gap:6px; flex-wrap:wrap; margin-bottom:14px; }
  .subdir-pill { padding:4px 14px; border-radius:20px; border:1px solid #cbd2d9; font-size:12px; font-weight:700; color:#3d4f5c; text-decoration:none; background:#fff; transition: all .15s; }
  .subdir-pill:hover { border-color:#0B8275; color:#0B8275; }
  .subdir-pill.active { background:#0B8275; color:#fff; border-color:#0B8275; }
`;

export const FOOTER_HTML = `<footer style="text-align:center;padding:32px 24px;color:#9aacb8;font-size:13px;font-weight:500;border-top:1px solid #e4e7eb;margin-top:48px;">
  <a href="https://github.com/mattkram/poc-conda-channel" target="_blank" rel="noopener"
     style="color:#6B7280;text-decoration:none;display:inline-flex;align-items:center;gap:8px;"
     onmouseover="this.style.color='#0B8275'" onmouseout="this.style.color='#6B7280'">
    <svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 496 512" width="18" height="18" fill="currentColor" aria-label="GitHub">
      <path d="M165.9 397.4c0 2-2.3 3.6-5.2 3.6-3.3.3-5.6-1.3-5.6-3.6 0-2 2.3-3.6 5.2-3.6 3-.3 5.6 1.3 5.6 3.6zm-31.1-4.5c-.7 2 1.3 4.3 4.3 4.9 2.6 1 5.6 0 6.2-2s-1.3-4.3-4.3-5.2c-2.6-.7-5.5.3-6.2 2.3zm44.2-1.7c-2.9.7-4.9 2.6-4.6 4.9.3 2 2.9 3.3 5.9 2.6 2.9-.7 4.9-2.6 4.6-4.6-.3-1.9-3-3.2-5.9-2.9zM244.8 8C106.1 8 0 113.3 0 252c0 110.9 69.8 205.8 169.5 239.2 12.8 2.3 17.3-5.6 17.3-12.1 0-6.2-.3-40.4-.3-61.4 0 0-70 15-84.7-29.8 0 0-11.4-29.1-27.8-36.6 0 0-22.9-15.7 1.6-15.4 0 0 24.9 2 38.6 25.8 21.9 38.6 58.6 27.5 72.9 20.9 2.3-16 8.8-27.1 16-33.7-55.9-6.2-112.3-14.3-112.3-110.5 0-27.5 7.6-41.3 23.6-58.9-2.6-6.5-11.1-33.3 2.6-67.9 20.9-6.5 69 27 69 27 20-5.6 41.5-8.5 62.8-8.5s42.8 2.9 62.8 8.5c0 0 48.1-33.6 69-27 13.7 34.7 5.2 61.4 2.6 67.9 16 17.7 25.8 31.5 25.8 58.9 0 96.5-58.9 104.2-114.8 110.5 9.2 7.9 17 22.9 17 46.4 0 33.7-.3 75.4-.3 83.6 0 6.5 4.6 14.4 17.3 12.1C428.2 457.8 496 362.9 496 252 496 113.3 390.1 8 244.8 8z"/>
    </svg>
    mattkram/poc-conda-channel
  </a>
</footer>`;

export const HERO_CSS = `
  .hero { background: linear-gradient(160deg, #f0fdf9 0%, #f8fafc 60%); border-bottom:1px solid #e4e7eb; padding:56px 24px 52px; text-align:center; }
  .hero-logo { width:340px; max-width:90vw; margin:0 auto 8px; display:block; }
  .hero-tagline { color:#6B7280; font-size:16px; font-weight:500; margin:0 0 36px; letter-spacing:0.01em; }
  .hero-search-wrap { max-width:640px; margin:0 auto 28px; display:flex; gap:0; box-shadow:0 4px 20px rgba(20,187,166,.15); border-radius:12px; overflow:hidden; border:1.5px solid #99f6e4; }
  .hero-search-wrap input[type=search] { flex:1; padding:18px 22px; border:none; font-size:17px; outline:none; color:#1e293b; min-width:0; font-family:inherit; background:#fff; }
  .hero-search-wrap button { padding:0 32px; background:#0B8275; color:#fff; border:none; font-size:15px; font-weight:700; cursor:pointer; white-space:nowrap; font-family:inherit; transition:background .15s; }
  .hero-search-wrap button:hover { background:#0f9a8a; }
  .hero-links { display:flex; gap:24px; justify-content:center; flex-wrap:wrap; font-size:14px; font-weight:600; }
  .hero-links a { color:#0B8275; text-decoration:none; display:flex; align-items:center; gap:6px; }
  .hero-links a:hover { text-decoration:underline; color:#0f9a8a; }
  #search-results { max-width:960px; margin:0 auto; padding:0 24px; }
  .results-table { width:100%; border-collapse:collapse; font-size:14px; margin-top:8px; }
  .results-table th { text-align:left; padding:8px 12px; color:#6B7280; font-size:12px; font-weight:700; text-transform:uppercase; letter-spacing:.06em; border-bottom:2px solid #e4e7eb; }
  .results-table td { padding:10px 12px; border-bottom:1px solid #f0f2f5; vertical-align:middle; }
  .results-table tr:last-child td { border-bottom:none; }
  .results-table tr:hover td { background:#f0fdf9; }
  .results-table a.pkg-link { font-weight:700; color:#0B8275; text-decoration:none; font-size:15px; }
  .results-table a.pkg-link:hover { text-decoration:underline; color:#0f9a8a; }
  .results-table .pkg-summary { color:#6B7280; font-size:13px; }
  .results-table .chan-link { color:#9aacb8; font-size:12px; text-decoration:none; font-weight:600; }
  .results-table .chan-link:hover { color:#0B8275; text-decoration:underline; }
  .results-count { color:#6B7280; font-size:13px; padding:12px 0 4px; font-weight:600; }
`;

