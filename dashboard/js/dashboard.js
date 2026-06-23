/* SASA Analytics Dashboard */

const API = '';
let currentProject = null;
let ws = null;
let charts = {};
let feedRows = [];
const MAX_FEED = 80;
const COLORS = ['#4f8ef7','#34d399','#fbbf24','#f87171','#7c5cfc','#fb923c','#38bdf8','#a3e635'];

// ── boot ─────────────────────────────────────────────────────────────────────
document.addEventListener('DOMContentLoaded', async () => {
  // close modal when clicking the dark backdrop
  document.getElementById('modal-backdrop').addEventListener('click', e => {
    if (e.target === e.currentTarget) closeModal();
  });

  initCharts();
  await loadProjects();
});

// ── projects ──────────────────────────────────────────────────────────────────
async function loadProjects() {
  const res  = await fetch(`${API}/api/projects`);
  const list = await res.json();
  renderProjectList(list);
  if (list.length) selectProject(list[0]);
}

function renderProjectList(projects) {
  const el = document.getElementById('project-list');
  el.innerHTML = projects.map(p => `
    <div class="proj-item ${currentProject?.id === p.id ? 'active':''}" id="proj-${escHtml(p.id)}" onclick="selectProject(${JSON.stringify(p).replace(/"/g,'&quot;')})">
      <div class="proj-dot" style="background:${safeColor(p.color)}"></div>
      <div class="proj-name">${escHtml(p.name)}</div>
      <div class="proj-sessions" id="proj-sessions-${escHtml(p.id)}">—</div>
    </div>`).join('');
}

function selectProject(p) {
  currentProject = p;
  document.getElementById('topbar-title').textContent = p.name;

  // update sidebar active state
  document.querySelectorAll('.proj-item').forEach(el => el.classList.remove('active'));
  document.getElementById(`proj-${p.id}`)?.classList.add('active');

  connectWS(p.id);
}

// ── WebSocket ─────────────────────────────────────────────────────────────────
function connectWS(projectId) {
  if (ws) ws.close();
  const url = `ws://${location.host}/ws/${projectId}`;
  ws = new WebSocket(url);

  ws.onopen = () => {
    document.getElementById('conn-circle').classList.add('ok');
    document.getElementById('conn-text').textContent = 'Connected';
  };
  ws.onmessage = e => {
    try { renderSnapshot(JSON.parse(e.data)); } catch {}
  };
  ws.onclose = () => {
    document.getElementById('conn-circle').classList.remove('ok');
    document.getElementById('conn-text').textContent = 'Reconnecting…';
    setTimeout(() => connectWS(projectId), 2000);
  };
  ws.onerror = () => ws.close();
}

// ── render snapshot ───────────────────────────────────────────────────────────
function renderSnapshot(data) {
  const live  = data.live  || {};
  const spark = data.spark || {};

  // stat cards
  setText('stat-sessions',  live.active_sessions ?? 0);
  setText('stat-events',    fmtNum(live.total_events ?? 0));
  setText('stat-eps',       live.events_per_sec ?? 0);
  setText('stat-pageviews', fmtNum((live.event_type_counts?.page_view) ?? 0));

  // sidebar session counters
  if (currentProject) {
    const el = document.getElementById(`proj-sessions-${currentProject.id}`);
    if (el) el.textContent = live.active_sessions ?? 0;
  }

  // timeline chart
  const tl = live.event_timeline || [];
  if (tl.length) {
    charts.timeline.data.labels             = tl.map(p => fmtTime(p.ts));
    charts.timeline.data.datasets[0].data   = tl.map(p => p.count);
    charts.timeline.update('none');
  }

  // event type doughnut
  const etc = live.event_type_counts || {};
  const etL = Object.keys(etc);
  const etV = Object.values(etc);
  if (etL.length) {
    charts.donut.data.labels             = etL;
    charts.donut.data.datasets[0].data   = etV;
    charts.donut.update('none');
  }

  // top pages table
  renderTopPages(live.top_pages || []);

  // top clicks table
  renderTopClicks(live.top_clicks || []);

  // video viewers
  const vpv = live.video_viewers || {};
  renderVideoViewers(vpv);

  // spark event windows chart
  const ec = spark.event_counts || [];
  if (ec.length) renderSparkChart(ec);
}

// ── top pages ─────────────────────────────────────────────────────────────────
function renderTopPages(pages) {
  const tbody = document.getElementById('top-pages-body');
  if (!pages.length) { tbody.innerHTML = '<tr><td colspan="3" style="color:var(--muted);text-align:center;padding:16px">No page views yet</td></tr>'; return; }
  const max = pages[0]?.views || 1;
  tbody.innerHTML = pages.slice(0,8).map(p => `
    <tr>
      <td style="font-family:monospace;color:var(--accent)">${escHtml(p.path)}</td>
      <td style="text-align:right;font-weight:700">${p.views}</td>
      <td style="width:80px">
        <div class="bar-inline"><div class="bar-inline-fill" style="width:${Math.round(p.views/max*100)}%"></div></div>
      </td>
    </tr>`).join('');
}

// ── top clicks ────────────────────────────────────────────────────────────────
function renderTopClicks(clicks) {
  const tbody = document.getElementById('top-clicks-body');
  if (!clicks.length) { tbody.innerHTML = '<tr><td colspan="2" style="color:var(--muted);text-align:center;padding:16px">No clicks tracked yet</td></tr>'; return; }
  tbody.innerHTML = clicks.slice(0,8).map(c => `
    <tr>
      <td>${escHtml(c.label)}</td>
      <td style="text-align:right;font-weight:700;color:var(--amber)">${c.count}</td>
    </tr>`).join('');
}

// ── video viewers ─────────────────────────────────────────────────────────────
function renderVideoViewers(vpv) {
  const el = document.getElementById('video-viewers');
  const entries = Object.entries(vpv).sort((a,b) => b[1]-a[1]).slice(0,6);
  if (!entries.length) { el.innerHTML = '<div class="empty"><div class="empty-icon">🎬</div><div class="empty-text">No video events yet</div></div>'; return; }
  const max = entries[0][1] || 1;
  el.innerHTML = entries.map(([vid, count], i) => `
    <div style="margin-bottom:10px">
      <div style="display:flex;justify-content:space-between;margin-bottom:3px;font-size:12px">
        <span style="color:var(--text);font-weight:600;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;max-width:70%">${escHtml(vid)}</span>
        <span style="color:var(--green);font-weight:700">${count} viewers</span>
      </div>
      <div class="bar-inline"><div class="bar-inline-fill" style="width:${Math.round(count/max*100)}%;background:${COLORS[i%COLORS.length]}"></div></div>
    </div>`).join('');
}

// ── spark windows chart ───────────────────────────────────────────────────────
function renderSparkChart(ec) {
  const windows   = [...new Set(ec.map(r => r.window_start))].sort().slice(-10);
  const eventTypes = [...new Set(ec.map(r => r.event_name || r.event_type))];
  const byWindow  = {};
  ec.forEach(r => {
    const w = r.window_start;
    const t = r.event_name || r.event_type;
    if (!byWindow[w]) byWindow[w] = {};
    byWindow[w][t] = (byWindow[w][t] || 0) + (r.count || 0);
  });

  charts.spark.data.labels   = windows.map(w => fmtTime(new Date(w).getTime()/1000));
  charts.spark.data.datasets = eventTypes.slice(0,6).map((t, i) => ({
    label: t,
    data:  windows.map(w => byWindow[w]?.[t] || 0),
    backgroundColor: COLORS[i % COLORS.length] + 'bb',
    stack: 's',
    borderRadius: 3,
  }));
  charts.spark.update('none');
}

// ── event feed (called from SDK demo events via polling) ──────────────────────
let _lastEventCount = 0;
setInterval(async () => {
  if (!currentProject) return;
  try {
    const res  = await fetch(`${API}/api/metrics/${currentProject.id}`);
    const data = await res.json();
    const live = data.live || {};
    const total = live.total_events || 0;
    if (total !== _lastEventCount) {
      _lastEventCount = total;
      pushFeedRow(live);
    }
  } catch {}
}, 3000);

function pushFeedRow(live) {
  const etc = live.event_type_counts || {};
  const etype = Object.keys(etc)[Math.floor(Math.random()*Object.keys(etc).length)] || 'event';
  const pages = live.top_pages || [];
  const path  = pages[Math.floor(Math.random()*pages.length)]?.path || '/';
  addFeedRow(etype, path);
}

function addFeedRow(etype, path, extra) {
  const feed = document.getElementById('feed-inner');
  const cls  = etype.startsWith('page') ? 'b-page'
             : etype.startsWith('click') ? 'b-click'
             : etype.startsWith('video') ? 'b-video'
             : etype === 'js_error' ? 'b-error' : 'b-other';
  const row = document.createElement('div');
  row.className = 'feed-row';
  row.innerHTML = `
    <span class="feed-badge ${cls}">${etype}</span>
    <span class="feed-path">${escHtml(path)}</span>
    <span class="feed-time">${fmtTimestamp()}</span>`;
  feed.prepend(row);
  feedRows.push(row);
  if (feedRows.length > MAX_FEED) feedRows.shift()?.remove();
}

// ── charts init ───────────────────────────────────────────────────────────────
function initCharts() {
  const grid  = 'rgba(37,45,66,.9)';
  const muted = '#6b7fa3';
  const base  = { responsive:true, maintainAspectRatio:false, animation:{duration:250},
                   plugins:{legend:{display:false}} };

  charts.timeline = new Chart(document.getElementById('chart-timeline'), {
    type: 'line',
    data: { labels:[], datasets:[{ data:[], borderColor:'#4f8ef7', backgroundColor:'rgba(79,142,247,.1)',
      fill:true, tension:.4, pointRadius:0, borderWidth:2 }] },
    options: { ...base, scales: {
      x:{ticks:{color:muted,maxTicksLimit:6},grid:{color:grid}},
      y:{ticks:{color:muted},grid:{color:grid},beginAtZero:true} } },
  });

  charts.donut = new Chart(document.getElementById('chart-donut'), {
    type: 'doughnut',
    data: { labels:[], datasets:[{ data:[], backgroundColor:COLORS, borderWidth:0 }] },
    options: { ...base, cutout:'68%',
      plugins:{ legend:{ display:true, position:'right',
        labels:{color:muted, font:{size:10}, boxWidth:10, padding:8} } } },
  });

  charts.spark = new Chart(document.getElementById('chart-spark'), {
    type: 'bar',
    data: { labels:[], datasets:[] },
    options: { ...base,
      plugins:{ legend:{ display:true, labels:{color:muted,font:{size:10},boxWidth:10,padding:8} } },
      scales: {
        x:{stacked:true, ticks:{color:muted,maxTicksLimit:8}, grid:{color:grid}},
        y:{stacked:true, ticks:{color:muted}, grid:{color:grid}, beginAtZero:true} } },
  });
}

// ── install snippet modal ─────────────────────────────────────────────────────
function openSnippet() {
  if (!currentProject) return;
  document.getElementById('modal-snippet').innerHTML = snippetHTML(currentProject);
  document.getElementById('modal-backdrop').classList.remove('hidden');
}
function closeModal() {
  document.getElementById('modal-backdrop').classList.add('hidden');
}

function snippetHTML(p) {
  const host = location.host;
  const scriptTag = `<script
  src="http://${host}/sdk/sasa.js"
  data-project="${p.id}"
  data-api-key="${p.api_key}"
  data-track-videos="true"
  data-track-clicks="true"
  data-track-scroll="true">
<\/script>`;

  return `
  <div class="modal-head">
    <div class="modal-title">Install SASA on <span style="color:var(--accent)">${escHtml(p.name)}</span></div>
    <button class="modal-close" onclick="closeModal()">✕</button>
  </div>
  <div class="modal-body">
    <div class="install-step">
      <div class="install-step-title">Step 1 — Paste this into your &lt;head&gt;</div>
      <div class="code-block">
        <pre>${escHtml(scriptTag)}</pre>
        <button class="copy-btn" onclick="copyCode(this, ${JSON.stringify(scriptTag)})">Copy</button>
      </div>
    </div>
    <div class="install-step">
      <div class="install-step-title">Step 2 — Optional manual tracking</div>
      <div class="code-block">
        <pre>// Track a custom event
SASA.track('purchase', { plan: 'pro', amount: 49 });

// Identify a user
SASA.identify('user_123', { email: 'jane@co.com', plan: 'pro' });

// Manual page view
SASA.page('Checkout');</pre>
        <button class="copy-btn" onclick="copyCode(this, \`SASA.track('purchase', { plan: 'pro', amount: 49 });\`)">Copy</button>
      </div>
    </div>
    <div class="install-step">
      <div class="install-step-title">Auto-tracked out of the box</div>
      <div class="tag-grid">
        <span class="tag on">✓ Page views</span>
        <span class="tag on">✓ Sessions</span>
        <span class="tag on">✓ Scroll depth</span>
        <span class="tag on">✓ Clicks</span>
        <span class="tag on">✓ Video play/pause/seek</span>
        <span class="tag on">✓ JS errors</span>
        <span class="tag on">✓ SPA navigation</span>
        <span class="tag on">✓ Page exit + time on page</span>
      </div>
    </div>
    <div class="install-step">
      <div class="install-step-title">Your API key</div>
      <div class="code-block"><pre>${p.api_key}</pre>
        <button class="copy-btn" onclick="copyCode(this, '${p.api_key}')">Copy</button>
      </div>
    </div>
    <div style="margin-top:12px;padding:12px;background:var(--surface2);border-radius:8px;font-size:12px;color:var(--muted);line-height:1.7">
      <strong style="color:var(--text)">Try it instantly:</strong>
      Open <a href="/demo" target="_blank" style="color:var(--accent)">/demo</a> — it's a sample page already instrumented with this project's key. Events appear in the dashboard within 1 second.
    </div>
  </div>`;
}

function copyCode(btn, text) {
  navigator.clipboard.writeText(text).then(() => {
    btn.textContent = 'Copied!';
    setTimeout(() => { btn.textContent = 'Copy'; }, 1500);
  });
}

// ── new project modal ─────────────────────────────────────────────────────────
function openNewProject() {
  document.getElementById('modal-backdrop').classList.remove('hidden');
  document.getElementById('modal-snippet').innerHTML = newProjectHTML();
}

function newProjectHTML() {
  return `
  <div class="modal-head">
    <div class="modal-title">New Project</div>
    <button class="modal-close" onclick="closeModal()">✕</button>
  </div>
  <div class="modal-body">
    <div class="form-group">
      <label class="form-label">Project Name</label>
      <input id="new-proj-name" class="form-input" placeholder="My App" autofocus>
    </div>
    <div class="form-group">
      <label class="form-label">Color</label>
      <input id="new-proj-color" type="color" value="#4f8ef7" style="height:36px;width:80px;border-radius:7px;border:1px solid var(--border);background:var(--surface2);cursor:pointer">
    </div>
    <button class="btn-primary" onclick="createProject()">Create Project</button>
  </div>`;
}

async function createProject() {
  const name  = document.getElementById('new-proj-name')?.value.trim();
  const color = document.getElementById('new-proj-color')?.value || '#4f8ef7';
  if (!name) return;
  const res = await fetch(`${API}/api/projects`, {
    method: 'POST', headers: {'Content-Type':'application/json'},
    body: JSON.stringify({ name, color }),
  });
  const p = await res.json();
  closeModal();
  await loadProjects();
  selectProject(p);
  setTimeout(() => openSnippet(), 300);
}

// ── utils ─────────────────────────────────────────────────────────────────────
function setText(id, val) {
  const el = document.getElementById(id);
  if (el) el.textContent = val;
}
function fmtNum(n) {
  if (n >= 1e6) return (n/1e6).toFixed(1)+'M';
  if (n >= 1e3) return (n/1e3).toFixed(1)+'K';
  return String(n);
}
function fmtTime(ts) {
  return new Date(ts*1000).toLocaleTimeString([],{hour:'2-digit',minute:'2-digit',second:'2-digit'});
}
function fmtTimestamp() {
  return new Date().toLocaleTimeString([],{hour:'2-digit',minute:'2-digit',second:'2-digit'});
}
function escHtml(s) {
  return String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');
}
// Only allow hex colours into inline style; fall back to the brand accent so a
// crafted `color` value can't break out of the style attribute. (H-3 hardening)
function safeColor(c) {
  return /^#[0-9a-fA-F]{3,8}$/.test(String(c||'')) ? c : 'var(--accent)';
}
