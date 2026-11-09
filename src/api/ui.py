"""
LangManus 应用管理层 Web UI

返回内嵌 HTML 单页应用，提供应用管理、Agent 实例、QoS 监控、资源状态四个功能页签。
挂载于 GET /ui
"""


def get_ui_html() -> str:
    return """<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>LangManus 应用管理</title>
<style>
  *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }
  body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
         background: #f0f2f5; color: #222; min-height: 100vh; }
  header { background: #1a1a2e; color: #fff; padding: 14px 28px;
           display: flex; align-items: center; gap: 12px; }
  header h1 { font-size: 20px; font-weight: 700; letter-spacing: .5px; }
  header .badge { background: #e43; color: #fff; border-radius: 4px;
                  font-size: 11px; padding: 2px 7px; font-weight: 600; }
  .main { max-width: 1200px; margin: 24px auto; padding: 0 16px; }

  /* Tabs */
  .tabs { display: flex; gap: 4px; margin-bottom: 20px; }
  .tab-btn { padding: 9px 22px; border: none; border-radius: 8px 8px 0 0;
             background: #d9deea; color: #556; cursor: pointer;
             font-size: 14px; font-weight: 500; transition: all .15s; }
  .tab-btn.active { background: #fff; color: #1a1a2e; box-shadow: 0 -2px 8px rgba(0,0,0,.08); }
  .tab-btn:hover:not(.active) { background: #c9d0e0; }
  .tab-panel { display: none; background: #fff; border-radius: 0 8px 8px 8px;
               padding: 24px; box-shadow: 0 2px 12px rgba(0,0,0,.07); }
  .tab-panel.active { display: block; }

  /* Cards */
  .card { border: 1px solid #e8ecf2; border-radius: 10px; padding: 20px;
          margin-bottom: 20px; }
  .card-title { font-size: 15px; font-weight: 600; color: #1a1a2e;
                margin-bottom: 16px; display: flex; align-items: center; gap: 8px; }
  .card-title .icon { font-size: 18px; }

  /* Form */
  .form-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 14px; }
  .form-grid.full { grid-template-columns: 1fr; }
  .form-group { display: flex; flex-direction: column; gap: 5px; }
  .form-group label { font-size: 13px; font-weight: 500; color: #445; }
  .form-group input, .form-group select, .form-group textarea {
    border: 1px solid #d0d6e0; border-radius: 6px; padding: 8px 11px;
    font-size: 14px; outline: none; transition: border .15s; background: #fafbfc; }
  .form-group input:focus, .form-group select:focus, .form-group textarea:focus {
    border-color: #4a6cf7; background: #fff; }
  .form-group textarea { resize: vertical; min-height: 72px; }
  .form-group small { font-size: 12px; color: #999; }

  /* Buttons */
  .btn { padding: 8px 18px; border: none; border-radius: 7px; cursor: pointer;
         font-size: 14px; font-weight: 500; transition: all .15s; display: inline-flex;
         align-items: center; gap: 6px; }
  .btn-primary { background: #4a6cf7; color: #fff; }
  .btn-primary:hover { background: #3a5ce7; }
  .btn-success { background: #22c55e; color: #fff; }
  .btn-success:hover { background: #16a34a; }
  .btn-danger  { background: #ef4444; color: #fff; }
  .btn-danger:hover  { background: #dc2626; }
  .btn-warning { background: #f59e0b; color: #fff; }
  .btn-warning:hover { background: #d97706; }
  .btn-ghost { background: #f1f5f9; color: #445; border: 1px solid #d0d6e0; }
  .btn-ghost:hover { background: #e2e8f0; }
  .btn-sm { padding: 5px 12px; font-size: 13px; }
  .btn:disabled { opacity: .55; cursor: not-allowed; }

  /* Table */
  table { width: 100%; border-collapse: collapse; font-size: 14px; }
  thead th { background: #f6f8fc; padding: 11px 14px; text-align: left;
             font-weight: 600; color: #556; border-bottom: 2px solid #e8ecf2; }
  tbody tr { border-bottom: 1px solid #f0f2f5; transition: background .1s; }
  tbody tr:hover { background: #fafbff; }
  tbody td { padding: 11px 14px; vertical-align: middle; }
  .empty-row td { text-align: center; color: #aaa; padding: 32px; }

  /* Status badges */
  .badge-status { display: inline-block; padding: 3px 10px; border-radius: 20px;
                  font-size: 12px; font-weight: 600; }
  .s-idle     { background: #f1f5f9; color: #64748b; }
  .s-starting { background: #fef9c3; color: #ca8a04; }
  .s-running  { background: #dcfce7; color: #16a34a; }
  .s-stopping { background: #ffedd5; color: #ea580c; }
  .s-stopped  { background: #f1f5f9; color: #94a3b8; }
  .s-error    { background: #fee2e2; color: #dc2626; }
  .s-scheduled{ background: #ede9fe; color: #7c3aed; }

  /* Alert */
  .alert { padding: 12px 16px; border-radius: 8px; margin-bottom: 14px;
           font-size: 14px; display: none; }
  .alert.success { background: #dcfce7; color: #166534; border: 1px solid #bbf7d0; }
  .alert.error   { background: #fee2e2; color: #991b1b; border: 1px solid #fecaca; }
  .alert.info    { background: #dbeafe; color: #1e40af; border: 1px solid #bfdbfe; }
  .alert.show { display: block; }

  /* Query panel */
  .query-panel { background: #f8faff; border: 1px solid #dde5f8; border-radius: 8px;
                 padding: 16px; margin-top: 10px; display: none; }
  .query-panel.open { display: block; }
  .query-result { background: #1e1e2e; color: #cdd6f4; border-radius: 8px;
                  padding: 16px; margin-top: 12px; max-height: 400px;
                  overflow-y: auto; font-size: 14px; font-family: system-ui, sans-serif;
                  white-space: pre-wrap; word-break: break-word; display: none; line-height: 1.6; }
  .query-result.show { display: block; }

  /* Progress spinner */
  .spinner { display: inline-block; width: 14px; height: 14px;
             border: 2px solid rgba(255,255,255,.4);
             border-top-color: #fff; border-radius: 50%;
             animation: spin .6s linear infinite; }
  @keyframes spin { to { transform: rotate(360deg); } }

  /* Section toolbar */
  .toolbar { display: flex; justify-content: space-between; align-items: center;
             margin-bottom: 16px; }
  .toolbar-right { display: flex; gap: 8px; align-items: center; }
  .refresh-hint { font-size: 12px; color: #aaa; }

  /* Metric cards row */
  .metric-row { display: grid; grid-template-columns: repeat(auto-fill, minmax(200px,1fr)); gap: 14px; }
  .metric-card { background: #f8faff; border: 1px solid #dde5f8; border-radius: 10px;
                 padding: 16px; }
  .metric-card .metric-label { font-size: 12px; color: #778; margin-bottom: 4px; }
  .metric-card .metric-value { font-size: 24px; font-weight: 700; color: #1a1a2e; }
  .metric-card .metric-unit  { font-size: 13px; color: #778; }

  /* collapse toggle */
  .collapse-header { cursor: pointer; display: flex; align-items: center; gap: 8px;
                     user-select: none; }
  .collapse-header .arrow { transition: transform .2s; }
  .collapse-header.open .arrow { transform: rotate(90deg); }
  .collapse-body { display: none; margin-top: 14px; }
  .collapse-body.open { display: block; }

  /* schedule history table (nested) */
  .sched-history { font-size: 12px; margin-top: 10px; }
  .sched-history th { font-size: 11px; padding: 6px 8px; }
  .sched-history td { padding: 6px 8px; font-size: 12px; }
  .sched-badge { display: inline-block; padding: 1px 7px; border-radius: 10px;
                 font-size: 11px; font-weight: 600; }
  .sb-running   { background: #dcfce7; color: #16a34a; }
  .sb-completed { background: #dbeafe; color: #2563eb; }
  .sb-failed    { background: #fee2e2; color: #dc2626; }
  .sb-cancelled { background: #f1f5f9; color: #94a3b8; }

  /* threshold warning */
  tr.warn-row td { background: #fff7ed !important; }
  tr.error-row td { background: #fff1f2 !important; }

  .divider { border: none; border-top: 1px solid #e8ecf2; margin: 18px 0; }
</style>
</head>
<body>
<header>
  <span style="font-size:24px">🤖</span>
  <h1>LangManus</h1>
  <span class="badge">应用管理控制台</span>
  <a href="/traffic/platform/ui" style="color:#fff;font-size:14px;text-decoration:none;margin-left:12px">交通联合编排 ↗</a>
  <span id="server-status" style="margin-left:auto;font-size:13px;opacity:.8">● 连接中...</span>
</header>

<div class="main">
  <!-- Global alert -->
  <div id="global-alert" class="alert"></div>

  <!-- Tabs -->
  <div class="tabs">
    <button class="tab-btn active" onclick="switchTab('apps')">📦 应用管理</button>
    <button class="tab-btn" onclick="switchTab('instances')">🖥 Agent 实例</button>
    <button class="tab-btn" onclick="switchTab('qos')">📊 QoS 监控</button>
    <button class="tab-btn" onclick="switchTab('resources')">🗃 资源状态</button>
  </div>

  <!-- ====== Tab: 应用管理 ====== -->
  <div id="tab-apps" class="tab-panel active">
    <!-- Install form -->
    <div class="card">
      <div class="collapse-header open" onclick="toggleCollapse(this)">
        <span class="arrow">▶</span>
        <span class="card-title" style="margin:0"><span class="icon">➕</span> 安装新应用</span>
      </div>
      <div class="collapse-body open">
        <div class="form-grid">
          <div class="form-group">
            <label>应用名称 *</label>
            <input id="f-name" type="text" placeholder="如：搜索助手">
          </div>
          <div class="form-group">
            <label>编排模式</label>
            <select id="f-mode">
              <option value="adaptive">adaptive（自适应）</option>
              <option value="sequential">sequential（顺序）</option>
              <option value="distributed">distributed（分布式）</option>
            </select>
          </div>
          <div class="form-group" style="grid-column:1/-1">
            <label>任务描述 *</label>
            <textarea id="f-task" placeholder="描述该应用要完成的任务，如：搜索并总结用户提供的主题"></textarea>
          </div>
          <div class="form-group">
            <label>所需 Agent 能力</label>
            <input id="f-agents" type="text" placeholder="search, nlp, code（逗号分隔）">
            <small>留空表示不限制</small>
          </div>
          <div class="form-group">
            <label>超时（秒）</label>
            <input id="f-timeout" type="number" value="120" min="10" max="600">
          </div>
          <div class="form-group">
            <label>周期调度间隔（秒）</label>
            <input id="f-schedule-interval" type="number" value="0" min="0" max="86400">
            <small>0 = 不启用周期调度；>0 = 安装后可启动定时执行</small>
          </div>
          <div class="form-group">
            <label>最大并行实例</label>
            <input id="f-schedule-parallel" type="number" value="5" min="1" max="50">
            <small>周期调度允许同时运行的最大工作流数</small>
          </div>
          <div class="form-group" style="display:flex;flex-direction:row;align-items:center;gap:8px">
            <input id="f-schedule-autorestart" type="checkbox">
            <label style="margin:0">服务重启后自动恢复调度</label>
          </div>
          <div class="form-group" style="grid-column:1/-1">
            <label>Skills.md 内容 <small style="color:#999;font-weight:400">（可选）— 应用专属技能指引，注入给编排引擎</small></label>
            <textarea id="f-skills" style="height:100px;font-size:12px;font-family:monospace" placeholder="# 应用技能
## 核心能力
..."></textarea>
          </div>
        </div>
        <hr class="divider">
        <button class="btn btn-primary" onclick="installApp()">
          <span>📦</span> 安装应用
        </button>
      </div>
    </div>

    <!-- App list -->
    <div class="toolbar">
      <span style="font-weight:600;font-size:15px">应用列表</span>
      <div class="toolbar-right">
        <span class="refresh-hint" id="apps-refresh-hint"></span>
        <button class="btn btn-ghost btn-sm" onclick="loadApps()">🔄 刷新</button>
      </div>
    </div>
    <div id="apps-alert" class="alert"></div>
    <table id="apps-table">
      <thead><tr>
        <th>名称</th><th>应用 ID</th><th>状态</th><th>Workflow Handle</th><th>操作</th>
      </tr></thead>
      <tbody id="apps-tbody">
        <tr class="empty-row"><td colspan="5">加载中...</td></tr>
      </tbody>
    </table>
  </div>

  <!-- ====== Tab: Agent 实例 ====== -->
  <div id="tab-instances" class="tab-panel">
    <div class="toolbar">
      <span style="font-weight:600;font-size:15px">Agent 实例</span>
      <div class="toolbar-right">
        <span class="refresh-hint" id="inst-refresh-hint"></span>
        <button class="btn btn-ghost btn-sm" onclick="loadInstances()">🔄 刷新</button>
      </div>
    </div>
    <table>
      <thead><tr>
        <th>Instance ID</th><th>Agent ID</th><th>Image ID</th>
        <th>状态</th><th>引用计数</th><th>订阅工作流</th>
      </tr></thead>
      <tbody id="inst-tbody">
        <tr class="empty-row"><td colspan="6">暂无实例数据</td></tr>
      </tbody>
    </table>
  </div>

  <!-- ====== Tab: QoS 监控 ====== -->
  <div id="tab-qos" class="tab-panel">
    <div id="qos-metrics-row" class="metric-row" style="margin-bottom:20px"></div>
    <div class="toolbar">
      <span style="font-weight:600;font-size:15px">Agent QoS 详情</span>
      <div class="toolbar-right">
        <span class="refresh-hint" id="qos-refresh-hint"></span>
        <button class="btn btn-ghost btn-sm" onclick="loadQos()">🔄 刷新</button>
      </div>
    </div>
    <table>
      <thead><tr>
        <th>Agent ID</th><th>总调用</th><th>成功</th><th>失败</th>
        <th>平均延迟 (ms)</th><th>最大延迟 (ms)</th><th>成功率</th><th>告警</th>
      </tr></thead>
      <tbody id="qos-tbody">
        <tr class="empty-row"><td colspan="8">暂无 QoS 数据</td></tr>
      </tbody>
    </table>
  </div>

  <!-- ====== Tab: 资源状态 ====== -->
  <div id="tab-resources" class="tab-panel">
    <div id="res-summary-row" class="metric-row" style="margin-bottom:20px"></div>
    <div class="toolbar">
      <span style="font-weight:600;font-size:15px">节点列表</span>
      <div class="toolbar-right">
        <span class="refresh-hint" id="res-refresh-hint"></span>
        <button class="btn btn-ghost btn-sm" onclick="loadResources()">🔄 刷新</button>
      </div>
    </div>
    <table>
      <thead><tr>
        <th>Node ID</th><th>IP</th><th>类型</th>
        <th>CPU (已用/总)</th><th>内存 (已用/总 MB)</th><th>GPU</th><th>状态</th>
      </tr></thead>
      <tbody id="res-tbody">
        <tr class="empty-row"><td colspan="7">加载中...</td></tr>
      </tbody>
    </table>
  </div>
</div>

<script>
// ====================================================================
// Utilities
// ====================================================================
const API = '';  // same origin

function showAlert(id, type, msg, duration=4000) {
  const el = document.getElementById(id);
  el.className = `alert ${type} show`;
  el.textContent = msg;
  if (duration > 0) setTimeout(() => el.classList.remove('show'), duration);
}

function statusBadge(s) {
  return `<span class="badge-status s-${s}">${s}</span>`;
}

function fmtTime() {
  return new Date().toLocaleTimeString('zh-CN');
}

// ====================================================================
// Tab switching
// ====================================================================
const TAB_LOADERS = { apps: loadApps, instances: loadInstances, qos: loadQos, resources: loadResources };
let activeTab = 'apps';

function switchTab(name) {
  document.querySelectorAll('.tab-btn').forEach((b,i) => {
    b.classList.toggle('active', ['apps','instances','qos','resources'][i] === name);
  });
  document.querySelectorAll('.tab-panel').forEach(p => p.classList.remove('active'));
  document.getElementById('tab-'+name).classList.add('active');
  activeTab = name;
  TAB_LOADERS[name]();
}

// ====================================================================
// Collapse
// ====================================================================
function toggleCollapse(header) {
  header.classList.toggle('open');
  header.nextElementSibling.classList.toggle('open');
}

// ====================================================================
// Tab: APPS
// ====================================================================

// 记住哪些查询面板是展开的，以及面板里的输入内容和结果
const _openPanels = new Set();       // app_id → 查询面板 open
const _panelInput = {};              // app_id → 查询输入框内容
const _panelResult = {};             // app_id → 查询结果
const _pendingQueries = new Set();   // app_id → 查询请求进行中
// 编辑面板状态
const _openEditPanels = new Set();   // app_id → 编辑面板 open
const _editName = {};                // app_id → 名称输入内容
const _editSkills = {};              // app_id → skills 输入内容

function _saveQueryState() {
  // 保存查询面板状态
  document.querySelectorAll('[id^="qp-"]').forEach(el => {
    const appId = el.id.slice(3);
    if (el.classList.contains('open')) {
      _openPanels.add(appId);
      const inp = document.getElementById('qi-' + appId);
      if (inp) _panelInput[appId] = inp.value;
      const res = document.getElementById('qr-' + appId);
      if (res) _panelResult[appId] = { text: res.textContent, show: res.classList.contains('show') };
    } else {
      _openPanels.delete(appId);
    }
  });
  // 保存调度历史面板状态
  document.querySelectorAll('[id^="shp-"]').forEach(el => {
    const appId = el.id.slice(4);
    if (el.classList.contains('open')) _openHistoryPanels.add(appId);
    else _openHistoryPanels.delete(appId);
  });
  // 保存编辑面板状态
  document.querySelectorAll('[id^="ep-"]').forEach(el => {
    const appId = el.id.slice(3);
    if (el.classList.contains('open')) {
      _openEditPanels.add(appId);
      const nameEl = document.getElementById('en-' + appId);
      if (nameEl) _editName[appId] = nameEl.value;
      const skillsEl = document.getElementById('es-' + appId);
      if (skillsEl) _editSkills[appId] = skillsEl.value;
    } else {
      _openEditPanels.delete(appId);
    }
  });
}

function _restoreQueryState() {
  // 恢复查询面板
  _openPanels.forEach(appId => {
    const panel = document.getElementById('qp-' + appId);
    if (panel) {
      panel.classList.add('open');
      const inp = document.getElementById('qi-' + appId);
      if (inp && _panelInput[appId] != null) inp.value = _panelInput[appId];
      const res = document.getElementById('qr-' + appId);
      if (res && _panelResult[appId]) {
        res.textContent = _panelResult[appId].text;
        if (_panelResult[appId].show) res.classList.add('show');
      }
    }
  });
  // 恢复调度历史面板
  _openHistoryPanels.forEach(appId => {
    const panel = document.getElementById('shp-' + appId);
    if (panel) panel.classList.add('open');
  });
  // 恢复编辑面板
  _openEditPanels.forEach(appId => {
    const panel = document.getElementById('ep-' + appId);
    if (panel) {
      panel.classList.add('open');
      const nameEl = document.getElementById('en-' + appId);
      if (nameEl && _editName[appId] != null) nameEl.value = _editName[appId];
      const skillsEl = document.getElementById('es-' + appId);
      if (skillsEl && _editSkills[appId] != null) skillsEl.value = _editSkills[appId];
    }
  });
}

async function loadApps() {
  // 如果有查询面板正在展开，先保存状态
  _saveQueryState();
  // 查询请求执行期间暂停自动刷新，避免重绘后结果写回旧 DOM 节点
  if (_pendingQueries.size > 0) {
    document.getElementById('apps-refresh-hint').textContent = `(查询执行中，刷新暂停) ${fmtTime()}`;
    return;
  }
  // 如果有面板展开且用户正在交互（输入框有焦点），跳过本次刷新避免打断
  const _activeId = document.activeElement && document.activeElement.id;
  if (_activeId && (_activeId.startsWith('qi-') || _activeId.startsWith('en-') || _activeId.startsWith('es-'))) {
    document.getElementById('apps-refresh-hint').textContent = `(编辑中，刷新暂停) ${fmtTime()}`;
    return;
  }
  if (_openEditPanels.size > 0) {
    document.getElementById('apps-refresh-hint').textContent = `(编辑面板开启，刷新暂停) ${fmtTime()}`;
    return;
  }
  try {
    const res = await fetch(`${API}/api/apps/`);
    const data = await res.json();
    renderApps(data.apps || []);
    _restoreQueryState();
    document.getElementById('apps-refresh-hint').textContent = `上次更新 ${fmtTime()}`;
  } catch(e) {
    document.getElementById('apps-tbody').innerHTML =
      `<tr class="empty-row"><td colspan="5">❌ 加载失败: ${e.message}</td></tr>`;
  }
}

function renderApps(apps) {
  const tbody = document.getElementById('apps-tbody');
  if (!apps.length) {
    tbody.innerHTML = '<tr class="empty-row"><td colspan="5">暂无应用，请先安装</td></tr>';
    return;
  }
  const _hasScheduleConfig = (a) => a.guidance_file && a.guidance_file.constraints && a.guidance_file.constraints.schedule_interval_seconds > 0;
  tbody.innerHTML = apps.map(a => {
    const hasSched = _hasScheduleConfig(a);
    const schedInterval = hasSched ? a.guidance_file.constraints.schedule_interval_seconds : 0;
    return `
    <tr id="app-row-${a.app_id}">
      <td><strong>${escHtml(a.name)}</strong></td>
      <td style="font-family:monospace;font-size:12px;color:#778">${a.app_id}</td>
      <td>${statusBadge(a.status)}${a.status==='scheduled'?`<div style="font-size:11px;color:#7c3aed;margin-top:2px">每 ${schedInterval}s</div>`:''}</td>
      <td style="font-family:monospace;font-size:12px;color:#aaa">${a.workflow_handle||'—'}</td>
      <td>
        <div style="display:flex;gap:6px;flex-wrap:wrap">
          ${a.status==='idle'||a.status==='stopped'
            ? `<button class="btn btn-success btn-sm" onclick="startApp('${a.app_id}')">▶ 启动</button>`
            : ''}
          ${(a.status==='idle'||a.status==='stopped') && hasSched
            ? `<button class="btn btn-sm" style="background:#7c3aed;color:#fff" onclick="startSchedule('${a.app_id}')">⏱ 定时启动</button>`
            : ''}
          ${a.status==='scheduled'
            ? `<button class="btn btn-danger btn-sm" onclick="stopSchedule('${a.app_id}')">⏹ 停止调度</button>
               <button class="btn btn-ghost btn-sm" onclick="toggleScheduleHistory('${a.app_id}')">📋 历史</button>`
            : ''}
          ${a.status==='running'
            ? `<button class="btn btn-warning btn-sm" onclick="toggleQuery('${a.app_id}')">💬 查询</button>
               <button class="btn btn-danger btn-sm" onclick="stopApp('${a.app_id}')">⏹ 停止</button>`
            : ''}
          ${hasSched && (a.status==='idle'||a.status==='stopped')
            ? `<button class="btn btn-ghost btn-sm" onclick="toggleScheduleHistory('${a.app_id}')">📋 历史</button>`
            : ''}
          <button class="btn btn-ghost btn-sm" onclick="toggleEdit('${a.app_id}')">✏️</button>
          <button class="btn btn-ghost btn-sm" onclick="uninstallApp('${a.app_id}')">🗑</button>
        </div>
        <!-- 编辑面板 -->
        <div class="query-panel" id="ep-${a.app_id}">
          <div style="display:flex;flex-direction:column;gap:8px;padding-top:8px">
            <div style="display:flex;gap:8px">
              <div style="flex:1">
                <label style="font-size:12px;color:#778">应用名称</label>
                <input id="en-${a.app_id}" type="text" value="${escHtml(a.name)}" style="width:100%;border:1px solid #d0d6e0;border-radius:6px;padding:6px 8px;font-size:13px">
              </div>
            </div>
            <div>
              <label style="font-size:12px;color:#778">Skills.md 内容</label>
              <textarea id="es-${a.app_id}" style="width:100%;border:1px solid #d0d6e0;border-radius:6px;padding:6px 8px;font-size:12px;font-family:monospace;resize:vertical;min-height:80px" placeholder="# 应用技能&#10;## Pipeline&#10;search:描述 -> nlp:描述">${escHtml((a.guidance_file && a.guidance_file.skills_content) || '')}</textarea>
            </div>
            <div style="display:flex;gap:8px;justify-content:flex-end">
              <button class="btn btn-ghost btn-sm" onclick="toggleEdit('${a.app_id}')">取消</button>
              <button class="btn btn-primary btn-sm" onclick="updateApp('${a.app_id}')">保存</button>
            </div>
          </div>
        </div>
        <!-- 查询面板 -->
        <div class="query-panel" id="qp-${a.app_id}">
          <div style="display:flex;gap:8px;align-items:flex-start">
            <textarea id="qi-${a.app_id}" style="flex:1;border:1px solid #d0d6e0;border-radius:6px;padding:8px;font-size:13px;resize:vertical;min-height:60px" placeholder="输入查询内容..."></textarea>
            <button class="btn btn-primary btn-sm" id="qbtn-${a.app_id}" onclick="sendQuery('${a.app_id}')">发送</button>
          </div>
          <div class="query-result" id="qr-${a.app_id}"></div>
        </div>
        <!-- 调度历史面板 -->
        <div class="query-panel" id="shp-${a.app_id}">
          <div style="display:flex;justify-content:space-between;align-items:center;margin-bottom:8px">
            <strong style="font-size:13px">调度执行历史</strong>
            <div style="display:flex;gap:6px;align-items:center">
              <span id="sh-status-${a.app_id}" style="font-size:11px;color:#778"></span>
              <button class="btn btn-ghost btn-sm" onclick="loadScheduleHistory('${a.app_id}')">🔄</button>
            </div>
          </div>
          <div id="sh-body-${a.app_id}"><div style="color:#aaa;font-size:12px">加载中...</div></div>
        </div>
      </td>
    </tr>
  `}).join('');
}

function escHtml(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}

async function installApp() {
  const name = document.getElementById('f-name').value.trim();
  const task = document.getElementById('f-task').value.trim();
  if (!name || !task) { showAlert('apps-alert','error','请填写应用名称和任务描述'); return; }
  const agentsRaw = document.getElementById('f-agents').value.trim();
  const agents = agentsRaw ? agentsRaw.split(',').map(s=>s.trim()).filter(Boolean) : [];
  const skillsMd = document.getElementById('f-skills').value.trim() || null;
  const schedInterval = parseInt(document.getElementById('f-schedule-interval').value)||0;
  const schedParallel = parseInt(document.getElementById('f-schedule-parallel').value)||5;
  const schedAutoRestart = document.getElementById('f-schedule-autorestart').checked;
  const constraints = { timeout_seconds: parseInt(document.getElementById('f-timeout').value)||120 };
  if (schedInterval > 0) {
    constraints.schedule_interval_seconds = schedInterval;
    constraints.schedule_max_parallel = schedParallel;
    constraints.schedule_auto_restart = schedAutoRestart;
  }
  const body = {
    name,
    task_description: task,
    orchestration_mode: document.getElementById('f-mode').value,
    agents_required: agents,
    constraints,
    skills_md: skillsMd
  };
  try {
    const res = await fetch(`${API}/api/apps/install`, {
      method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail||JSON.stringify(data));
    showAlert('apps-alert','success',`✅ 安装成功：${data.name}（${data.app_id}）`);
    document.getElementById('f-name').value='';
    document.getElementById('f-task').value='';
    loadApps();
  } catch(e) {
    showAlert('apps-alert','error',`❌ 安装失败：${e.message}`);
  }
}

async function startApp(appId) {
  try {
    const res = await fetch(`${API}/api/apps/${appId}/start`, {method:'POST'});
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail||JSON.stringify(data));
    showAlert('apps-alert','success',`▶ 启动成功：${data.workflow_handle}`);
    loadApps();
  } catch(e) {
    showAlert('apps-alert','error',`❌ 启动失败：${e.message}`);
  }
}

async function stopApp(appId) {
  try {
    const res = await fetch(`${API}/api/apps/${appId}/stop`, {method:'POST'});
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail||JSON.stringify(data));
    showAlert('apps-alert','success','⏹ 应用已停止');
    loadApps();
  } catch(e) {
    showAlert('apps-alert','error',`❌ 停止失败：${e.message}`);
  }
}

async function uninstallApp(appId) {
  if (!confirm(`确认卸载应用 ${appId}？`)) return;
  try {
    const res = await fetch(`${API}/api/apps/${appId}`, {method:'DELETE'});
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail||JSON.stringify(data));
    showAlert('apps-alert','success','🗑 卸载成功');
    loadApps();
  } catch(e) {
    showAlert('apps-alert','error',`❌ 卸载失败：${e.message}`);
  }
}

function toggleQuery(appId) {
  const panel = document.getElementById(`qp-${appId}`);
  panel.classList.toggle('open');
  if (panel.classList.contains('open')) {
    _openPanels.add(appId);
    // 自动聚焦输入框
    const inp = document.getElementById('qi-' + appId);
    if (inp) setTimeout(() => inp.focus(), 50);
  } else {
    _openPanels.delete(appId);
  }
}

function toggleEdit(appId) {
  const panel = document.getElementById(`ep-${appId}`);
  panel.classList.toggle('open');
  if (panel.classList.contains('open')) {
    _openEditPanels.add(appId);
    // 初始化缓存（避免首次进入覆盖 API 值）
    const nameEl = document.getElementById('en-' + appId);
    if (nameEl && _editName[appId] == null) _editName[appId] = nameEl.value;
    const skillsEl = document.getElementById('es-' + appId);
    if (skillsEl && _editSkills[appId] == null) _editSkills[appId] = skillsEl.value;
    setTimeout(() => { if (nameEl) nameEl.focus(); }, 50);
  } else {
    _openEditPanels.delete(appId);
    // 关闭时清除缓存，下次重新从 API 数据填充
    delete _editName[appId];
    delete _editSkills[appId];
  }
}

async function updateApp(appId) {
  const name = document.getElementById(`en-${appId}`).value.trim() || null;
  const skillsMd = document.getElementById(`es-${appId}`).value.trim() || null;
  try {
    const res = await fetch(`${API}/api/apps/${appId}`, {
      method: 'PATCH',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({ name, skills_md: skillsMd }),
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail || JSON.stringify(data));
    showAlert('apps-alert', 'success', `✅ 更新成功：${appId}`);
    document.getElementById(`ep-${appId}`).classList.remove('open');
    loadApps();
  } catch(e) {
    showAlert('apps-alert', 'error', `❌ 更新失败：${e.message}`);
  }
}

async function sendQuery(appId) {
  const input = document.getElementById(`qi-${appId}`);
  const query = input.value.trim();
  if (!query) return;
  const btn = document.getElementById(`qbtn-${appId}`);
  const resultEl = document.getElementById(`qr-${appId}`);
  _pendingQueries.add(appId);
  btn.disabled = true;
  btn.innerHTML = '<span class="spinner"></span>';
  resultEl.className = 'query-result show';
  resultEl.textContent = '⏳ 执行中，请稍候...';
  _panelResult[appId] = { text: resultEl.textContent, show: true };
  try {
    const res = await fetch(`${API}/api/apps/${appId}/interface`, {
      method:'POST', headers:{'Content-Type':'application/json'},
      body: JSON.stringify({query})
    });
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail||JSON.stringify(data));
    let text;
    if (typeof data.result === 'string') {
      text = data.result;
    } else if (data.result && Array.isArray(data.result.messages) && data.result.messages.length > 0) {
      // 从后往前找第一条非用户消息（type 不是 human，role 不是 user）
      const msgs = data.result.messages;
      const aiMsg = [...msgs].reverse().find(m =>
        m.type !== 'human' && m.role !== 'user'
      ) || msgs[msgs.length - 1]; // fallback：直接取最后一条
      const c = aiMsg.content;
      text = typeof c === 'string' ? c : JSON.stringify(c, null, 2);
    } else {
      text = JSON.stringify(data, null, 2);
    }
    resultEl.textContent = text;
    _panelResult[appId] = { text, show: true };
  } catch(e) {
    const text = `❌ 错误：${e.message}`;
    resultEl.textContent = text;
    _panelResult[appId] = { text, show: true };
  } finally {
    _pendingQueries.delete(appId);
    btn.disabled = false;
    btn.innerHTML = '发送';
  }
}

// ====================================================================
// Schedule controls
// ====================================================================
async function startSchedule(appId) {
  try {
    const res = await fetch(`${API}/api/apps/${appId}/schedule/start`, {method:'POST'});
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail||JSON.stringify(data));
    showAlert('apps-alert','success','⏱ 周期调度已启动');
    loadApps();
  } catch(e) {
    showAlert('apps-alert','error',`❌ 启动调度失败：${e.message}`);
  }
}

async function stopSchedule(appId) {
  try {
    const res = await fetch(`${API}/api/apps/${appId}/schedule/stop`, {method:'POST'});
    const data = await res.json();
    if (!res.ok) throw new Error(data.detail||JSON.stringify(data));
    showAlert('apps-alert','success','⏹ 周期调度已停止');
    loadApps();
  } catch(e) {
    showAlert('apps-alert','error',`❌ 停止调度失败：${e.message}`);
  }
}

const _openHistoryPanels = new Set();

function toggleScheduleHistory(appId) {
  const panel = document.getElementById(`shp-${appId}`);
  if (!panel) return;
  panel.classList.toggle('open');
  if (panel.classList.contains('open')) {
    _openHistoryPanels.add(appId);
    loadScheduleHistory(appId);
  } else {
    _openHistoryPanels.delete(appId);
  }
}

function schedBadge(s) {
  return `<span class="sched-badge sb-${s}">${s}</span>`;
}

async function loadScheduleHistory(appId) {
  const body = document.getElementById(`sh-body-${appId}`);
  const statusEl = document.getElementById(`sh-status-${appId}`);
  if (!body) return;
  try {
    // Fetch status and history in parallel
    const [statusRes, histRes] = await Promise.all([
      fetch(`${API}/api/apps/${appId}/schedule/status`),
      fetch(`${API}/api/apps/${appId}/schedule/history?limit=20`)
    ]);
    const statusData = await statusRes.json();
    const histData = await histRes.json();

    // Show status summary
    if (statusData.scheduled) {
      statusEl.innerHTML = `<span style="color:#7c3aed">活跃 ${statusData.active_runs||0} 个 / 累计 ${statusData.total_runs||0} 次</span>`;
    } else {
      statusEl.textContent = '未调度';
    }

    const records = histData.records || [];
    if (!records.length) {
      body.innerHTML = '<div style="color:#aaa;font-size:12px;padding:8px 0">暂无执行记录</div>';
      return;
    }
    body.innerHTML = `<table class="sched-history" style="width:100%">
      <thead><tr><th>Run ID</th><th>状态</th><th>开始时间</th><th>完成时间</th><th>耗时</th></tr></thead>
      <tbody>${records.map(r => {
        const startT = r.started_at ? new Date(r.started_at).toLocaleString('zh-CN') : '—';
        const endT = r.finished_at ? new Date(r.finished_at).toLocaleString('zh-CN') : '—';
        let duration = '—';
        if (r.started_at && r.finished_at) {
          const ms = new Date(r.finished_at) - new Date(r.started_at);
          duration = ms < 1000 ? ms+'ms' : (ms/1000).toFixed(1)+'s';
        } else if (r.status === 'running') {
          duration = '<span class="spinner" style="width:10px;height:10px;border-width:1.5px;border-color:rgba(124,58,237,.3);border-top-color:#7c3aed"></span>';
        }
        return `<tr>
          <td style="font-family:monospace">${r.run_id}</td>
          <td>${schedBadge(r.status)}</td>
          <td>${startT}</td>
          <td>${endT}</td>
          <td>${duration}</td>
        </tr>`;
      }).join('')}</tbody>
    </table>`;
  } catch(e) {
    body.innerHTML = `<div style="color:#ef4444;font-size:12px">加载失败：${e.message}</div>`;
  }
}

// ====================================================================
// Tab: INSTANCES
// ====================================================================
async function loadInstances() {
  try {
    const res = await fetch(`${API}/api/agents/instances`);
    const data = await res.json();
    const rows = data.instances || [];
    const tbody = document.getElementById('inst-tbody');
    document.getElementById('inst-refresh-hint').textContent = `上次更新 ${fmtTime()}`;
    if (!rows.length) {
      tbody.innerHTML = '<tr class="empty-row"><td colspan="6">暂无 Agent 实例</td></tr>';
      return;
    }
    tbody.innerHTML = rows.map(r => `
      <tr>
        <td style="font-family:monospace;font-size:12px">${r.instance_id||'—'}</td>
        <td>${escHtml(r.agent_id||'—')}</td>
        <td style="font-size:12px;color:#778">${r.image_id||'—'}</td>
        <td>${statusBadge(r.status||'unknown')}</td>
        <td style="text-align:center">${r.ref_count??'—'}</td>
        <td style="font-size:12px;color:#778">${(r.subscribed_workflows||[]).join(', ')||'—'}</td>
      </tr>
    `).join('');
  } catch(e) {
    document.getElementById('inst-tbody').innerHTML =
      `<tr class="empty-row"><td colspan="6">❌ ${e.message}</td></tr>`;
  }
}

// ====================================================================
// Tab: QoS
// ====================================================================
async function loadQos() {
  try {
    const res = await fetch(`${API}/api/qos/metrics`);
    const data = await res.json();
    document.getElementById('qos-refresh-hint').textContent = `上次更新 ${fmtTime()}`;
    // summary cards
    const s = data.summary || {};
    document.getElementById('qos-metrics-row').innerHTML = `
      <div class="metric-card">
        <div class="metric-label">监控 Agent 数</div>
        <div class="metric-value">${s.total_agents??0}</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">告警 Agent 数</div>
        <div class="metric-value" style="color:${(s.alert_agents_count||0)>0?'#ef4444':'#22c55e'}">${s.alert_agents_count??0}</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">总调用次数</div>
        <div class="metric-value">${s.total_calls??0}</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">整体成功率</div>
        <div class="metric-value">${s.overall_success_rate!=null?(s.overall_success_rate*100).toFixed(1)+'%':'—'}</div>
      </div>
    `;
    // table
    const metrics = data.metrics || [];
    const tbody = document.getElementById('qos-tbody');
    if (!metrics.length) {
      tbody.innerHTML = '<tr class="empty-row"><td colspan="8">暂无 QoS 数据（需先有 Agent 调用记录）</td></tr>';
      return;
    }
    tbody.innerHTML = metrics.map(m => {
      const alerting = m.is_alerting;
      const sr = m.success_rate != null ? (m.success_rate*100).toFixed(1)+'%' : '—';
      return `<tr class="${alerting?'warn-row':''}">
        <td><strong>${escHtml(m.agent_id)}</strong></td>
        <td>${m.total_calls??0}</td>
        <td style="color:#16a34a">${m.success_count??0}</td>
        <td style="color:#dc2626">${m.failure_count??0}</td>
        <td>${m.avg_latency_ms!=null?m.avg_latency_ms.toFixed(0):'-'}</td>
        <td>${m.max_latency_ms!=null?m.max_latency_ms.toFixed(0):'-'}</td>
        <td>${sr}</td>
        <td>${alerting?'<span style="color:#ef4444;font-weight:600">⚠️ 告警</span>':'<span style="color:#16a34a">✅ 正常</span>'}</td>
      </tr>`;
    }).join('');
  } catch(e) {
    document.getElementById('qos-tbody').innerHTML =
      `<tr class="empty-row"><td colspan="8">❌ ${e.message}</td></tr>`;
  }
}

// ====================================================================
// Tab: Resources
// ====================================================================
async function loadResources() {
  try {
    const res = await fetch(`${API}/api/resources/`);
    const data = await res.json();
    document.getElementById('res-refresh-hint').textContent = `上次更新 ${fmtTime()}`;
    const s = data.summary || {};
    document.getElementById('res-summary-row').innerHTML = `
      <div class="metric-card">
        <div class="metric-label">总节点数</div>
        <div class="metric-value">${s.total_nodes??0}</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">可用节点</div>
        <div class="metric-value" style="color:#22c55e">${s.available_nodes??0}</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">可用 CPU 核</div>
        <div class="metric-value">${s.available_cpu!=null?s.available_cpu.toFixed(1):0}</div>
      </div>
      <div class="metric-card">
        <div class="metric-label">可用内存 (MB)</div>
        <div class="metric-value">${s.available_memory_mb??0}</div>
      </div>
    `;
    const nodes = data.nodes || [];
    const tbody = document.getElementById('res-tbody');
    if (!nodes.length) {
      tbody.innerHTML = '<tr class="empty-row"><td colspan="7">暂无节点数据</td></tr>';
      return;
    }
    tbody.innerHTML = nodes.map(n => {
      const cpuUsed = (n.cpu_total - n.cpu_available)||0;
      const memUsed = (n.memory_total_mb - n.memory_available_mb)||0;
      return `<tr>
        <td><strong>${escHtml(n.node_id)}</strong></td>
        <td style="font-family:monospace;font-size:12px">${n.ip||'—'}</td>
        <td>${n.node_type||'—'}</td>
        <td>
          <div>${cpuUsed.toFixed(1)} / ${(n.cpu_total||0).toFixed(1)}</div>
          <div style="height:5px;background:#e8ecf2;border-radius:3px;margin-top:4px">
            <div style="height:100%;width:${Math.min(100,(cpuUsed/(n.cpu_total||1)*100)).toFixed(0)}%;background:#4a6cf7;border-radius:3px"></div>
          </div>
        </td>
        <td>
          <div>${memUsed} / ${n.memory_total_mb||0}</div>
          <div style="height:5px;background:#e8ecf2;border-radius:3px;margin-top:4px">
            <div style="height:100%;width:${Math.min(100,(memUsed/(n.memory_total_mb||1)*100)).toFixed(0)}%;background:#22c55e;border-radius:3px"></div>
          </div>
        </td>
        <td>${n.gpu_count||0}</td>
        <td>${statusBadge(n.status||'unknown')}</td>
      </tr>`;
    }).join('');
  } catch(e) {
    document.getElementById('res-tbody').innerHTML =
      `<tr class="empty-row"><td colspan="7">❌ ${e.message}</td></tr>`;
  }
}

// ====================================================================
// Auto-refresh & init
// ====================================================================
async function checkServer() {
  try {
    const res = await fetch(`${API}/api/apps/`);
    document.getElementById('server-status').textContent = res.ok ? '● 已连接' : '● 异常';
    document.getElementById('server-status').style.color = res.ok ? '#4ade80' : '#f87171';
  } catch {
    document.getElementById('server-status').textContent = '● 离线';
    document.getElementById('server-status').style.color = '#f87171';
  }
}

// Load initial tab and start auto-refresh
loadApps();
checkServer();
setInterval(() => TAB_LOADERS[activeTab](), 5000);
setInterval(checkServer, 10000);
</script>
</body>
</html>"""
