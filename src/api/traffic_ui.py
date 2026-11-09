"""Dependency-free traffic replay UI. Model work starts through HTTP only."""


def get_traffic_ui_html() -> str:
    return r"""<!DOCTYPE html>
<html lang="zh-CN">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>LangManus · 交通预测任务</title>
<link rel="icon" href="data:,">
<style>
* { box-sizing: border-box; }
body { margin: 0; background: #f0f2f5; color: #28354a; font: 16px/1.45 -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", sans-serif; }
button, input, select { font: inherit; }
button, a, input, select { outline-offset: 3px; }
button { cursor: pointer; }
button:disabled { cursor: not-allowed; opacity: .5; }
[hidden] { display: none !important; }
.demo { width: min(1000px, calc(100% - 32px)); margin: 20px auto; background: #fff; border-radius: 14px; box-shadow: 0 4px 24px #1a1a2e0d; overflow: hidden; }
.header { padding: 18px 24px 14px; display: flex; align-items: center; justify-content: space-between; gap: 16px; border-bottom: 1px solid #e8ecf2; }
.brand { display: flex; gap: 13px; align-items: center; }
.brand-icon { width: 44px; height: 44px; border-radius: 10px; background: #edf2ff; color: #4a6cf7; padding: 8px; }
h1 { margin: 0 0 4px; font-size: 25px; color: #1a1a2e; letter-spacing: .3px; }
h2 { margin: 0 0 12px; font-size: 18px; color: #1a1a2e; }
p { margin: 0; }
.muted { color: #64748b; }
.header-right { text-align: right; flex-shrink: 0; }
.header-right a { color: #4a6cf7; text-decoration: none; display: block; margin-top: 5px; }
.badge { display: inline-block; padding: 3px 9px; background: #eef2f7; color: #526178; border-radius: 6px; white-space: nowrap; }
.good { background: #e5f6ed; color: #187447; }
.bad { background: #fcebea; color: #a12e32; }
.waiting { background: #fff4da; color: #88601a; }
.tabs { display: grid; grid-template-columns: repeat(4, 1fr); padding: 12px 24px 0; gap: 8px; background: #fafbfe; }
.tab { border: 1px solid transparent; background: transparent; padding: 10px 4px 12px; color: #64748b; font-size: 17px; border-radius: 8px 8px 0 0; }
.tab[aria-selected="true"] { background: #fff; color: #3558df; border-color: #e8ecf2; border-bottom-color: #fff; font-weight: 600; }
.tab-number { margin-right: 7px; color: #8b9ab1; }
.tab[aria-selected="true"] .tab-number { color: #4a6cf7; }
.view { padding: 16px 24px; }
.card { border: 1px solid #e4e9f2; border-radius: 10px; padding: 16px; background: #fff; }
.card + .card { margin-top: 14px; }
.card-head { display: flex; align-items: center; justify-content: space-between; gap: 10px; margin-bottom: 12px; }
.card-head h2 { margin: 0; }
.form-grid { display: grid; grid-template-columns: repeat(4, minmax(0, 1fr)); gap: 12px 16px; }
label { display: block; color: #526178; }
label > input, label > select { margin-top: 5px; width: 100%; }
input, select { min-width: 0; border: 1px solid #d0d6e0; border-radius: 6px; padding: 7px 9px; background: #fafbfc; color: #28354a; height: 39px; }
input:focus, select:focus { border-color: #4a6cf7; background: #fff; }
input[readonly] { color: #526178; background: #f3f6fb; }
.note { margin-top: 12px; padding: 9px 12px; border-radius: 7px; background: #f4f7fd; color: #526178; }
table { border-collapse: collapse; width: 100%; font-size: 16px; }
th { text-align: left; color: #526178; font-weight: 500; background: #f6f8fc; }
th, td { padding: 7px 9px; border-bottom: 1px solid #e8ecf2; vertical-align: top; }
tbody tr:last-child td { border-bottom: 0; }
.tensor { white-space: nowrap; color: #3558b4; font-variant-numeric: tabular-nums; }
.subline { display: block; color: #64748b; }
.cap-table th:nth-child(1) { width: 10%; }
.cap-table th:nth-child(2), .cap-table th:nth-child(3) { width: 27%; }
.cap-table th, .cap-table td { padding: 5px 7px; }
[data-panel="config"] .card, [data-panel="execution"] .card { padding: 14px; }
[data-panel="config"] .card-head, [data-panel="execution"] .card-head { margin-bottom: 10px; }
.cap-table .subline { display: inline; }
.split { display: grid; grid-template-columns: 1fr 1fr; gap: 16px; }
.dag { width: 100%; height: 204px; display: block; }
.dag text { font-size: 18px; }
svg text { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", "PingFang SC", sans-serif; font-size: 16px; }
.statuses th, .statuses td { padding: 4px; white-space: nowrap; }
.statuses .badge { padding: 0 5px; line-height: 1.3; }
.trace-table th:nth-child(1) { width: 10%; }
.trace-table th:nth-child(2) { width: 29%; }
.trace-table th:nth-child(3) { width: 41%; }
.trace-table th:nth-child(4) { width: 20%; }
.trace-table { line-height: 1.3; }
.trace-table td { padding-top: 4px; padding-bottom: 4px; }
.metrics { display: grid; grid-template-columns: repeat(3, 1fr); gap: 14px; }
.metric { background: #f7f9ff; border: 1px solid #e0e7f6; border-radius: 9px; padding: 13px 15px; }
.metric-value { font-size: 27px; font-weight: 650; color: #1a1a2e; margin: 3px 0; font-variant-numeric: tabular-nums; }
.metric-value small { font-size: 16px; font-weight: 400; color: #64748b; }
.quota-chart { width: 100%; height: 246px; display: block; }
.pager { display: flex; align-items: center; justify-content: space-between; margin-top: 10px; }
.ghost { color: #4a6cf7; background: #f4f7fd; border: 1px solid #dce4f5; border-radius: 6px; padding: 4px 10px; }
.ledger th, .ledger td { padding: 6px 5px; white-space: nowrap; }
.ledger .badge { padding: 1px 5px; }
.result-controls { display: grid; grid-template-columns: 1fr 1.2fr 1.5fr; gap: 14px; margin-bottom: 14px; }
.result-split { display: grid; grid-template-columns: 43% 1fr; gap: 16px; }
.heatmap { width: 100%; height: 280px; display: block; }
.forecast-chart { width: 100%; height: 195px; display: block; }
.legend { display: flex; align-items: center; gap: 10px; margin-top: 8px; color: #526178; }
.legend-scale { height: 10px; border-radius: 4px; background: linear-gradient(90deg, #eef4ff, #2455c4); flex: 1; }
.dot { display: inline-block; width: 10px; height: 10px; border-radius: 50%; margin-right: 6px; }
.forecast-legend { display: flex; gap: 20px; margin-bottom: 5px; flex-wrap: wrap; }
.result-detail { margin-top: 12px; padding-top: 10px; border-top: 1px solid #e8ecf2; }
.result-detail th, .result-detail td { padding: 5px 9px; }
.empty { padding: 42px 18px; text-align: center; color: #64748b; border: 1px dashed #d6deeb; border-radius: 10px; }
.error { margin: 14px 24px 0; padding: 11px 14px; background: #fff0ef; border: 1px solid #f4cecb; color: #a12e32; border-radius: 8px; }
.footer { display: flex; align-items: center; justify-content: space-between; gap: 16px; padding: 12px 24px 14px; border-top: 1px solid #e8ecf2; }
.footer-left { display: flex; align-items: center; gap: 12px; min-width: 0; }
.primary { border: 0; background: #4a6cf7; color: #fff; border-radius: 7px; padding: 10px 20px; white-space: nowrap; }
@media (max-width: 820px) { .split, .result-split { grid-template-columns: 1fr; } .form-grid { grid-template-columns: 1fr 1fr; } .card { overflow-x: auto; } .header { align-items: flex-start; } .header-right a { font-size: 16px; } .footer-left { flex-wrap: wrap; } }
</style>
</head>
<body>
<main id="traffic-demo" class="demo" data-shot="config">
  <header class="header">
    <div class="brand">
      <svg class="brand-icon" viewBox="0 0 28 28" aria-hidden="true"><path d="M3 6h22M3 14h22M3 22h22M6 3v22M14 3v22M22 3v22" fill="none" stroke="currentColor" stroke-width="1.5"/><circle cx="14" cy="14" r="4" fill="currentColor"/></svg>
      <div><h1>LangManus · 交通预测任务</h1><p class="muted">历史数据回放 · NYC TLC Green · 2024 年 1 月</p></div>
    </div>
    <div class="header-right"><span id="service-ready" class="badge waiting" data-ready="false" role="status">正在连接模型服务</span><button id="refresh-service" class="ghost" hidden>重新连接</button><a href="/traffic/platform/ui">智能体与资源联合编排 ↗</a><a id="app-management-link" href="/ui" hidden>应用管理控制台 ↗</a></div>
  </header>
  <nav class="tabs" role="tablist" aria-label="交通任务演示">
    <button id="tab-config" class="tab" role="tab" aria-selected="true" aria-controls="view-config" data-tab="config" data-view="config"><span class="tab-number">01</span>配置</button>
    <button id="tab-execution" class="tab" role="tab" aria-selected="false" aria-controls="view-execution" data-tab="execution" data-view="execution" tabindex="-1"><span class="tab-number">02</span>执行与契约</button>
    <button id="tab-resources" class="tab" role="tab" aria-selected="false" aria-controls="view-resources" data-tab="resources" data-view="resources" tabindex="-1"><span class="tab-number">03</span>资源配额</button>
    <button id="tab-results" class="tab" role="tab" aria-selected="false" aria-controls="view-results" data-tab="results" data-view="results" tabindex="-1"><span class="tab-number">04</span>预测结果</button>
  </nav>
  <div id="error-message" class="error" role="alert" hidden></div>
  <section id="view-config" class="view" data-panel="config" role="tabpanel" aria-labelledby="tab-config">
    <form id="traffic-form" novalidate>
      <div class="card">
        <div class="card-head"><h2>回放任务配置</h2><span id="dataset-summary" class="muted">30 分钟 / 帧 · 6 × 6 网格</span></div>
        <div class="form-grid">
          <label>数据集<select id="data-ref" aria-label="数据集"><option value="nyc_tlc_green_2024_01_6x6">NYC Green · 2024 Jan</option></select></label>
          <label>首个目标时间<input id="target-time" type="datetime-local" value="2024-01-27T08:00" step="1800" required></label>
          <label>回放样本数<select id="batch-size" required><option value="1">1</option><option value="4">4</option><option value="8" selected>8</option><option value="16">16</option><option value="32">32</option></select></label>
          <label>预测任务<select id="task-select"><option value="all">all · 三任务联合</option><option value="state">state · 交通状态</option><option value="flow">flow · 流入 / 流出</option><option value="od">od · 起终点关系</option></select></label>
          <label>调度策略<select id="policy-select" disabled><option value="drc">DRC-Sched</option></select></label>
          <label>CPU 配额<input id="cpu-budget" type="number" value="4" min="0.01" max="4" step="0.01" required></label>
          <label>总内存配额 (MiB)<input id="memory-budget" type="number" value="2048" min="513" max="2048" step="1" required></label>
          <label>最大并发<input id="max-concurrency" type="number" value="4" min="1" max="4" step="1" required></label>
        </div>
        <input id="model-version" type="hidden" value="">
        <p id="config-quota" class="note">连接服务后读取模型基础内存与任务可用配额。</p>
      </div>
    </form>
    <div class="card">
      <div class="card-head"><h2>模型能力与数据契约</h2><span id="contract-summary" class="muted">读取服务能力中</span></div>
      <table class="cap-table"><thead><tr><th>能力</th><th>输入轴 / shape</th><th>输出轴 / shape</th><th>通道与单位</th></tr></thead><tbody id="capability-rows"><tr><td colspan="4">等待模型服务返回能力。</td></tr></tbody></table>
      <p id="normalization-note" class="note">历史数据按朴素年月日解析；每个目标需具备同日历史，不转换时区。</p>
    </div>
  </section>
  <section id="view-execution" class="view" data-panel="execution" role="tabpanel" aria-labelledby="tab-execution" hidden>
    <div id="execution-content" class="empty">运行回放任务后查看节点执行与契约适配记录。</div>
  </section>
  <section id="view-resources" class="view" data-panel="resources" role="tabpanel" aria-labelledby="tab-resources" hidden>
    <div id="resources-content" class="empty">运行回放任务后查看配额占用、峰值与释放账本。</div>
  </section>
  <section id="view-results" class="view" data-panel="results" role="tabpanel" aria-labelledby="tab-results" hidden>
    <div id="results-empty" class="empty">运行回放任务后查看各时间点的预测。</div>
    <div id="results-content" hidden>
      <div class="result-controls">
        <label>预测能力<select id="result-task"></select></label>
        <label>目标时间<select id="result-time"></select></label>
        <label>网格通道<select id="result-channel"></select></label>
      </div>
      <div class="result-split">
        <div class="card"><div class="card-head"><h2 id="heatmap-title">原始单位预测</h2><span>6 × 6</span></div><div id="heatmap"></div><div class="legend"><span id="heatmap-min"></span><span class="legend-scale"></span><span id="heatmap-max"></span></div><p id="heatmap-unit" class="muted" style="margin-top:9px"></p></div>
        <div class="card"><h2 id="forecast-title">流入 / 流出预测</h2><div id="forecast-legend" class="forecast-legend"></div><div id="forecast-chart"></div><p id="forecast-note" class="muted"></p><div id="result-detail" class="result-detail"></div></div>
      </div>
      <p id="result-summary" class="note"></p>
    </div>
  </section>
  <footer class="footer"><div class="footer-left"><span id="workflow-status" class="badge" data-status="idle" role="status">待执行</span><span id="run-summary" class="muted">批量一步预测 · 每样本使用此前 90 分钟历史</span></div><button id="run-btn" class="primary" type="submit" form="traffic-form" disabled>运行回放任务</button></footer>
</main>
<script>
(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  const fields = ['state', 'flow', 'od'];
  const names = {state: '交通状态', flow: '流入 / 流出', od: '起终点关系'};
  const policyNames = {drc: 'DRC-Sched', fcfs: 'FCFS'};
  const stageNames = {prepare: '历史准备', predict: '联合预测', validate_state: 'state 校验', validate_flow: 'flow 校验', validate_od: 'od 校验', aggregate: '结果汇总'};
  try {
    const source = new URL(document.referrer);
    $('app-management-link').hidden = !(source.origin === window.location.origin && source.pathname === '/ui');
  } catch (_) { /* An absent or invalid referrer leaves the return link hidden. */ }
  let capabilities = null, request = null, result = null, busy = false, ledgerPage = 0;
  Object.defineProperty(window, 'trafficDemo', {get: () => Object.freeze({capabilities, request, result})});
  const esc = value => String(value ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;', '<':'&lt;', '>':'&gt;', '"':'&quot;', "'":'&#39;'}[c]));
  const fmt = (value, digits = 2) => Number.isFinite(value) ? Number(value).toLocaleString('en-US', {maximumFractionDigits: digits}) : '—';
  const shape = values => '(' + values.map(v => v === null ? 'B' : v).join(',') + ')';
  const axes = values => values.join(',');
  const unit = value => ({'active trips':'在途行程数', 'mph':'mph', 'trips/30min':'行程/30min'}[value] || value);
  const channelName = (task, channel) => task === 'state' ? ['在途行程数', '行程平均速度'][channel] : task === 'flow' ? ['流入', '流出'][channel] : '终点 ' + channel;
  const clock = value => value.slice(11, 16);
  const dateTime = value => value.slice(5, 16).replace('T', ' ');
  function freeze(value) {
    if (value && typeof value === 'object' && !Object.isFrozen(value)) {
      Object.values(value).forEach(freeze);
      Object.freeze(value);
    }
    return value;
  }
  function errorMessage(message) { $('error-message').textContent = message; $('error-message').hidden = !message; }
  function status(value, text) {
    const node = $('workflow-status');
    node.dataset.status = value; node.textContent = text;
    node.className = 'badge ' + (value === 'success' ? 'good' : value === 'running' ? 'waiting' : value === 'idle' ? '' : 'bad');
  }
  function badge(value) {
    const style = ['success','valid','completed'].includes(value) ? 'good' : ['failed','invalid','cancelled','skipped'].includes(value) ? 'bad' : '';
    return '<span class="badge ' + style + '">' + esc(value ?? '—') + '</span>';
  }
  function switchView(view) {
    document.querySelectorAll('[data-panel]').forEach(node => { node.hidden = node.dataset.panel !== view; });
    document.querySelectorAll('[data-tab]').forEach(node => {
      const active = node.dataset.tab === view;
      node.setAttribute('aria-selected', String(active)); node.tabIndex = active ? 0 : -1;
    });
    $('traffic-demo').dataset.shot = view;
  }
  document.querySelectorAll('[data-tab]').forEach((node, index, tabs) => {
    node.addEventListener('click', () => switchView(node.dataset.tab));
    node.addEventListener('keydown', event => {
      if (!['ArrowLeft','ArrowRight','Home','End'].includes(event.key)) return;
      event.preventDefault();
      const next = event.key === 'Home' ? 0 : event.key === 'End' ? tabs.length - 1 : (index + (event.key === 'ArrowRight' ? 1 : -1) + tabs.length) % tabs.length;
      switchView(tabs[next].dataset.tab); tabs[next].focus();
    });
  });
  function httpError(code, detail) {
    if (code === 422) {
      const issues = Array.isArray(detail) ? detail : [];
      if (issues.some(x => (x.loc || []).includes('target_frames')) || /same.day|target_frames|history/i.test(String(detail))) return '目标时间缺少同日历史或超出数据范围，请调整时间和批次。';
      if (issues.some(x => (x.loc || []).includes('resources')) || /budget|memory|cpu|resource/i.test(String(detail))) return '资源配额无法满足任务请求，请增加任务内存或检查 CPU、并发上限。';
      return '任务参数未通过服务校验，请检查时间、批次和配额。';
    }
    if (code === 409) return '当前请求与服务状态冲突，请重新连接模型服务后重试。';
    if (code === 503) return '模型服务暂不可用，请稍后重新连接。';
    return '服务未完成请求（HTTP ' + code + '），请稍后重试。';
  }
  async function api(path, options) {
    let response;
    try { response = await fetch('/traffic' + path, options); }
    catch (_) { throw new Error('无法连接交通服务，请确认服务可用后重试。'); }
    let data;
    try { data = await response.json(); }
    catch (_) { throw new Error('服务返回内容无法读取，请重新连接后重试。'); }
    if (!response.ok) throw new Error(httpError(response.status, data.detail));
    return data;
  }
  function contract(task) { return capabilities.capabilities.find(item => item.name === task && item.version === capabilities.version); }
  function showQuota() {
    if (!capabilities) return;
    const total = Number($('memory-budget').value), base = capabilities.base_resources.memory_mb;
    $('config-quota').textContent = '模型基础 ' + fmt(base) + ' MiB + 任务可用 ' + (Number.isFinite(total) && total > base ? fmt(total - base) : '—') + ' MiB；CPU / 内存 / 并发均为调度配额。';
  }
  async function loadCapabilities() {
    if (busy) return;
    capabilities = null;
    $('service-ready').dataset.ready = 'false'; $('service-ready').className = 'badge waiting';
    $('service-ready').textContent = '正在连接模型服务'; $('run-btn').disabled = true; $('refresh-service').hidden = true;
    errorMessage('');
    try {
      const data = await api('/capabilities');
      if (!data.version || !Array.isArray(data.capabilities) || !data.data_refs?.includes($('data-ref').value) || data.frame_minutes !== 30 || data.horizon !== 1 || data.history_length !== 3 || !data.base_resources || !data.budgets || !Array.isArray(data.policies)) throw new Error('服务能力与当前回放数据不一致，请检查服务配置。');
      for (const task of fields) {
        const item = data.capabilities.find(x => x.name === task && x.version === data.version);
        if (!item?.inputs?.[task]?.shape || !item?.outputs?.[task]?.shape || !item.outputs[task].units?.length) throw new Error('服务未返回完整的三任务数据契约。');
      }
      capabilities = freeze(data);
      $('model-version').value = data.version;
      const previousBatch = $('batch-size').value;
      $('batch-size').innerHTML = Array.from({length:data.max_batch}, (_,i) => '<option value="' + (i+1) + '">' + (i+1) + '</option>').join('');
      $('batch-size').value = Number(previousBatch) <= data.max_batch ? previousBatch : String(data.max_batch);
      $('cpu-budget').max = String(data.budgets.cpu_budget);
      $('memory-budget').min = String(data.base_resources.memory_mb + 1);
      $('memory-budget').max = String(data.budgets.memory_budget);
      $('max-concurrency').max = String(data.budgets.max_concurrency);
      $('policy-select').innerHTML = data.policies.map(p => '<option value="' + esc(p) + '">' + esc(policyNames[p] || p) + '</option>').join('');
      $('policy-select').value = data.policies.includes('drc') ? 'drc' : data.policies[0]; $('policy-select').disabled = false;
      const grid = contract('state').outputs.state.shape;
      $('dataset-summary').textContent = data.frame_minutes + ' 分钟 / 帧 · ' + grid[2] + ' × ' + grid[3] + ' 网格';
      $('contract-summary').textContent = '模型 ' + data.version + ' · H=' + data.horizon;
      $('capability-rows').innerHTML = fields.map(task => {
        const item = contract(task), input = item.inputs[task], output = item.outputs[task];
        const units = task === 'od' ? '36 个终点 · ' + unit(output.units[0]) : task === 'flow' ? '流入 / 流出 · ' + unit(output.units[0]) : output.units.map((u, i) => channelName(task, i) + (u === 'active trips' ? '' : ' · ' + unit(u))).join('<br>');
        return '<tr><td title="' + esc(names[task]) + '"><strong>' + task + '</strong></td><td class="tensor">' + esc(axes(input.axes)) + ' <span class="subline">' + esc(shape(input.shape)) + '</span></td><td class="tensor">' + esc(axes(output.axes)) + ' <span class="subline">' + esc(shape(output.shape)) + '</span></td><td>' + units + '</td></tr>';
      }).join('');
      $('normalization-note').textContent = '输入 ' + data.normalization.method + '（' + data.normalization.split + ' 统计），输出 ' + contract('state').outputs.state.normalization + ' 原始单位。三能力共享历史输入；B 为批次，D 为终点通道。';
      showQuota();
      $('service-ready').textContent = '● 模型服务就绪'; $('service-ready').className = 'badge good'; $('service-ready').dataset.ready = 'true'; $('run-btn').disabled = false;
    } catch (error) {
      $('service-ready').textContent = '模型服务连接失败'; $('service-ready').className = 'badge bad'; $('refresh-service').hidden = false;
      errorMessage(error.message);
    }
  }
  $('refresh-service').addEventListener('click', loadCapabilities);
  $('memory-budget').addEventListener('input', showQuota);
  function numberField(id, label, lower, upper, integer = false) {
    const raw = $(id).value.trim(), value = Number(raw);
    if (!raw || !Number.isFinite(value) || value < lower || value > upper || (integer && !Number.isInteger(value))) throw new Error(label + '须为 ' + lower + '–' + upper + ' 范围内的' + (integer ? '整数。' : '数值。'));
    return value;
  }
  function makeRequest() {
    if (!capabilities) throw new Error('请等待模型服务就绪后运行任务。');
    const input = $('target-time').value;
    const match = /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2}):(\d{2})(?::00)?$/.exec(input);
    if (!match) throw new Error('请输入有效的年月日与时间，例如 2024-01-27 08:00。');
    const [year, month, day, hour, minute] = match.slice(1).map(Number);
    // UTC is used only to validate calendar components, never to shift dataset time.
    const calendar = new Date(Date.UTC(year, month - 1, day, hour, minute));
    if (calendar.getUTCFullYear() !== year || calendar.getUTCMonth() !== month - 1 || calendar.getUTCDate() !== day || hour > 23 || minute > 59) throw new Error('日期或时间无效，请检查年月日、小时和分钟。');
    if (year !== 2024 || month !== 1) throw new Error('当前数据集只覆盖 2024 年 1 月，请选择该月内的目标时间。');
    if (![0, 30].includes(minute)) throw new Error('目标时间须对齐 30 分钟帧，请选择整点或半点。');
    const batch = numberField('batch-size', '回放样本数', 1, capabilities.max_batch, true);
    const first = (day - 1) * 48 + hour * 2 + minute / 30;
    const frames = Array.from({length: batch}, (_, i) => first + i);
    if (frames.some(frame => frame >= 1488)) throw new Error('批次超出 1 月数据范围，请缩短批次或提前目标时间。');
    const missing = frames.find(frame => frame % 48 < capabilities.history_length);
    if (missing !== undefined) {
      const d = Math.floor(missing / 48) + 1, h = Math.floor((missing % 48) / 2), m = (missing % 2) * 30;
      throw new Error('批次包含 1 月 ' + d + ' 日 ' + String(h).padStart(2, '0') + ':' + String(m).padStart(2, '0') + '，缺少同日 3 帧（90 分钟）历史。请调整开始时间或缩短跨午夜批次；每天从 01:30 起可预测。');
    }
    const cpu = numberField('cpu-budget', 'CPU 配额', 0.01, capabilities.budgets.cpu_budget);
    const memory = numberField('memory-budget', '总内存配额 (MiB)', capabilities.base_resources.memory_mb + 1, capabilities.budgets.memory_budget, true);
    const concurrency = numberField('max-concurrency', '最大并发', 1, capabilities.budgets.max_concurrency, true);
    const task = $('task-select').value, policy = $('policy-select').value;
    if (!['all', ...fields].includes(task) || !capabilities.policies.includes(policy)) throw new Error('请选择服务支持的预测任务和调度策略。');
    return freeze({data_ref: $('data-ref').value, target_frames: frames, task, schema_version: capabilities.version, model_version: capabilities.version, horizon: capabilities.horizon, policy, resources: {cpu_budget: cpu, memory_budget: memory, max_concurrency: concurrency, aging_interval: 30, resource_timeout: 30}});
  }
  function validPredictions(data) {
    if (!data.workflow_id || !data.tasks || !data.resources || !Array.isArray(data.resource_events)) throw new Error('服务未返回完整工作流记录，请重试。');
    if (data.status !== 'success') return;
    if (data.version !== request.model_version || JSON.stringify(data.target_frames) !== JSON.stringify(request.target_frames) || data.target_time_range?.length !== request.target_frames.length) throw new Error('返回的模型版本或目标时间与请求不一致，请重新连接后重试。');
    const selected = request.task === 'all' ? fields : [request.task];
    for (const task of selected) {
      const item = data.predictions?.[task], channels = task === 'od' ? 36 : 2;
      const expected = [request.target_frames.length, 1, 6, 6, channels];
      if (!item || JSON.stringify(item.shape) !== JSON.stringify(expected) || item.layout !== 'B,H,Y,X,C' || item.units?.length !== channels) throw new Error('预测张量的轴、尺寸或单位与数据契约不一致。');
      function check(array, depth) {
        if (depth === expected.length) return Number.isFinite(array) && array >= 0;
        return Array.isArray(array) && array.length === expected[depth] && array.every(child => check(child, depth + 1));
      }
      if (!check(item.prediction, 0)) throw new Error('预测包含无效数值或缺失网格，请重新运行。');
    }
  }
  function renderExecution() {
    const tasks = Object.entries(result.tasks || {});
    const position = {prepare:[5,98], predict:[130,98], validate_state:[263,20], validate_flow:[263,98], validate_od:[263,176], aggregate:[402,98]};
    const w = 108, h = 42;
    let edges = '', nodes = '';
    for (const [id, task] of tasks) {
      const p = position[id]; if (!p) continue;
      for (const parent of task.depends_on || task.spec?.depends_on || []) {
        const from = position[parent]; if (!from) continue;
        edges += '<path d="M' + (from[0] + w) + ',' + (from[1] + h/2) + ' L' + p[0] + ',' + (p[1] + h/2) + '" stroke="#a9b8d1" stroke-width="1.7" marker-end="url(#dag-arrow)" fill="none"/>';
      }
      const good = task.call_status === 'success' && task.quality_status === 'valid' && task.schedule_status === 'completed';
      nodes += '<g><title>' + esc(id + ': ' + task.schedule_status + ' / ' + task.call_status + ' / ' + task.quality_status) + '</title><rect x="' + p[0] + '" y="' + p[1] + '" width="' + w + '" height="' + h + '" rx="7" fill="' + (good ? '#edf8f2' : '#f3f6fb') + '" stroke="' + (good ? '#a3d9bb' : '#ced8e8') + '"/><text x="' + (p[0] + w/2) + '" y="' + (p[1] + 27) + '" text-anchor="middle" fill="#28354a">' + esc(stageNames[id] || id) + '</text></g>';
    }
    const dag = '<svg class="dag" viewBox="0 0 515 235" role="img" aria-label="真实任务依赖图"><defs><marker id="dag-arrow" markerWidth="7" markerHeight="7" refX="6" refY="3.5" orient="auto"><path d="M0,0 L7,3.5 L0,7" fill="#a9b8d1"/></marker></defs>' + edges + nodes + '</svg>';
    const rows = tasks.map(([id, task]) => '<tr><td title="' + esc(id) + '">' + esc(stageNames[id] || id) + '</td><td>' + badge(task.call_status) + '</td><td>' + badge(task.quality_status) + '</td><td>' + badge(task.schedule_status) + '</td></tr>').join('');
    const traceRows = fields.map(task => {
      const trace = result.schema_trace?.[task] || [], target = trace.find(x => x.rule === 'validate' && x.side === 'target')?.schema;
      const outputTrace = result.schema_trace?.['validate_' + task] || [];
      const output = outputTrace.find(x => x.schema)?.schema;
      const prediction = result.predictions?.[task];
      const rules = trace.filter(entry => entry.rule !== 'validate').map(entry => entry.rule === 'transpose' ? 'transpose [' + entry.permutation.join(',') + ']' : entry.rule === 'standardize' ? 'standardize · ' + entry.stats.split + ' · (x−μ)/σ' : entry.rule);
      const transposition = trace.find(x => x.rule === 'transpose');
      const conversion = transposition ? axes(transposition.from_axes) + ' → ' + axes(transposition.to_axes) : '';
      const actualInputShape = target?.shape.map(x => x === null ? result.target_frames.length : x);
      const input = target ? esc(axes(target.axes)) + ' ' + esc(shape(actualInputShape)) : '—';
      const out = prediction ? prediction.layout + ' ' + shape(prediction.shape) : output ? axes(output.axes) + ' ' + shape(output.shape.map(x => x === null ? result.target_frames.length : x)) : '—';
      const units = output?.units || prediction?.units || [];
      return '<tr><td><strong>' + task + '</strong></td><td class="tensor">' + input + '<span class="subline">→ ' + esc(out) + '</span></td><td title="' + esc(conversion) + '">' + (rules.length ? rules.map(rule => '<span style="display:block">' + esc(rule) + '</span>').join('') : '暂无适配记录') + '</td><td>' + [...new Set(units)].map(x => esc(unit(x))).join('<br>') + '</td></tr>';
    }).join('');
    $('execution-content').className = '';
    $('execution-content').innerHTML = '<div class="card"><div class="card-head"><h2>' + tasks.length + ' 节点工作流 · 真实依赖关系</h2><span class="muted">call / quality / schedule</span></div><div class="split">' + dag + '<table class="statuses"><thead><tr><th>节点</th><th>call</th><th>quality</th><th>schedule</th></tr></thead><tbody>' + rows + '</tbody></table></div></div><div class="card"><div class="card-head"><h2>契约适配记录</h2><span class="muted">模型 ' + esc(result.version || '—') + '</span></div><table class="trace-table"><thead><tr><th>能力</th><th>输入 → 输出轴 / shape</th><th>schema_trace 实际规则</th><th>原始单位</th></tr></thead><tbody>' + traceRows + '</tbody></table></div>';
  }
  function metric(label, value, budget, peak, suffix = '') {
    return '<div class="metric"><p class="muted">' + label + '</p><div class="metric-value">' + fmt(value) + ' <small>/ ' + fmt(budget) + suffix + '</small></div><p class="muted">峰值 ' + fmt(peak) + suffix + '</p></div>';
  }
  function resourceChart(events, budgets) {
    if (!events.length) return '<p class="empty">服务未返回资源账本事件。</p>';
    const origin = Math.min(...events.map(e => e.now)), end = Math.max(...events.map(e => e.now)), elapsed = (end - origin) * 1000;
    const x = now => 50 + (now - origin) / (end - origin || 1) * 355;
    function step(key, ceiling, top, height, color) {
      const y = value => top + height - value / ceiling * height;
      let path = 'M50,' + y(0);
      for (const event of events) path += ' H' + x(event.now) + ' V' + y(event[key]);
      return '<path d="' + path + '" fill="none" stroke="' + color + '" stroke-width="2.5"/>';
    }
    let grid = '';
    for (const [top, height, ceiling] of [[28,75,budgets.cpu_budget],[149,64,budgets.task_memory_budget]]) {
      for (const part of [0, .5, 1]) {
        const y = top + height * (1 - part);
        grid += '<path d="M50,' + y + ' H405" stroke="#e7edf6"/><text x="43" y="' + (y+5) + '" text-anchor="end" fill="#64748b">' + fmt(ceiling * part) + '</text>';
      }
    }
    return '<svg class="quota-chart" viewBox="0 0 440 246" role="img" aria-label="按真实资源事件绘制的配额占用阶梯曲线"><text x="50" y="17" fill="#3558b4">CPU 配额</text><text x="170" y="17" fill="#179b8f">并发（右轴上限 ' + esc(budgets.max_concurrency) + '）</text>' + grid + step('cpu_used', budgets.cpu_budget, 28, 75, '#4a6cf7') + step('concurrency', budgets.max_concurrency, 28, 75, '#179b8f') + '<text x="50" y="137" fill="#8a63cf">任务内存 (MiB)</text>' + step('memory_used_mb', budgets.task_memory_budget, 149, 64, '#8a63cf') + '<text x="50" y="240" fill="#64748b">0 ms</text><text x="405" y="240" text-anchor="end" fill="#64748b">' + fmt(elapsed, 3) + ' ms</text></svg>';
  }
  function renderResources() {
    const resources = result.resources, peaks = result.peaks || resources.peaks || {}, budgets = result.budgets;
    const events = [...result.resource_events].sort((a,b) => a.now - b.now);
    const reserves = events.filter(e => e.event === 'reserve').length, releases = events.filter(e => e.event === 'release').length;
    $('resources-content').className = '';
    $('resources-content').innerHTML = '<div class="metrics">' + metric('最终任务 CPU 占用', resources.cpu_used, budgets.cpu_budget, peaks.cpu_used) + metric('最终任务内存占用', resources.memory_used_mb, budgets.task_memory_budget, peaks.memory_used_mb, ' MiB') + metric('最终任务并发', resources.concurrency, budgets.max_concurrency, peaks.concurrency) + '</div><p class="note">总内存 ' + fmt(budgets.memory_budget) + ' MiB = 模型基础 ' + fmt(result.base_resources.memory_mb) + ' MiB + 任务配额 ' + fmt(budgets.task_memory_budget) + ' MiB。账本记录调度占用，非操作系统利用率。</p><div class="card" style="margin-top:14px"><div class="card-head"><h2>配额账本与占用变化</h2><span class="muted">reserve ' + reserves + ' 份 / release ' + releases + ' 份</span></div><div class="split"><div>' + resourceChart(events, budgets) + '<p class="muted">按事件时间绘制；起点为首个资源事件。</p></div><div><table class="ledger"><thead><tr><th>事件 / 节点</th><th>CPU</th><th>MiB</th><th>并发</th></tr></thead><tbody id="ledger-rows"></tbody></table><div class="pager"><button id="ledger-prev" class="ghost">上一页</button><span id="ledger-page" class="muted"></span><button id="ledger-next" class="ghost">下一页</button></div></div></div></div>';
    $('ledger-prev').addEventListener('click', () => { ledgerPage--; renderLedger(); });
    $('ledger-next').addEventListener('click', () => { ledgerPage++; renderLedger(); });
    renderLedger();
  }
  function renderLedger() {
    const events = [...result.resource_events].sort((a,b) => a.now - b.now), size = 6;
    const pages = Math.max(1, Math.ceil(events.length / size));
    ledgerPage = Math.max(0, Math.min(ledgerPage, pages - 1));
    const origin = events[0]?.now;
    $('ledger-rows').innerHTML = events.slice(ledgerPage * size, (ledgerPage + 1) * size).map(event => '<tr><td title="' + esc(event.task_id + ' · ' + event.reservation_id + ' · +' + fmt((event.now-origin)*1000, 3) + 'ms') + '"><span class="badge ' + (event.event === 'release' ? 'good' : '') + '">' + esc(event.event) + '</span> ' + esc(stageNames[event.task_id] || event.task_id) + '</td><td>' + fmt(event.cpu_used) + '</td><td>' + fmt(event.memory_used_mb) + '</td><td>' + fmt(event.concurrency) + '</td></tr>').join('') || '<tr><td colspan="4">暂无资源事件。</td></tr>';
    $('ledger-page').textContent = (ledgerPage + 1) + ' / ' + pages + ' · ' + events.length + ' 条';
    $('ledger-prev').disabled = ledgerPage === 0; $('ledger-next').disabled = ledgerPage === pages - 1;
  }
  function setupResults() {
    const available = fields.filter(task => result.predictions?.[task]);
    $('results-empty').hidden = available.length > 0;
    $('results-content').hidden = available.length === 0;
    if (!available.length) return;
    $('result-task').innerHTML = available.map(task => '<option value="' + task + '">' + task + ' · ' + names[task] + '</option>').join('');
    $('result-task').value = available.includes('flow') ? 'flow' : available[0];
    $('result-time').innerHTML = result.target_time_range.map((range,i) => '<option value="' + i + '">' + esc(dateTime(range.start)) + '</option>').join('');
    setupChannels();
  }
  function setupChannels() {
    const task = $('result-task').value, item = result.predictions[task];
    $('result-channel').innerHTML = (task === 'od' ? '<option value="sum">全部终点求和</option>' : '') + item.channels.map((channel,i) => '<option value="' + i + '">' + esc(channelName(task, i)) + (task !== 'od' ? ' · ' + esc(unit(item.units[i])) : '') + '</option>').join('');
    renderResults();
  }
  const sum = values => values.reduce((a,b) => a+b, 0);
  function renderResults() {
    const task = $('result-task').value, b = Number($('result-time').value), channel = $('result-channel').value;
    const item = result.predictions[task], grid = item.prediction[b][0];
    const values = grid.flatMap(row => row.map(cell => channel === 'sum' ? sum(cell) : cell[Number(channel)]));
    const min = Math.min(...values), max = Math.max(...values);
    const chosenUnit = item.units[channel === 'sum' ? 0 : Number(channel)];
    let cells = '';
    for (let y=0; y<6; y++) for (let x=0; x<6; x++) {
      const value = values[y*6+x], ratio = max === min ? 0 : (value-min)/(max-min);
      const start = [238,244,255], end = [36,85,196];
      const color = start.map((n,i) => Math.round(n + (end[i]-n)*ratio));
      const number = value >= 1000 ? fmt(value,0) : fmt(value,1);
      cells += '<g><title>网格 (' + y + ',' + x + ') · ' + esc(String(value) + ' ' + unit(chosenUnit)) + '</title><rect x="' + (39+x*40) + '" y="' + (31+y*40) + '" width="37" height="37" rx="4" fill="rgb(' + color.join(',') + ')"/><text x="' + (57.5+x*40) + '" y="' + (55+y*40) + '" text-anchor="middle" fill="' + (ratio>.55 ? '#fff' : '#28354a') + '">' + esc(number) + '</text></g>';
    }
    let labels = '';
    for (let i=0; i<6; i++) labels += '<text x="' + (57.5+i*40) + '" y="21" text-anchor="middle" fill="#64748b">' + i + '</text><text x="23" y="' + (55+i*40) + '" text-anchor="middle" fill="#64748b">' + i + '</text>';
    $('heatmap').innerHTML = '<svg class="heatmap" viewBox="0 0 300 280" role="img" aria-label="' + esc(task + ' 原始单位预测网格') + '">' + labels + cells + '</svg>';
    $('heatmap-title').textContent = task === 'od' ? 'OD 起点网格预测' : channelName(task, Number(channel)) + '预测';
    $('heatmap-min').textContent = fmt(min); $('heatmap-max').textContent = fmt(max);
    $('heatmap-unit').textContent = 'raw · ' + unit(chosenUnit) + ' · ' + dateTime(result.target_time_range[b].start) + (task === 'od' ? ' · 终点按行优先编号 0–35' : '');
    renderForecast(task);
    if (task === 'od') {
      const pairs = [];
      grid.forEach((row,y) => row.forEach((cell,x) => cell.forEach((value,d) => pairs.push({origin:y*6+x,destination:d,value}))));
      pairs.sort((a,b) => b.value-a.value);
      $('result-detail').innerHTML = '<p style="margin-bottom:5px">当前目标 OD · Top 5 预测关系</p><table><thead><tr><th>起点 → 终点</th><th>预测行程/30min</th></tr></thead><tbody>' + pairs.slice(0,5).map(pair => '<tr><td>' + pair.origin + ' → ' + pair.destination + '</td><td>' + fmt(pair.value,3) + '</td></tr>').join('') + '</tbody></table>';
    } else {
      const aggregate = task === 'state' && Number(channel) === 1 ? sum(values)/values.length : sum(values);
      $('result-detail').innerHTML = '<p>当前目标 · ' + (task === 'state' && Number(channel) === 1 ? '36 网格速度均值 ' : '36 网格预测总量 ') + '<strong>' + fmt(aggregate) + '</strong> ' + esc(unit(chosenUnit)) + '</p>';
    }
    $('result-summary').textContent = '批量一步预测 · ' + result.target_frames.length + ' 个回放样本 · H=' + item.shape[1] + ' · 每样本分别使用此前 90 分钟历史。输出 ' + item.layout + ' ' + shape(item.shape) + '，仅展示原始单位预测。';
  }
  function renderForecast(selectedTask) {
    const task = result.predictions.flow ? 'flow' : selectedTask;
    const item = result.predictions[task], batches = item.prediction;
    let series, labels, units;
    if (task === 'flow' || task === 'state') {
      series = [0,1].map(c => batches.map(batch => {
        const cells = batch[0].flatMap(row => row.map(cell => cell[c]));
        return sum(cells) / (task === 'state' && c === 1 ? cells.length : 1);
      }));
      labels = [channelName(task,0), channelName(task,1)]; units = [unit(item.units[0]), unit(item.units[1])];
    } else {
      series = [batches.map(batch => sum(batch[0].flatMap(row => row.flat())))]; labels = ['OD 总行程']; units = [unit(item.units[0])];
    }
    const colors = ['#4a6cf7','#179b8f'];
    const independent = task === 'state';
    const ceilings = series.map(values => Math.max(...values) || 1);
    if (!independent) ceilings.fill(Math.max(...ceilings));
    const width = 470, left = 45, right = independent ? 420 : 450, top = 22, bottom = 150;
    const x = i => left + i/Math.max(batches.length-1,1)*(right-left);
    const y = (value,index) => bottom - value/ceilings[index]*(bottom-top);
    let drawing = '';
    for (const part of [0,.5,1]) {
      const yy = bottom-part*(bottom-top);
      drawing += '<path d="M' + left + ',' + yy + ' H' + right + '" stroke="#e7edf6"/><text x="' + (left-6) + '" y="' + (yy+5) + '" text-anchor="end" fill="#64748b">' + fmt(ceilings[0]*part,1) + '</text>';
      if (independent) drawing += '<text x="' + (right+5) + '" y="' + (yy+5) + '" fill="#179b8f">' + fmt(ceilings[1]*part,1) + '</text>';
    }
    series.forEach((values,index) => {
      drawing += '<polyline points="' + values.map((v,i) => x(i)+','+y(v,index)).join(' ') + '" fill="none" stroke="' + colors[index] + '" stroke-width="2.5"/>';
      values.forEach((value,i) => { drawing += '<circle cx="' + x(i) + '" cy="' + y(value,index) + '" r="3.5" fill="' + colors[index] + '"><title>' + esc(dateTime(result.target_time_range[i].start) + ' · ' + labels[index] + ' ' + fmt(value,4) + ' ' + units[index]) + '</title></circle>'; });
    });
    const stride = Math.max(1, Math.ceil(batches.length/8));
    result.target_time_range.forEach((range,i) => {
      if (i%stride===0 || i===batches.length-1) drawing += '<text x="' + x(i) + '" y="181" text-anchor="middle" fill="#64748b">' + esc(clock(range.start)) + '</text>';
    });
    $('forecast-title').textContent = (task === 'flow' ? '流入 / 流出预测' : task === 'state' ? '状态预测' : 'OD 总量预测') + ' · ' + batches.length + ' 个回放目标';
    $('forecast-legend').innerHTML = labels.map((label,i) => '<span><i class="dot" style="background:' + colors[i] + '"></i>' + esc(label + ' · ' + units[i]) + '</span>').join('');
    $('forecast-chart').innerHTML = '<svg class="forecast-chart" viewBox="0 0 ' + width + ' 195" role="img" aria-label="回放目标区间的一步预测曲线">' + drawing + '</svg>';
    $('forecast-note').textContent = task === 'state' ? '在途行程按网格求和；速度为 36 网格均值（右轴 mph）。' : '全网格原始预测求和 · 每点为独立一步预测。';
  }
  $('result-task').addEventListener('change', setupChannels);
  $('result-time').addEventListener('change', renderResults);
  $('result-channel').addEventListener('change', renderResults);
  function resetOutput() {
    $('execution-content').className = 'empty'; $('execution-content').textContent = '等待本次工作流返回节点与契约记录。';
    $('resources-content').className = 'empty'; $('resources-content').textContent = '等待本次工作流返回资源账本。';
    $('results-empty').hidden = false; $('results-empty').textContent = '等待本次工作流返回预测。'; $('results-content').hidden = true;
  }
  $('traffic-form').addEventListener('submit', async event => {
    event.preventDefault(); if (busy) return; errorMessage('');
    let next;
    try { next = makeRequest(); }
    catch (error) { errorMessage(error.message); status('invalid', '配置需调整'); switchView('config'); return; }
    request = next; result = null; busy = true; ledgerPage = 0;
    $('run-btn').disabled = true; $('run-btn').textContent = '等待服务执行';
    status('running', '服务执行中'); $('run-summary').textContent = '等待真实工作流响应'; resetOutput();
    try {
      result = freeze(await api('/workflows/run', {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(request)}));
      validPredictions(result);
      renderExecution(); renderResources();
      if (result.status !== 'success') {
        $('results-empty').textContent = '本次任务未完成预测，请检查执行状态与资源配额。';
        const failed = Object.values(result.tasks).find(task => task.quality_status === 'invalid' || task.call_status === 'failed');
        throw new Error(failed?.quality_status === 'invalid' ? '预测未通过数据契约校验，请检查执行与契约记录后重试。' : '本次工作流未成功完成，请检查资源配额和节点状态后重试。');
      }
      setupResults(); status('success', '执行成功');
      const times = result.target_time_range;
      $('run-summary').textContent = dateTime(times[0].start) + '–' + clock(times[times.length-1].end_exclusive) + ' · ' + times.length + ' 个目标 · ' + request.policy;
    } catch (error) {
      status(result?.status && result.status !== 'success' ? result.status : 'error', '执行未完成');
      $('run-summary').textContent = '请检查反馈后重新运行'; errorMessage(error.message);
      if (!result) {
        $('execution-content').textContent = '服务未返回本次工作流记录。'; $('resources-content').textContent = '服务未返回本次资源账本。'; $('results-empty').textContent = '本次请求未获得预测结果。';
      }
    } finally { busy = false; $('run-btn').disabled = !capabilities; $('run-btn').textContent = '运行回放任务'; }
  });
  loadCapabilities();
})();
</script>
</body>
</html>"""
