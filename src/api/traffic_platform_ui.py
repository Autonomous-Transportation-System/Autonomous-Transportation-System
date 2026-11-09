"""Self-contained management UI; mounting and execution belong to the parent API.

No backend, model, or web framework imports are needed to obtain this HTML.
The browser reads the management API and never simulates workflow execution.
"""


def get_traffic_platform_html() -> str:
    """Return the standalone, Chinese traffic orchestration platform page."""
    return _HTML


_HTML = r'''<!doctype html>
<html lang="zh-CN">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<meta name="color-scheme" content="light">
<title>交通计算平台 · 应用与联合编排</title>
<link rel="icon" href="data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'%3E%3Crect width='32' height='32' rx='8' fill='%23315ee8'/%3E%3Cpath d='M8 9h16v4h-6v12h-4V13H8z' fill='white'/%3E%3C/svg%3E">
<style>
:root{--bg:#f3f5f9;--card:#fff;--ink:#19273b;--muted:#6c7a8e;--line:#e5eaf1;--blue:#315ee8;--light:#eef2ff;--green:#168065;--amber:#ad7211;--red:#c04449;--radius:16px}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--ink);font:14px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI","PingFang SC","Microsoft YaHei",sans-serif}button,input,select{font:inherit}button,a,input,select{outline-offset:3px}button{cursor:pointer;border:1px solid var(--line);border-radius:9px;background:white;color:var(--ink);padding:8px 14px;transition:background .15s}button:hover{background:var(--light)}button:disabled{cursor:default;opacity:.48}a{color:var(--blue)}h1,h2,h3,p{margin:0}h1{font-size:23px;letter-spacing:-.5px}h2{font-size:16px}h3{font-size:14px}code,.mono{font:12px/1.5 ui-monospace,SFMono-Regular,Menlo,monospace;overflow-wrap:anywhere}[hidden]{display:none!important}.shell{max-width:1500px;margin:0 auto;padding:26px 32px 28px}.topbar,.row,.card-head,.footer,.actions,.tabs,.chips{display:flex;align-items:center;gap:12px}.topbar{justify-content:space-between;margin-bottom:22px}.brand{display:flex;gap:14px;align-items:center}.logo{width:44px;height:44px;background:var(--blue);border-radius:13px;display:grid;place-items:center;color:white;font-weight:700;font-size:20px;box-shadow:0 6px 18px #315ee826}.eyebrow{font-size:10px;letter-spacing:2px;color:var(--blue);font-weight:750}.sub,.muted{color:var(--muted)}.sub{font-size:12px;margin-top:3px}.top-actions{display:flex;align-items:center;gap:10px}.badge{display:inline-flex;align-items:center;gap:5px;font-size:11px;border-radius:6px;padding:3px 8px;background:#eef1f6;color:#657286;white-space:nowrap}.badge.good{background:#e8f6f0;color:var(--green)}.badge.bad{background:#fdeeee;color:var(--red)}.badge.busy{background:#fff3dd;color:var(--amber)}.badge.blue{background:var(--light);color:var(--blue)}.primary{background:var(--blue);border-color:var(--blue);color:white;font-weight:650}.primary:hover{background:#244dc8}.danger{color:var(--red)}.small{padding:5px 10px;font-size:12px}.notice{border:1px solid #f0c9cb;background:#fff2f2;color:#a33940;padding:11px 14px;border-radius:10px;margin-bottom:14px;white-space:pre-wrap}.notice.success{background:#ebf8f2;border-color:#c9e7d9;color:var(--green)}.tabs{border-bottom:1px solid var(--line);margin-bottom:22px;gap:8px;overflow:auto}.tab{border:0;border-radius:0;background:transparent;padding:12px 17px;white-space:nowrap;color:var(--muted);border-bottom:3px solid transparent;font-weight:600}.tab[aria-selected=true]{color:var(--blue);border-bottom-color:var(--blue)}.tab-no{font-size:10px;opacity:.6;margin-right:8px}.view{min-height:520px}.page-head{display:flex;align-items:center;justify-content:space-between;gap:18px;margin-bottom:16px}.page-head .muted{font-size:12px;margin-top:4px}.card{background:var(--card);border:1px solid var(--line);border-radius:var(--radius);padding:20px;min-width:0}.card-head{justify-content:space-between;margin-bottom:15px;flex-wrap:wrap}.split{display:grid;grid-template-columns:350px minmax(0,1fr);gap:18px}.two{display:grid;grid-template-columns:1fr 1fr;gap:18px}.stack{display:grid;gap:18px}.metrics{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:12px;margin-bottom:18px}.metric{padding:15px 17px;background:white;border:1px solid var(--line);border-radius:12px}.metric .label{font-size:11px;color:var(--muted)}.metric .value{font-size:25px;font-weight:700;line-height:1.35;margin:5px 0;overflow-wrap:anywhere}.metric .note{font-size:10px;color:var(--muted)}label{display:block;color:#48566b;font-size:12px;font-weight:550}input,select{width:100%;margin-top:5px;border:1px solid #dce2ed;border-radius:8px;padding:8px 10px;color:var(--ink);background:#fff;min-height:38px}input:focus,select:focus{border-color:var(--blue)}.form-grid{display:grid;grid-template-columns:1fr 1fr;gap:12px}.form-row{margin-bottom:13px}.check{display:flex;align-items:center;gap:8px;margin-top:12px;font-weight:400}.check input{width:15px;min-height:15px;margin:0;accent-color:var(--blue)}.hint{font-size:11px;color:var(--muted);margin-top:8px;line-height:1.7}.frame-note{border:1px solid #dfe6ff;border-radius:9px;background:#f5f7ff;padding:10px 12px;margin:14px 0;color:#526391;font-size:11px}.form-submit{width:100%;margin-top:15px}.empty{padding:34px 20px;text-align:center;color:var(--muted);font-size:12px;border:1px dashed #dce3ee;border-radius:11px;background:#fafbfe}.table-wrap{overflow:auto}table{border-collapse:collapse;width:100%;text-align:left;font-size:12px}th{color:var(--muted);font-size:11px;font-weight:550;background:#fafbfd;white-space:nowrap}th,td{padding:11px 10px;border-bottom:1px solid var(--line);vertical-align:top}td{overflow-wrap:anywhere}tbody tr:last-child td{border-bottom:0}tbody tr.selected{background:#f3f6ff}td .actions{gap:5px;flex-wrap:wrap}.id{display:block;color:var(--muted);font-size:10px;margin-top:3px;max-width:240px;overflow-wrap:anywhere}.kv{display:grid;grid-template-columns:minmax(105px,.65fr) minmax(0,1fr);font-size:12px;gap:7px 14px}.kv dt{color:var(--muted)}.kv dd{margin:0;overflow-wrap:anywhere}.divider{height:1px;background:var(--line);margin:16px 0}.chips{flex-wrap:wrap;gap:6px}.chip{border:1px solid var(--line);border-radius:6px;background:#f9faff;padding:3px 8px;font-size:11px}.catalog-grid{display:grid;grid-template-columns:repeat(3,minmax(0,1fr));gap:12px}.catalog-card{border:1px solid var(--line);border-radius:11px;padding:14px}.catalog-card h3{margin-bottom:9px;overflow-wrap:anywhere}.section-gap{margin-top:18px}.select-inline{display:flex;gap:10px;align-items:center;flex-wrap:wrap}.select-inline label{min-width:180px;max-width:390px}.dag-wrap{overflow:auto;min-height:160px}.dag{display:flex;align-items:flex-start;gap:25px;padding:10px 2px}.dag-level{display:grid;gap:12px;min-width:180px;max-width:230px}.dag-node{border:1px solid #dce4f6;border-left:3px solid var(--blue);border-radius:10px;padding:12px;background:#fcfdff}.dag-node .cap{font-weight:650;font-size:12px;overflow-wrap:anywhere}.dag-node .deps{font-size:10px;margin-top:8px;color:var(--muted);overflow-wrap:anywhere}.dag-node .instance{font:10px/1.6 ui-monospace,monospace;overflow-wrap:anywhere;margin-top:7px}.dag-node .reason{font-size:11px;margin-top:7px;white-space:pre-wrap}.level-title{font-size:10px;color:var(--muted);margin-bottom:3px}.timeline{display:grid;gap:9px;max-height:320px;overflow:auto}.event{border-left:2px solid #dce5fa;padding:5px 0 5px 12px}.event strong{font-size:12px;font-weight:600}.event .muted{font-size:11px;white-space:pre-wrap;overflow-wrap:anywhere}details{font-size:11px;color:var(--muted);margin-top:12px}summary{cursor:pointer}pre{background:#f6f8fc;border:1px solid var(--line);padding:11px;border-radius:8px;max-height:340px;overflow:auto;white-space:pre-wrap;overflow-wrap:anywhere;font:11px/1.6 ui-monospace,monospace}.quota-row{margin:15px 0}.quota-label{display:flex;justify-content:space-between;gap:12px;font-size:12px}.quota-track{height:7px;background:#edf0f7;border-radius:5px;overflow:hidden;margin-top:8px}.quota-fill{height:100%;background:var(--blue);border-radius:5px}.grid{display:grid;gap:5px;max-width:500px;aspect-ratio:1;margin:0 auto}.cell{border-radius:7px;display:grid;place-items:center;font:11px ui-monospace,monospace;min-height:35px;border:1px solid #315ee815}.legend{display:flex;align-items:center;gap:10px;font-size:11px;color:var(--muted);justify-content:center;margin-top:14px}.gradient{height:8px;width:150px;background:linear-gradient(90deg,#eef2ff,#315ee8);border-radius:4px}.footer{margin-top:18px;padding:14px 18px;background:white;border:1px solid var(--line);border-radius:12px;justify-content:space-between}.footer-left{display:flex;align-items:center;gap:10px;flex-wrap:wrap}.footer .muted{font-size:11px}.result-controls{display:grid;grid-template-columns:1.4fr 1fr 1fr;gap:12px;margin-bottom:18px}.workflow-banner{border:1px solid var(--line);background:white;border-radius:10px;padding:12px 15px;margin-bottom:18px;display:flex;gap:12px;align-items:center;flex-wrap:wrap}.raw-note{font-size:11px;color:var(--muted);margin-bottom:14px}.status-line{display:flex;gap:8px;align-items:center;flex-wrap:wrap}.loading-dot{display:inline-block;width:6px;height:6px;background:currentColor;border-radius:50%}.stop-note{font-size:11px;color:var(--muted);margin-top:12px}
.port-summary{border-top:1px solid var(--line);padding-top:7px;margin-top:7px;font-size:10px;line-height:1.55;overflow-wrap:anywhere}.port-summary strong{font-size:10px;color:#50617a}.port-summary .mono{font-size:10px}.cap-role{font-size:11px;color:var(--muted);margin-bottom:10px}.catalog-card .kv{font-size:11px;grid-template-columns:68px minmax(0,1fr);gap:5px 9px}.candidate-list{margin-top:8px;display:grid;gap:5px;font-size:10px}.candidate-item{padding:5px 7px;border:1px solid var(--line);border-radius:6px;background:white;overflow-wrap:anywhere}.candidate-item.selected{border-color:#b7c8fa;background:#f0f4ff}
@media(min-width:1500px){.shell{padding-top:30px}}@media(max-width:1100px){.shell{padding:20px}.split{grid-template-columns:310px minmax(0,1fr)}.catalog-grid{grid-template-columns:repeat(2,minmax(0,1fr))}.metrics{grid-template-columns:repeat(2,minmax(0,1fr))}.two{grid-template-columns:1fr}}@media(max-width:760px){.shell{padding:14px}.topbar{align-items:flex-start;gap:12px}.top-actions{flex-wrap:wrap;justify-content:flex-end}.brand{gap:9px}.logo{width:36px;height:36px}h1{font-size:18px}.sub{font-size:10px}.split,.catalog-grid{grid-template-columns:1fr}.tab{padding:10px 9px;font-size:12px}.tab-no{display:none}.card{padding:15px}.page-head{align-items:flex-start;flex-direction:column}.result-controls{grid-template-columns:1fr}.footer{align-items:flex-start;flex-direction:column}.metrics{gap:9px}.metric{padding:12px}.metric .value{font-size:21px}.view{min-height:0}}
</style>
</head>
<body>
<main class="shell">
  <header class="topbar">
    <div class="brand"><div class="logo" aria-hidden="true">交</div><div><div class="eyebrow">TRAFFIC COMPUTE PLATFORM</div><h1>交通计算平台</h1><p class="sub">应用管理 · 能力目录 · 智能体与资源联合编排</p></div></div>
    <div class="top-actions"><span id="service-ready" class="badge" data-ready="false" role="status">连接管理服务…</span><button id="refresh-btn" class="small">刷新</button></div>
  </header>
  <div id="error-banner" class="notice" role="alert" hidden></div>
  <div id="action-message" class="notice success" role="status" hidden></div>
  <nav class="tabs" role="tablist" aria-label="平台页面">
    <button id="tab-apps" class="tab" role="tab" aria-selected="true" aria-controls="view-apps" data-tab="apps"><span class="tab-no">01</span>应用管理</button>
    <button id="tab-catalog" class="tab" role="tab" aria-selected="false" aria-controls="view-catalog" data-tab="catalog" tabindex="-1"><span class="tab-no">02</span>能力与实例</button>
    <button id="tab-orchestration" class="tab" role="tab" aria-selected="false" aria-controls="view-orchestration" data-tab="orchestration" tabindex="-1"><span class="tab-no">03</span>联合编排</button>
    <button id="tab-resources" class="tab" role="tab" aria-selected="false" aria-controls="view-resources" data-tab="resources" tabindex="-1"><span class="tab-no">04</span>资源与 QoS</button>
    <button id="tab-results" class="tab" role="tab" aria-selected="false" aria-controls="view-results" data-tab="results" tabindex="-1"><span class="tab-no">05</span>真实结果</button>
  </nav>

  <section id="view-apps" class="view" role="tabpanel" aria-labelledby="tab-apps" data-panel="apps">
    <div class="page-head"><div><h2>交通应用</h2><p class="muted">配置应用后运行；多个应用的任务共享同一调度队列和资源池。</p></div><span class="badge blue">NYCTLC2024jan30min6x6</span></div>
    <div id="overview-metrics" class="metrics"></div>
    <div class="split">
      <div class="card"><div class="card-head"><h2>创建应用</h2><span class="badge">一步预测 · H = 1</span></div>
        <form id="create-form">
          <div class="form-row"><label>应用名称<input id="app-name" name="name" value="早高峰流量预测" maxlength="120" required></label></div>
          <div class="form-row"><label>业务模板<select id="template-select" name="template"><option value="flow">流量预测 · flow</option><option value="analysis">状态与流量分析 · analysis</option><option value="od_hotspots">OD 热点分析 · od_hotspots</option></select></label><p id="template-note" class="hint">等待服务端模板目录。</p></div>
          <div class="form-row"><label>首个目标时间（数据集原始时间）<input id="target-time" type="datetime-local" value="2024-01-27T08:00" min="2024-01-01T01:30" max="2024-01-31T23:30" step="1800" required></label></div>
          <div class="form-grid"><label>独立样本数<input id="batch-size" type="number" value="8" min="1" max="32" step="1" required></label><label>截止时限（秒）<input id="deadline-seconds" type="number" value="60" min="0.1" step="0.1" required></label></div>
          <div class="form-row" style="margin-top:12px"><label>业务优先级<input id="priority" type="number" value="0" min="0" step="1" required></label></div>
          <label class="check"><input id="enable-reuse" type="checkbox" checked>启用同版本、同窗口推理复用</label>
          <label class="check"><input id="enable-recovery" type="checkbox" checked>启用有界失败恢复</label>
          <div id="frame-preview" class="frame-note"></div>
          <button id="create-btn" class="primary form-submit" type="submit" disabled>创建应用</button>
          <p class="hint">90 分钟历史 → 30 分钟预测。每个目标分别读取同日此前 3 帧；时间按数据集钟面映射，不作浏览器时区转换。</p>
        </form>
      </div>
      <div class="stack"><div class="card"><div class="card-head"><h2>应用列表 <span id="app-count" class="badge">—</span></h2><span id="apps-updated" class="muted" style="font-size:11px">等待服务端应用</span></div><div id="app-list" class="table-wrap"></div></div>
        <div class="card"><div class="card-head"><h2>当前应用</h2><label style="min-width:160px"><select id="app-select" aria-label="选择应用"><option value="">暂无应用</option></select></label></div><div id="app-detail" class="empty">创建或选择一个应用，查看真实配置与最近工作流。</div><p class="stop-note">停止操作会请求服务端取消；运行任务的配额以真实执行终态和释放账本为准。</p></div></div>
    </div>
  </section>

  <section id="view-catalog" class="view" role="tabpanel" aria-labelledby="tab-catalog" data-panel="catalog" hidden>
    <div class="page-head"><div><h2>能力、实现与运行实例</h2><p class="muted">目录决定可行候选；实例健康、并发与性能记录来自服务端。</p></div><span id="catalog-count" class="badge">目录待加载</span></div>
    <div class="two"><div class="card"><div class="card-head"><h2>实际实例</h2></div><div id="instances-content"></div></div><div class="card"><div class="card-head"><h2>Implementation</h2></div><div id="implementations-content"></div></div></div>
    <div class="card section-gap"><div class="card-head"><h2>能力目录</h2><span class="muted" style="font-size:11px">真实端口与张量摘要 · 完整契约可展开</span></div><div id="capabilities-content" class="catalog-grid"></div></div>
    <div id="catalog-raw"></div>
  </section>

  <section id="view-orchestration" class="view" role="tabpanel" aria-labelledby="tab-orchestration" data-panel="orchestration" hidden>
    <div class="page-head"><div><h2>跨工作流联合编排</h2><p class="muted">显示实际编译 DAG、执行实例与选择理由；三状态分别保留调度、调用、质量结果。</p></div><div class="select-inline"><label>查看工作流<select id="workflow-select"><option value="">暂无工作流</option></select></label></div></div>
    <div id="workflow-banner" class="workflow-banner"><span class="muted">运行应用后选择工作流。</span></div>
    <div class="card"><div class="card-head"><h2>实际编译 DAG</h2><span id="dag-note" class="muted" style="font-size:11px">等待服务端计划</span></div><div id="dag-content" class="dag-wrap"></div><div id="plan-raw"></div></div>
    <div class="card section-gap"><div class="card-head"><h2>全局任务队列</h2><span id="queue-count" class="badge">—</span></div><div id="queue-content" class="table-wrap"></div></div>
    <div class="two section-gap"><div class="card"><div class="card-head"><h2>实例选择与调度事件</h2></div><div id="selection-events" class="timeline"></div></div><div class="card"><div class="card-head"><h2>失败与恢复事件</h2></div><div id="recovery-events" class="timeline"></div></div></div>
  </section>

  <section id="view-resources" class="view" role="tabpanel" aria-labelledby="tab-resources" data-panel="resources" hidden>
    <div class="page-head"><div><h2>资源、QoS 与推理复用</h2><p class="muted">配额占用为调度账本；实测进程、执行时长等字段按服务端原始名称与单位单独展示。</p></div><span id="policy-badge" class="badge">策略未返回</span></div>
    <div class="two"><div class="card"><div class="card-head"><h2>共享资源配额</h2><span class="badge blue">调度账本</span></div><div id="quota-content"></div><p class="hint">CPU 配额、内存预留和并发占用反映任务资源约束，不能解释为操作系统利用率。</p></div><div class="card"><div class="card-head"><h2>服务端实测</h2></div><div id="measured-content"></div></div></div>
    <div id="inference-metrics" class="metrics section-gap"></div>
    <div class="two"><div class="card"><div class="card-head"><h2>QoS 记录</h2></div><div id="qos-content"></div></div><div class="card"><div class="card-head"><h2>缓存与推理 profile</h2></div><div id="cache-content"></div></div></div>
    <div class="card section-gap"><div class="card-head"><h2>资源预留 / 释放账本</h2><span id="ledger-count" class="badge">—</span></div><div id="ledger-content" class="table-wrap"></div><div id="resource-raw"></div></div>
  </section>

  <section id="view-results" class="view" role="tabpanel" aria-labelledby="tab-results" data-panel="results" hidden>
    <div class="page-head"><div><h2>工作流真实结果</h2><p id="result-context" class="muted">选择已运行应用的工作流，查看服务端预测或分析摘要。</p></div><button id="show-orchestration" class="small">查看编排</button></div>
    <div class="card"><div class="result-controls"><label>目标样本<select id="result-time"><option value="">暂无结果</option></select></label><label>输出<select id="result-task"><option value="">暂无输出</option></select></label><label>物理通道 / OD 终点<select id="result-channel"><option value="">暂无通道</option></select></label></div>
      <div class="two"><div><p id="grid-note" class="raw-note"></p><div id="result-grid" class="empty">服务端尚未返回预测网格。</div><div id="grid-legend" class="legend" hidden></div></div><div><h3 style="margin-bottom:13px">结果摘要</h3><div id="result-summary"></div><div id="result-raw"></div></div></div>
    </div>
  </section>
  <footer class="footer"><div class="footer-left"><span id="current-app-status" class="badge">未选择应用</span><span id="refresh-status" class="muted" role="status">连接中 · 可见页面自动刷新</span></div><div class="actions"><button id="stop-btn" class="danger" disabled>停止应用</button><button id="run-btn" class="primary" disabled>运行当前应用</button></div></footer>
</main>
<script>
(() => {
  'use strict';
  const $ = id => document.getElementById(id);
  const API_BASE = location.protocol === 'file:' ? 'http://127.0.0.1:8000/traffic/platform' : '/traffic/platform';
  const state = {overview:null,catalog:null,apps:null,templates:null,resources:null,qos:null,lastWorkflow:null,
    selectedApp:'',workflowId:'',connected:false,mutating:false,action:'',refreshing:null,pending:new Map(),errors:{},lastRefresh:null};
  const empty = text => '<div class="empty">' + esc(text) + '</div>';
  const isObject = value => value !== null && typeof value === 'object' && !Array.isArray(value);
  const object = value => isObject(value) ? value : {};
  const esc = value => String(value ?? '').replace(/[&<>"']/g, char => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[char]));
  const at = (value,path) => path.split('.').reduce((item,key) => item == null ? undefined : item[key],value);
  const pick = (sources,paths) => {
    for (const source of sources) for (const path of paths) {
      const value = at(source,path); if (value !== undefined && value !== null) return value;
    }
    return undefined;
  };
  function body(payload,key) {
    if (payload == null) return null;
    if (isObject(payload) && payload[key] !== undefined) return payload[key];
    if (isObject(payload.data)) return payload.data[key] ?? payload.data;
    if (Array.isArray(payload.data)) return payload.data;
    return payload;
  }
  function records(value) {
    if (Array.isArray(value)) return value.map(item => isObject(item) ? item : {_value:item});
    if (!isObject(value)) return [];
    if (Array.isArray(value.items)) return records(value.items);
    return Object.entries(value).map(([key,item]) => isObject(item) ? {...item,_key:key} : {_key:key,_value:item});
  }
  function freeze(value) {
    if (value && typeof value === 'object' && !Object.isFrozen(value)) {
      Object.values(value).forEach(freeze); Object.freeze(value);
    }
    return value;
  }
  Object.defineProperty(window,'trafficPlatform',{value:Object.freeze({
    get overview(){return state.overview;},get catalog(){return state.catalog;},
    get apps(){return state.apps;},get lastWorkflow(){return state.lastWorkflow;}
  }),writable:false,configurable:false,enumerable:true});
  function display(value) {
    if (value === undefined || value === null || value === '') return '未返回';
    if (typeof value === 'boolean') return value ? '是' : '否';
    if (typeof value === 'number') return Number.isFinite(value) ? value.toLocaleString('zh-CN',{maximumFractionDigits:4}) : '无效数值';
    if (typeof value === 'object') return JSON.stringify(value);
    return String(value);
  }
  const translations = {pending:'待调度',ready:'就绪',queued:'排队中',reserved:'已预留',running:'运行中',completed:'已完成',success:'成功',succeeded:'成功',failed:'失败',error:'错误',cancelled:'已取消',canceled:'已取消',stopping:'停止中',stopped:'已停止',created:'已创建',idle:'空闲',skipped:'已跳过',timeout:'超时',resource_timeout:'资源超时',start_failed:'启动失败',not_called:'未调用',not_evaluated:'未评价',valid:'有效',invalid:'无效',healthy:'健康',unhealthy:'不健康'};
  function badge(value) {
    const key = String(value ?? '');
    const tone = /^(success|succeeded|completed|valid|healthy|ready)$/.test(key) ? 'good' : /^(failed|error|invalid|unhealthy|timeout|resource_timeout|start_failed)$/.test(key) ? 'bad' : /^(running|reserved|queued|stopping)$/.test(key) ? 'busy' : '';
    return '<span class="badge '+tone+'" data-status="'+esc(key)+'">'+esc(translations[key] || display(value))+'</span>';
  }
  function raw(value,title='原始服务端字段') {
    if (value == null) return '';
    const text = JSON.stringify(value,null,2);
    return '<details><summary>'+esc(title)+'</summary><pre>'+esc(text.length > 24000 ? text.slice(0,24000)+'\n… 显示已截断；完整数据可由管理 API 查询。' : text)+'</pre></details>';
  }
  function kv(pairs) {
    return '<dl class="kv">'+pairs.map(([key,value])=>'<dt>'+esc(key)+'</dt><dd>'+esc(display(value))+'</dd>').join('')+'</dl>';
  }
  function metric(label,value,note) {
    return '<div class="metric"><div class="label">'+esc(label)+'</div><div class="value">'+esc(display(value))+'</div><div class="note">'+esc(note)+'</div></div>';
  }
  function table(headers,rows,id) {
    return '<table'+(id?' id="'+esc(id)+'"':'')+'><thead><tr>'+headers.map(h=>'<th>'+esc(h)+'</th>').join('')+'</tr></thead><tbody>'+rows.map(cells=>'<tr>'+cells.map(cell=>'<td>'+cell+'</td>').join('')+'</tr>').join('')+'</tbody></table>';
  }
  const overview = () => object(body(state.overview,'overview'));
  const runtime = () => object(pick([overview()],['runtime','snapshot.runtime','snapshot']) ?? overview());
  const scheduler = () => object(pick([overview(),runtime()],['scheduler','snapshot.scheduler']) ?? runtime());
  const apps = () => records(body(state.apps,'apps'));
  const appId = app => String(pick([app],['app_id','id','_key']) ?? '');
  const appRequest = app => object(pick([app],['request','config.request','guidance.request','config','guidance']));
  const appWorkflow = app => String(pick([app],['workflow_id','latest_workflow_id','last_workflow_id','workflow.workflow_id','workflow.id','runtime.workflow_id','handle.workflow_id']) ?? '');
  const selectedApp = () => apps().find(app=>appId(app)===state.selectedApp);
  const active = app => Boolean(app && /^(running|queued|pending|starting|stopping|cancelling|reserved)$/.test(String(app.status ?? app.state)));
  const workflow = () => object(body(state.lastWorkflow,'workflow'));
  const globalTasks = () => records(pick([scheduler(),runtime(),overview()],['tasks','queue.tasks','queue']) ?? []);
  const workflowTasks = () => records(pick([workflow()],['tasks','scheduler.tasks','snapshot.tasks']) ?? []);
  function workflowRecords() {
    const map = new Map();
    records(pick([scheduler(),runtime(),overview()],['workflows']) ?? []).forEach(item=>{
      const id = String(item.workflow_id ?? item.id ?? item._key ?? ''); if(id) map.set(id,{...item,workflow_id:id});
    });
    apps().forEach(app=>{const id=appWorkflow(app);if(id && !map.has(id))map.set(id,{workflow_id:id,app_id:appId(app),status:app.status});});
    if (state.workflowId && !map.has(state.workflowId)) map.set(state.workflowId,{workflow_id:state.workflowId});
    return [...map.values()];
  }
  function errorText(error) {return error instanceof Error ? error.message : display(error);}
  function renderErrors() {
    const errors = Object.entries(state.errors).filter(([,value])=>value).map(([key,value])=>key+'：'+value);
    $('error-banner').hidden = errors.length===0;
    $('error-banner').textContent = errors.join('\n')+(errors.length && location.protocol==='file:' ? '\n本地 HTML 默认连接 127.0.0.1:8000；也可从服务端 /traffic/platform/ui 打开。' : '');
  }
  async function api(path,options={}) {
    const get = !options.method || options.method==='GET';
    if (get && state.pending.has(path)) return state.pending.get(path);
    const request = (async()=>{
      const controller = new AbortController();
      const timeout = setTimeout(()=>controller.abort(),get?20000:90000);
      try {
        const response = await fetch(API_BASE+path,{...options,signal:controller.signal,cache:'no-store',headers:{Accept:'application/json',...(options.body?{'Content-Type':'application/json'}:{}),...options.headers}});
        const text = await response.text(); let data;
        try {data=text?JSON.parse(text):{};} catch (_) {throw new Error('接口返回了非 JSON 内容（HTTP '+response.status+'）。');}
        if (!response.ok) throw new Error('HTTP '+response.status+' · '+display(data.detail ?? data.error ?? data.message ?? response.statusText));
        return freeze(data);
      } catch (error) {
        if (error.name==='AbortError') throw new Error(get?'读取超时，请刷新重试。':'操作响应超时；正在刷新服务端状态，请确认当前应用再决定下一步。');
        if (error instanceof TypeError) throw new Error('无法连接管理 API，请检查本地服务和接口地址。');
        throw error;
      } finally {clearTimeout(timeout);}
    })();
    if (get) state.pending.set(path,request);
    try {return await request;} finally {if(get)state.pending.delete(path);}
  }
  async function readEndpoint(key,path) {
    try {state[key]=await api(path);delete state.errors[key];if(key==='overview'){state.connected=true;state.lastRefresh=new Date();}}
    catch(error){state.errors[key]=errorText(error);if(key==='overview')state.connected=false;}
  }
  async function loadWorkflow() {
    const id=state.workflowId;
    if(!id){state.lastWorkflow=null;delete state.errors.workflow;return;}
    try {const value=await api('/workflows/'+encodeURIComponent(id));if(state.workflowId===id){state.lastWorkflow=value;delete state.errors.workflow;}}
    catch(error){if(state.workflowId===id)state.errors.workflow=errorText(error);}
  }
  function refresh() {
    if(state.refreshing)return state.refreshing;
    $('refresh-status').textContent='正在读取服务端状态…';$('refresh-btn').disabled=true;
    state.refreshing=(async()=>{
      await Promise.allSettled([readEndpoint('overview','/overview'),readEndpoint('catalog','/catalog'),readEndpoint('templates','/templates'),readEndpoint('apps','/apps'),readEndpoint('resources','/resources'),readEndpoint('qos','/qos')]);
      const list=apps();
      if(!list.some(app=>appId(app)===state.selectedApp))state.selectedApp=appId(list[0]??{});
      if(!state.workflowId)state.workflowId=selectedApp()?appWorkflow(selectedApp()):String(workflowRecords()[0]?.workflow_id ?? '');
      await loadWorkflow();render();
    })().finally(()=>{state.refreshing=null;$('refresh-btn').disabled=state.mutating;renderControls();});
    return state.refreshing;
  }
  function syncOptions(id,items,selected,fallback) {
    const select=$(id);const html=items.length?items.map(([value,label])=>'<option value="'+esc(value)+'">'+esc(label)+'</option>').join(''):'<option value="">'+esc(fallback)+'</option>';
    if(select.innerHTML!==html)select.innerHTML=html;
    if(items.some(([value])=>String(value)===String(selected)))select.value=String(selected);
    select.disabled=!items.length;
  }
  function renderControls() {
    const app=selectedApp();const busy=state.mutating;
    $('create-btn').disabled=busy||!state.connected;$('create-btn').textContent=busy&&state.action==='create'?'创建中…':'创建应用';
    $('run-btn').disabled=busy||!state.connected||!app||active(app);
    $('run-btn').textContent=busy&&state.action==='run'?'提交运行中…':'运行当前应用';
    $('stop-btn').disabled=busy||!state.connected||!app||!active(app);
    $('stop-btn').textContent=busy&&state.action==='stop'?'请求停止中…':'停止应用';
    $('refresh-btn').disabled=busy||Boolean(state.refreshing);
    document.querySelectorAll('[data-action]').forEach(button=>{
      const item=apps().find(app=>appId(app)===button.dataset.app);
      button.disabled=busy || (button.dataset.action!=='view' && (!state.connected || (button.dataset.action==='run'?active(item):!active(item))));
    });
    $('current-app-status').innerHTML=app?badge(app.status??app.state)+' '+esc(app.name??appId(app)):'未选择应用';
    if(state.lastRefresh)$('refresh-status').textContent=(state.mutating?'操作中 · ':state.connected?'已刷新 · ':'保留上次数据 · ')+state.lastRefresh.toLocaleTimeString('zh-CN',{hour12:false})+' · 可见页面每 5 秒读取状态';
  }
  function renderOverview() {
    const ready=pick([overview(),runtime()],['ready','initialized','service_ready']);
    $('service-ready').dataset.ready=String(state.connected);
    $('service-ready').className='badge '+(state.connected?(ready===false?'busy':'good'):'bad');
    $('service-ready').textContent=state.connected?(ready===true?'● 运行服务就绪':ready===false?'● 管理接口已连接 · 运行时待初始化':'● 管理接口已连接'):'管理服务连接失败';
    const loaded=state.apps!==null, tasks=globalTasks(), workflows=workflowRecords();
    $('overview-metrics').innerHTML=metric('应用总数',loaded?apps().length:undefined,'管理 API 应用记录')+metric('运行中的应用',loaded?apps().filter(active).length:undefined,'按应用状态统计')+metric('全局任务数',pick([scheduler(),runtime(),overview()],['tasks'])!==undefined?tasks.length:undefined,'跨 workflow 调度任务')+metric('工作流记录',pick([scheduler(),runtime(),overview()],['workflows'])!==undefined?workflows.length:undefined,'服务端已返回的工作流');
    $('policy-badge').textContent='调度策略 · '+display(pick([scheduler(),runtime(),overview()],['policy','config.policy']));
  }
  function renderTemplates() {
    const templates=records(body(state.templates,'templates'));
    const allowed=new Set(['flow','analysis','od_hotspots']);
    const options=templates.map(item=>[String(item.template??item.template_id??item.id??item._key??item._value??''),String(item.name??item.title??item.label??item.template??item._key??item._value??'')]).filter(([id])=>allowed.has(id));
    const selected=$('template-select').value;
    if(options.length)syncOptions('template-select',options,selected,'暂无模板');
    const template=templates.find(item=>String(item.template??item.template_id??item.id??item._key??item._value)===$('template-select').value);
    $('template-note').textContent=template?display(template.description??template.summary??'模板由管理 API 返回；运行后查看实际编译计划。'):state.errors.templates?'模板目录读取失败；请求使用已约定的 flow / analysis / od_hotspots 模板。':'服务端未返回模板说明；运行后查看实际编译计划。';
  }
  function renderApps() {
    const list=apps();$('app-count').textContent=state.apps===null?'未返回':String(list.length);
    $('apps-updated').textContent=state.errors.apps?'应用读取失败 · 保留上次记录':state.apps===null?'等待服务端应用':'服务端应用记录';
    $('app-list').innerHTML=list.length?table(['应用 / 模板','状态 / 最近运行','工作流','操作'],list.map(app=>{
      const id=appId(app),wf=appWorkflow(app),request=appRequest(app);
      return ['<strong>'+esc(app.name??id)+'</strong><span class="id">'+esc(id)+'</span><span class="badge blue">'+esc(display(request.template??app.template))+'</span>',badge(app.status??app.state)+(app.last_run_status?'<div style="margin-top:5px">'+badge(app.last_run_status)+'</div>':''),wf?'<code>'+esc(wf)+'</code>':'<span class="muted">尚未运行</span>','<div class="actions"><button class="small" data-action="view" data-app="'+esc(id)+'">查看</button><button class="small" data-action="run" data-app="'+esc(id)+'">运行</button><button class="small danger" data-action="stop" data-app="'+esc(id)+'">停止</button></div>'];
    }),'apps-table'):empty(state.apps===null?'等待应用接口；连接后即可创建。':'尚无应用。填写左侧配置，创建第一个交通应用。');
    syncOptions('app-select',list.map(app=>[appId(app),String(app.name??appId(app))]),state.selectedApp,'暂无应用');
    const app=selectedApp();$('app-detail').className='';
    $('app-detail').innerHTML=app?kv([['应用 ID',appId(app)],['应用名称',app.name],['业务模板',appRequest(app).template??app.template],['目标帧',appRequest(app).target_frames],['截止时限（秒）',appRequest(app).deadline_seconds],['优先级',appRequest(app).priority],['推理复用',appRequest(app).enable_reuse],['失败恢复',appRequest(app).enable_recovery],['最近运行状态',app.last_run_status],['工作流 ID',appWorkflow(app)||undefined],['服务端错误',app.error??app.last_error]])+raw(app,'应用原始记录'):empty('创建或选择一个应用，查看真实配置与最近工作流。');
  }
  const capabilityRoles={
    'history.prepare':'读取目标此前的同日历史，准备模型输入。',
    'traffic.predict':'调用交通模型，联合预测状态、流量与 OD。',
    'validate.state':'校验状态预测的张量布局与时空契约。',
    'validate.flow':'校验流量预测的张量布局与时空契约。',
    'validate.od':'校验 OD 预测的张量布局与时空契约。',
    'analysis.congestion':'按规则分析状态与流量中的拥堵代理指标。',
    'analysis.od_hotspots':'按预测 OD 行程数提取热点起终点对。',
    'result.aggregate':'汇总请求需要的预测和分析结果。'
  };
  function compactUnits(units) {
    if(!Array.isArray(units))return display(units);
    if(!units.length)return '未声明单位';
    const groups=[];
    units.forEach(unit=>{const last=groups.at(-1);if(last&&last.unit===unit)last.count++;else groups.push({unit,count:1});});
    return groups.map(({unit,count})=>display(unit)+(count>1?' ×'+count:'')).join(' / ');
  }
  function portSummary(value,direction,path='') {
    if(value==null)return '<div class="port-summary">'+esc(direction)+'端口：未返回</div>';
    if(isObject(value)&&(value.axes!==undefined||value.shape!==undefined)) {
      const axes=Array.isArray(value.axes)?value.axes.join(','):display(value.axes);
      const shape=Array.isArray(value.shape)?'['+value.shape.map((size,index)=>size===null?(value.axes?.[index]==='B'?'B':'?'):display(size)).join(',')+']':display(value.shape);
      return '<div class="port-summary"><strong>'+esc(direction+' · '+(path||value.field||'未返回端口名'))+'</strong><div class="mono">'+esc(axes+' · '+shape)+'</div><div>'+esc(compactUnits(value.units??value.unit))+' · '+esc(display(value.normalization))+'</div>'+(value.version!=null?'<div class="muted">版本 '+esc(display(value.version))+'</div>':'')+'</div>';
    }
    const entries=Array.isArray(value)?value.map((item,index)=>[String(item.port??item.name??index),item]):Object.entries(object(value));
    if(!entries.length)return '<div class="port-summary">'+esc(direction+' · '+(path||'端口'))+'：'+esc(isObject(value)||Array.isArray(value)?'未声明张量端口':display(value))+'</div>';
    return entries.map(([key,item])=>portSummary(item,direction,path?path+'.'+key:key)).join('');
  }
  function renderCatalog() {
    const catalog=object(body(state.catalog,'catalog'));
    const implementations=records(catalog.implementations??catalog.implementation);
    const instances=records(catalog.instances??catalog.agents);
    const capabilityMap=new Map();
    records(catalog.capabilities).forEach(item=>capabilityMap.set(String(item.capability_id??item.id??item._key??item._value??''),item));
    implementations.forEach(implementation=>records(implementation.capabilities).forEach(item=>{
      const id=String(item.capability_id??item.id??item._key??item._value??'');
      if(id&&!capabilityMap.has(id))capabilityMap.set(id,item);
    }));
    const capabilities=[...capabilityMap.entries()];
    $('catalog-count').textContent=state.catalog===null?'目录未返回':capabilities.length+' 能力 / '+implementations.length+' 实现 / '+instances.length+' 实例';
    $('capabilities-content').innerHTML=capabilities.length?capabilities.map(([id,item])=>{
      const contract=object(item.contract),resources=object(item.resources??item.resource_profile);
      const grid=Array.isArray(contract.grid)?contract.grid.join('×'):display(contract.grid);
      return '<article class="catalog-card"><h3>'+esc(id)+'</h3><p class="cap-role">'+esc(display(item.description??item.name??capabilityRoles[id]))+'</p>'+kv([['资源',display(resources.cpu)+' CPU / '+display(resources.memory_mb)+' MiB'],['预计耗时',display(item.expected_seconds??item.profile?.expected_seconds)+' 秒'],['网格',grid+(contract.grid_id?' · '+display(contract.grid_id):'')],['版本',item.version??contract.model_version??contract.version]])+portSummary(contract.inputs??item.inputs,'输入')+portSummary(contract.outputs??item.outputs,'输出')+raw(item,'完整原始能力 / contract')+'</article>';
    }).join(''):empty('服务端尚未返回能力目录。');
    $('implementations-content').innerHTML=implementations.length?implementations.map(item=>'<article class="catalog-card" style="margin-bottom:10px"><h3>'+esc(display(item.implementation_id??item.id??item._key))+'</h3>'+kv([['版本',item.version]])+'<div class="chips">'+records(item.capabilities).map(capability=>'<span class="chip">'+esc(display(capability.capability_id??capability.id??capability._key??capability._value))+'</span>').join('')+'</div>'+raw(item,'完整实现描述')+'</article>').join(''):empty('服务端尚未返回 implementation。');
    const executions=records(catalog.executions);
    $('instances-content').innerHTML=instances.length?table(['实例 / 实现','健康与生命周期','并发 / 实测'],instances.map(item=>{
      const id=String(item.instance_id??item.id??item._key??'');
      const live=object(pick([runtime(),overview()],['runtime_instances.'+id,'instance_states.'+id,'instances.'+id]));
      const observed=executions.filter(entry=>String(entry.instance_id)===id);
      const executionCount=observed.length&&observed.every(entry=>typeof entry.executions==='number')?observed.reduce((sum,entry)=>sum+entry.executions,0):undefined;
      return ['<code>'+esc(display(id))+'</code><span class="id">'+esc(display(item.implementation_id))+'</span>'+esc(display(item.endpoint)),(item.healthy===true?badge('healthy'):item.healthy===false?badge('unhealthy'):badge(item.health))+'<span class="id">'+esc(display(live.lifecycle??item.status??item.lifecycle))+'</span>'+(item.health_reason??item.reason?esc(display(item.health_reason??item.reason)):''),kv([['并发上限',item.max_concurrency],['基础内存 MiB',item.base_memory_mb],['实际执行次数',executionCount]])+raw({instance:item,runtime:live,executions:observed},'完整实例 / 执行 profile')];
    })):empty('服务端尚未返回实际实例。');
    $('catalog-raw').innerHTML=raw(state.catalog,'完整目录响应');
  }
  function taskId(task) {return String(task.node_id??task.spec?.node_id??task.task_id??task._key??'');}
  function eventsFrom(sources,paths) {
    const values=pick(sources,paths);return records(values);
  }
  function renderEvents(id,events,fallback) {
    $(id).innerHTML=events.length?events.slice(-60).reverse().map(event=>'<div class="event"><strong>'+esc(display(event.event??event.type??event.action??event._key))+'</strong> '+(event.status??event.call_status?badge(event.status??event.call_status):'')+'<div class="muted">'+esc(display(event.workflow_id??event.task_id??event.node_id))+' · t='+esc(display(event.now??event.timestamp??event.time))+'</div><div class="muted">'+esc(display(event.reason??event.selection?.reason??event.error??event.instance_id??event.message))+'</div>'+raw(event,'事件字段')+'</div>').join(''):empty(fallback);
  }
  function selectionReason(reason) {
    if(reason==='feasible instance with minimum EFT; profile includes execution, cold start and adapter')return '在可行实例中选择预计完成时间（EFT）最小者；估计包含执行、冷启动与适配耗时。';
    return display(reason);
  }
  function renderDAG() {
    const wf=workflow();const plan=object(pick([wf],['plan','compiled_plan','workflow_plan','workflow.plan','result.plan']));
    const tasks=workflowTasks();const explicitNodes=plan.nodes??wf.nodes;
    // Task node IDs and dependencies are the scheduler's actual compiled specs.
    const nodes=explicitNodes!==undefined?records(explicitNodes):tasks;
    $('plan-raw').innerHTML=raw(Object.keys(plan).length?plan:null,'实际编译计划 / 契约');
    if(!nodes.length){$('dag-content').innerHTML=empty('服务端尚未返回实际编译 DAG。');$('dag-note').textContent='等待计划字段';return;}
    const nodeMap=new Map(nodes.map(node=>[String(node.node_id??node.id??node._key??''),node]));
    const depths=new Map();const visiting=new Set();
    const dependencies=node=>Array.isArray(node.depends_on??node.dependencies)?node.depends_on??node.dependencies:records(node.depends_on??node.dependencies).map(item=>item.node_id??item._key??item._value);
    function depth(id) {
      if(depths.has(id))return depths.get(id);if(visiting.has(id))return 0;
      visiting.add(id);const deps=dependencies(nodeMap.get(id)??{}).filter(dep=>nodeMap.has(String(dep)));
      const level=deps.length?1+Math.max(...deps.map(dep=>depth(String(dep)))):0;visiting.delete(id);depths.set(id,level);return level;
    }
    const levels=new Map();nodes.forEach(node=>{const id=String(node.node_id??node.id??node._key??'');const d=depth(id);if(!levels.has(d))levels.set(d,[]);levels.get(d).push(node);});
    $('dag-note').textContent=nodes.length+' 个服务端节点 · '+(explicitNodes!==undefined?'编译计划':'实际任务依赖');
    $('dag-content').innerHTML='<div class="dag">'+[...levels.entries()].sort(([a],[b])=>a-b).map(([level,items])=>'<div class="dag-level"><div class="level-title">依赖层 '+(level+1)+'</div>'+items.map(node=>{
      const id=String(node.node_id??node.id??node._key??'');const task=tasks.find(task=>taskId(task)===id||taskId(task)===state.workflowId+':'+id)??{};
      const selection=object(task.selection??task.selection_reason);
      const candidatePayload=selection.alternatives??selection.options??node.candidates;
      const candidates=records(candidatePayload);const instance=task.instance_id??task.assignment?.instance_id??selection.instance_id;
      const reason=pick([task,node],['selection.reason','selection_reason.reason','selection_reason','reason','assignment.reason','decision.reason']);
      const deps=dependencies(node);
      const candidateHTML=candidates.map(candidate=>{const candidateId=candidate.instance_id??candidate._key??candidate._value;return '<div class="candidate-item'+(candidateId===instance?' selected':'')+'"><strong>'+esc(display(candidateId))+'</strong>'+(candidateId===instance?' · 已选':'')+'<br>EFT '+esc(display(candidate.estimated_finish))+'（单调秒） · '+(candidate.feasible===true?'可行':candidate.feasible===false?'不可行':'可行性未返回')+'</div>';}).join('');
      return '<div class="dag-node" data-node-id="'+esc(id)+'"><div class="cap">'+esc(display(node.capability_id??node.capability))+'</div><span class="id">'+esc(id)+'</span>'+badge(task.schedule_status??task.status)+'<div class="deps">依赖：'+esc(deps.join(' · ')||(node.depends_on!==undefined||node.dependencies!==undefined?'无（计划根节点）':'未返回'))+'</div><div class="instance">选中实例：'+esc(display(instance))+'<br>候选：'+(candidatePayload!=null?candidates.length+' 个':'未返回')+'</div><div class="candidate-list">'+candidateHTML+'</div><div class="reason">选择理由：'+esc(selectionReason(reason))+'</div>'+raw(task.selection??task.selection_reason,'原始选择理由 / 候选评分')+raw(node,'节点 / 候选契约')+'</div>';
    }).join('')+'</div>').join('')+'</div>';
  }
  function renderOrchestration() {
    const wf=workflow();syncOptions('workflow-select',workflowRecords().map(item=>[String(item.workflow_id),String(item.workflow_id)+(item.status?' · '+(translations[item.status]??item.status):'')]),state.workflowId,'暂无工作流');
    const flowState=object(scheduler().workflows?.[state.workflowId]);
    const owner=apps().find(app=>appId(app)===(wf.app_id??wf.result?.app_id??flowState.app_id)||appWorkflow(app)===state.workflowId);
    const deadline=pick([wf,flowState],['deadline','plan.deadline']);
    const submitted=pick([wf,flowState],['submitted_at','result.submitted_at']);
    const finished=pick([wf,flowState],['finished_at','result.finished_at']);
    const declared=pick([wf,flowState,appRequest(owner??{})],['request.deadline_seconds','deadline_seconds']);
    const finite=value=>typeof value==='number'&&Number.isFinite(value);
    const derived=finite(deadline)&&finite(submitted)&&deadline>=submitted?deadline-submitted:undefined;
    const seconds=finite(declared)&&declared>0?declared:derived;
    const done=pick([wf,flowState],['done','result.done']);
    const reportedMissed=pick([wf,flowState],['deadline_missed']);
    const missed=typeof reportedMissed==='boolean'?reportedMissed:done===true&&finite(deadline)&&finite(finished)?finished>deadline:undefined;
    const timely=seconds===undefined?'按期信息未返回':missed===true?'已超期':done===false?'尚未终态':done===true&&missed===false?(/^(success|succeeded|completed)$/.test(String(wf.status??flowState.status))?'按期完成':'终态未超期'):'按期信息未返回';
    $('workflow-banner').innerHTML=state.workflowId?'<code>'+esc(state.workflowId)+'</code>'+badge(wf.status??flowState.status)+'<span class="muted">模板 '+esc(display(wf.template??wf.plan?.template??wf.result?.template??flowState.template))+' · 目标帧 '+esc(display(wf.target_frames??wf.plan?.target_frames??wf.result?.target_frames))+' · 业务时限 '+esc(display(seconds))+(seconds!==undefined?' 秒':'')+' · '+esc(timely)+'</span>'+raw({deadline,submitted_at:submitted,finished_at:finished,deadline_missed:reportedMissed,done,request_deadline_seconds:declared,derived_deadline_seconds:derived},'原始时限 / 单调时间记录'):empty('运行应用后选择工作流。');
    renderDAG();const tasks=globalTasks();$('queue-count').textContent=tasks.length?tasks.length+' 个全局任务':pick([scheduler(),runtime(),overview()],['tasks'])!==undefined?'0 个任务':'队列未返回';
    $('queue-content').innerHTML=tasks.length?table(['工作流 / 节点','能力 / 实例','调度状态','调用状态','质量状态','尝试 / 选择理由'],tasks.map(task=>['<code>'+esc(display(task.workflow_id))+'</code><span class="id">'+esc(taskId(task))+'</span>','<code>'+esc(display(task.capability_id??task.spec?.capability_id))+'</code><span class="id">'+esc(display(task.instance_id))+'</span>',badge(task.schedule_status??task.status),badge(task.call_status),badge(task.quality_status),esc(display(task.attempt??task.attempts))+'<span class="id">'+esc(display(task.selection?.reason??task.selection_reason?.reason??task.selection_reason??task.reason??task.error))+'</span>'+raw(task.selection??task.selection_reason,'完整选择理由 / 候选评分')]),'queue-table'):empty('全局调度队列暂无记录。');
    const sources=[scheduler(),runtime(),overview(),wf];
    const all=eventsFrom(sources,['events','task_events','scheduler.events']);
    const selection=eventsFrom(sources,['selection_events','dispatch_events','decisions']);
    renderEvents('selection-events',selection.length?selection:all.filter(event=>/select|assign|dispatch|reserve|retry|start/.test(String(event.event??event.type??''))),'服务端尚未返回实例选择或调度事件。');
    const recovery=eventsFrom(sources,['recovery_events','failure_events']);
    renderEvents('recovery-events',recovery.length?recovery:all.filter(event=>/fail|recover|retry|cancel|timeout|health/.test(String(event.event??event.type??''))||/^(failed|start_failed)$/.test(String(event.schedule_status))||/^(failed|error|timeout)$/.test(String(event.call_status))),'尚无服务端失败或恢复事件。');
  }
  function quota(label,used,budget,unit) {
    const numeric=typeof used==='number'&&typeof budget==='number'&&budget>0&&Number.isFinite(used)&&Number.isFinite(budget);
    return '<div class="quota-row"><div class="quota-label"><span>'+esc(label)+'</span><span>'+esc(display(used))+' / '+esc(display(budget))+' '+esc(unit)+'</span></div>'+(numeric?'<div class="quota-track" aria-label="'+esc(label)+'账本占用"><div class="quota-fill" style="width:'+Math.max(0,Math.min(100,used/budget*100))+'%"></div></div>':'<p class="hint">占用或配额字段未返回。</p>')+'</div>';
  }
  function renderResources() {
    const payload=object(state.resources?.data??state.resources);const sources=[payload,scheduler(),runtime(),overview()];
    const pool=object(pick([payload],['pool','resources','resource_pool'])??payload);
    const budgets=object(pick(sources,['budgets','config']));const pools=[pool,...sources];
    $('quota-content').innerHTML=quota('CPU 任务配额',pick(pools,['cpu_used','usage.cpu_used']),pick([...pools,budgets],['cpu_budget']),'核配额')+quota('内存任务预留',pick(pools,['memory_used_mb','usage.memory_used_mb']),pick([...pools,budgets],['memory_budget','memory_budget_mb','task_memory_budget']),'MiB')+quota('共享并发',pick(pools,['concurrency','active_count']),pick([...pools,budgets],['max_concurrency']),'任务')+raw(pick(sources,['peaks'])??pool.peaks,'服务端资源峰值');
    const measured=pick([payload,runtime(),overview()],['runtime_resources.measurement','resources.measurement','measurement','measured','measurements','resource_measurements','measured_resources','process_metrics','profile.resources']);
    const runtimeResources=pick([payload,runtime(),overview()],['runtime_resources','resources']);
    const profiles=records(pick([runtime(),overview(),payload],['profiles','instance_profiles','performance']));
    $('measured-content').innerHTML=(measured!=null?kv(Object.entries(object(measured)).map(([key,value])=>[key,value]))+raw(measured,'完整实测字段'):empty('服务端尚未返回资源实测字段。配额账本不会代替实测。'))+(profiles.length?'<div class="divider"></div><h3>实际操作耗时与进程记录</h3><div class="table-wrap">'+table(['能力 / 实例','墙钟 / 线程 CPU 秒','RSS 前 / 后 MiB'],profiles.slice(-8).reverse().map(item=>[esc(display(item.capability_id))+'<span class="id">'+esc(display(item.instance_id))+'</span>',esc(display(item.duration_seconds))+' / '+esc(display(item.thread_cpu_seconds)),esc(display(item.rss_before_mb))+' / '+esc(display(item.rss_after_mb))]))+'</div>':'')+raw(runtimeResources,'基础模型 / 缓存 / 任务配额原始字段');
    const cache=pick([runtime(),overview(),payload],['cache','cache_stats','inference.cache']);
    const inference=pick([runtime(),overview(),payload],['inference','inference_stats','profile']);
    const metrics=[object(inference),runtime(),overview(),object(cache)];
    const eventPayload=pick([scheduler(),runtime(),overview()],['events','task_events']);
    const retries=eventPayload!==undefined?records(eventPayload).filter(event=>event.event==='retry').length:undefined;
    $('inference-metrics').innerHTML=metric('实际模型推理次数',pick(metrics,['model_forward_count','actual_inference_count','actual_inferences','inference_count','predict_calls','forward_count']),'服务端模型 forward 计数')+metric('缓存命中',pick([object(cache),...metrics],['hits','cache_hits']),'服务端缓存记录')+metric('在途请求合并',pick([object(cache),...metrics],['singleflight_hits','coalesced','inflight_hits','coalesced_hits','merged_requests']),'同完整键请求合并')+metric('实际重试次数',pick(metrics,['recovery_count','recoveries','retry_count'])??retries,'按服务端 retry 事件统计');
    const qos=body(state.qos,'qos');
    const qosRows=records(qos);
    $('qos-content').innerHTML=qos!=null&&Object.keys(Object(qos)).length?raw(qos,'完整 QoS 响应')+(Array.isArray(qos)?'<div class="table-wrap">'+table(['实例','实际调用 / 成功 / 失败','成功率（0–1）','平均延迟 ms'],qosRows.slice(-20).map(item=>[esc(display(item.agent_id??item.instance_id)),esc(display(item.total_calls))+' / '+esc(display(item.success_count))+' / '+esc(display(item.failure_count)),esc(display(item.success_rate)),esc(display(item.avg_latency_ms))]))+'</div>':kv(Object.entries(object(qos)).slice(0,10))):empty('服务端尚无 QoS 记录。');
    $('cache-content').innerHTML=cache!=null||inference!=null||profiles.length?kv([['缓存条目',object(cache).entries],['缓存字节 / 上限',cache?display(cache.bytes)+' / '+display(cache.max_bytes):undefined],['在途完整键',object(cache).inflight],['成功模型 forward',pick(metrics,['model_forward_successes'])],['实际执行 Future',pick(metrics,['active_futures'])]])+raw(cache,'缓存原始字段')+raw(inference,'推理统计原始字段')+raw(profiles.length?profiles:null,'实例实测 profile'):empty('服务端尚未返回缓存或推理 profile。');
    const ledgerPayload=pick(sources,['resource_events','pool.resource_events'])??pool.events;
    const ledger=records(ledgerPayload);
    $('ledger-count').textContent=ledgerPayload!==undefined?ledger.length+' 条服务端事件':'账本未返回';
    $('ledger-content').innerHTML=ledger.length?table(['时间（单调秒）','事件 / 节点','预留 ID','CPU / MiB','占用 / 并发','真实终态确认'],ledger.slice(-80).reverse().map(event=>[esc(display(event.now??event.time)),esc(display(event.event??event.type))+'<span class="id">'+esc(display(event.task_id))+'</span>','<code>'+esc(display(event.reservation_id))+'</code>',esc(display(event.cpu))+' / '+esc(display(event.memory_mb)),esc(display(event.cpu_used))+' CPU · '+esc(display(event.memory_used_mb))+' MiB · '+esc(display(event.concurrency)),esc(display(event.execution_finished))]),'resource-ledger'):empty('服务端尚未返回资源预留与释放事件。');
    $('resource-raw').innerHTML=raw(state.resources,'完整资源响应');
  }
  function predictionRecords() {
    const wf=workflow();const output=pick([wf],['predictions','result.predictions','results.predictions','outputs','result.outputs']);
    return records(output).map(item=>({...item,_name:String(item.task??item.name??item.output_key??item._key??'')}));
  }
  const predictionData = item => item?.prediction??item?.values??item?.data??item?.grid;
  function channelLabel(name,index,task) {
    const labels={dropoffs_in_interval:'落客流量',pickups_in_interval:'上客流量',active_trip_count_at_interval_end_by_origin:'活动行程数（代理）',mean_completed_trip_speed_by_destination:'已完成行程平均速度'};
    return labels[name]??(task==='od'?'终点 '+index+' · 网格 ['+Math.floor(index/6)+', '+index%6+']':name??'通道 '+index);
  }
  function analysisView(summary,sample) {
    if(summary==null)return '';
    const items=summary.frames?[summary]:records(summary);
    return items.map(item=>{
      const frames=records(item.frames),frame=frames[sample];
      if(!frame)return '';
      const fields=Object.entries(frame).filter(([key])=>!['pairs','mask','origin_totals','destination_totals','target_frame'].includes(key));
      const pairs=records(frame.pairs);
      return '<div class="status-line"><span class="badge blue">'+esc(display(item.method??item._key))+'</span><span class="muted">目标帧 '+esc(display(frame.target_frame))+'</span></div>'+kv(fields)+
        (pairs.length?'<div class="table-wrap">'+table(['OD 起点 → 终点','预测行程数'],pairs.map(pair=>[esc(display(pair.origin))+' → '+esc(display(pair.destination)),esc(display(pair.trips))]))+'</div>':'')+
        (item.limitations?'<p class="hint">'+esc(item.limitations)+'</p>':'')+raw(item,'服务端分析定义与全部样本');
    }).join('');
  }
  function renderResults() {
    const wf=workflow(),predictions=predictionRecords();
    const frames=pick([wf],['target_frames','plan.target_frames','request.target_frames','result.target_frames']);
    const ranges=pick([wf],['target_time_range','target_times','result.target_time_range','result.target_times']);
    const samples=Array.isArray(frames)?frames.map((frame,index)=>[String(index),'帧 '+frame+' · '+(ranges?.[index]?.start??frameTime(frame))]):Array.isArray(ranges)?ranges.map((range,index)=>[String(index),String(range.start??range)]):[];
    syncOptions('result-time',samples,$('result-time').value,'目标样本未返回');
    syncOptions('result-task',predictions.map(item=>[item._name,item._name]),$('result-task').value,'暂无预测输出');
    const prediction=predictions.find(item=>item._name===$('result-task').value);
    const channels=prediction?.channels??prediction?.channel_names;
    const count=Array.isArray(channels)?channels.length:Array.isArray(prediction?.shape)?prediction.shape.at(-1):0;
    syncOptions('result-channel',Number.isInteger(count)&&count>0&&count<=256?Array.from({length:count},(_,index)=>[String(index),String(channelLabel(channels?.[index],index,prediction?._name))]):[],$('result-channel').value,'通道未返回');
    $('result-context').textContent=state.workflowId?'工作流 '+state.workflowId+' · '+display(wf.status)+' · 独立一步样本，90 分钟历史 → 30 分钟预测':'选择已运行应用的工作流，查看服务端预测或分析摘要。';
    const summary=pick([wf],['summary','analysis','result.summary','result.analysis','results.summary','result','results']);
    const sample=Number($('result-time').value);
    const analysisHTML=analysisView(summary,sample);
    $('result-summary').innerHTML=analysisHTML||(summary!=null?raw(summary,'服务端结果 / 分析摘要')+(!Array.isArray(summary)?kv(Object.entries(object(summary)).filter(([key])=>!['predictions','outputs'].includes(key)).slice(0,12)):kv([['摘要',summary]])):empty('服务端尚未返回结果摘要。'));
    if(prediction)$('result-summary').innerHTML=kv([['实际输出',prediction._name],['布局 / 形状',display(prediction.layout)+' / '+display(prediction.shape)],['目标帧',Array.isArray(frames)?frames[sample]:undefined],['目标起点',Array.isArray(frames)?frameTime(frames[sample]):ranges?.[sample]?.start],['模型版本',wf.model_version??wf.result?.model_version],['物理单位',prediction.units??prediction.unit]])+'<div class="divider"></div>'+$('result-summary').innerHTML;
    $('result-raw').innerHTML=raw(prediction?{...prediction,prediction:undefined,values:undefined,data:undefined,grid:undefined}:null,'输出布局 / 单位 / 版本');
    renderGrid(prediction);
  }
  function renderGrid(prediction) {
    $('grid-legend').hidden=true;$('grid-note').textContent='';
    if(!prediction){$('result-grid').className='empty';$('result-grid').textContent='服务端尚未返回预测网格；可查看右侧分析摘要。';return;}
    const data=predictionData(prediction),shape=prediction.shape,layout=prediction.layout??prediction.axes?.join(',');
    const sample=Number($('result-time').value),channel=Number($('result-channel').value);
    let grid;
    if(Array.isArray(data) && Array.isArray(shape) && shape.length===5 && (layout==='B,H,Y,X,C'||layout==='B,H,Y,X,D')) {
      grid=data[sample]?.[0];
      if(Array.isArray(grid))grid=grid.map(row=>Array.isArray(row)?row.map(cell=>Array.isArray(cell)?cell[channel]:undefined):[]);
    } else if(Array.isArray(data) && Array.isArray(shape) && shape.length===2)grid=data;
    else if(Array.isArray(prediction.grid) && prediction.grid.every(row=>Array.isArray(row)&&row.every(value=>typeof value==='number')))grid=prediction.grid;
    const valid=Array.isArray(grid)&&grid.length>0&&grid.length<=36&&grid.every(row=>Array.isArray(row)&&row.length===grid[0].length&&row.every(value=>typeof value==='number'&&Number.isFinite(value)))&&grid[0].length>0&&grid[0].length<=36;
    if(!valid){$('result-grid').className='empty';$('result-grid').innerHTML=esc('服务端输出尚无可识别的有限数值网格；保留真实布局和摘要。')+raw({shape,layout},'输出布局');return;}
    const values=grid.flat(),min=Math.min(...values),max=Math.max(...values),span=max-min;
    $('result-grid').className='grid';$('result-grid').style.gridTemplateColumns='repeat('+grid[0].length+',minmax(0,1fr))';
    $('result-grid').innerHTML=grid.map((row,y)=>row.map((value,x)=>{
      const ratio=span>0?(value-min)/span:.5;const light=97-ratio*47;
      return '<div class="cell" style="background:hsl(225 78% '+light+'%);color:'+(ratio>.62?'#fff':'#23345e')+'" title="行 '+y+'，列 '+x+' · '+esc(value)+'">'+esc(display(value))+'</div>';
    }).join('')).join('');
    const units=prediction.units??prediction.unit;const unit=Array.isArray(units)?units[channel]:units;
    $('grid-note').textContent=prediction._name+' · '+display(layout)+' · '+display(shape)+' · 单位 '+display(unit)+' · 归一化 '+display(prediction.normalization)+'（原始服务端数值）';
    $('grid-legend').hidden=false;$('grid-legend').innerHTML=esc(display(min))+'<span class="gradient"></span>'+esc(display(max))+' · 色阶按当前网格范围';
  }
  function frameTime(frame) {
    if(!Number.isInteger(frame)||frame<0||frame>=1488)return '时间未返回';
    const day=Math.floor(frame/48)+1,hour=Math.floor((frame%48)/2),minute=frame%2*30;
    return '2024-01-'+String(day).padStart(2,'0')+' '+String(hour).padStart(2,'0')+':'+String(minute).padStart(2,'0');
  }
  function makeRequest() {
    const name=$('app-name').value.trim();if(!name)throw new Error('请填写应用名称。');
    const match=/^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::00)?$/.exec($('target-time').value);
    if(!match)throw new Error('请输入有效的目标日期与时间。');
    const [year,month,day,hour,minute]=match.slice(1).map(Number);
    const calendar=new Date(Date.UTC(year,month-1,day,hour,minute));
    if(year!==2024||month!==1||day<1||day>31||hour>23||![0,30].includes(minute)||calendar.getUTCDate()!==day)throw new Error('请选择 2024 年 1 月内的整点或半点时间。');
    const number=(id,label,minimum,integer=false)=>{
      const text=$(id).value.trim(),value=Number(text);
      if(!text||!Number.isFinite(value)||value<minimum||(integer&&!Number.isInteger(value)))throw new Error(label+'须为不小于 '+minimum+' 的'+(integer?'整数。':'数值。'));
      return value;
    };
    const count=number('batch-size','独立样本数',1,true);if(count>32)throw new Error('每次最多 32 个独立样本。');
    const first=(day-1)*48+hour*2+minute/30;
    const target_frames=Array.from({length:count},(_,index)=>first+index);
    if(target_frames.some(frame=>frame>=1488))throw new Error('目标批次超出 1 月数据范围，请缩短样本数。');
    const missing=target_frames.find(frame=>frame%48<3);
    if(missing!==undefined)throw new Error(frameTime(missing)+' 缺少同日 90 分钟历史。每天从 01:30 起可预测，请调整跨午夜批次。');
    const template=$('template-select').value;if(!['flow','analysis','od_hotspots'].includes(template))throw new Error('请选择有效的业务模板。');
    return {name,request:{template,target_frames,deadline_seconds:number('deadline-seconds','截止时限',.1),priority:number('priority','优先级',0,true),enable_reuse:$('enable-reuse').checked,enable_recovery:$('enable-recovery').checked}};
  }
  function renderFramePreview() {
    try {const request=makeRequest().request,frames=request.target_frames;
      $('frame-preview').textContent='目标帧 '+frames[0]+'–'+frames.at(-1)+' · '+frames.length+' 个独立一步样本\n'+frameTime(frames[0])+' → '+frameTime(frames.at(-1))+'（目标起点）';
      $('frame-preview').style.whiteSpace='pre-line';
    } catch(error){$('frame-preview').textContent=errorText(error);}
  }
  function switchTab(name) {
    document.querySelectorAll('[data-tab]').forEach(tab=>{const selected=tab.dataset.tab===name;tab.setAttribute('aria-selected',String(selected));tab.tabIndex=selected?0:-1;});
    document.querySelectorAll('[data-panel]').forEach(panel=>panel.hidden=panel.dataset.panel!==name);
  }
  async function chooseApp(id,view=false) {
    state.selectedApp=id;const target=appWorkflow(selectedApp()??{});
    if(target!==state.workflowId){state.workflowId=target;state.lastWorkflow=null;await loadWorkflow();}
    render();if(view&&target)switchTab('orchestration');
  }
  async function mutate(action,id) {
    if(state.mutating)return;
    const app=apps().find(item=>appId(item)===id);
    if(action==='run'&&active(app)){state.errors.action='应用已运行，等待当前工作流终态。';renderErrors();return;}
    let payload;
    try {if(action==='create')payload=makeRequest();else if(!id)throw new Error('请先选择应用。');}
    catch(error){state.errors.action=errorText(error);renderErrors();return;}
    state.mutating=true;state.action=action;delete state.errors.action;$('action-message').hidden=true;renderControls();
    try {
      // Finish an in-flight read before POST; do not let a stale poll undo selection.
      if(state.refreshing)await state.refreshing;
      const result=await api(action==='create'?'/apps':'/apps/'+encodeURIComponent(id)+'/'+action,{method:'POST',...(payload?{body:JSON.stringify(payload)}:{})});
      const item=object(body(result,'app'));
      state.selectedApp=action==='create'?String(item.app_id??item.id??result.app_id??result.id??state.selectedApp):id;
      if(action==='create'){state.workflowId='';state.lastWorkflow=null;}
      const workflowId=String(item.workflow_id??result.workflow_id??item.latest_workflow_id??'');
      if(workflowId){state.workflowId=workflowId;state.lastWorkflow=null;}
      $('action-message').textContent=action==='create'?'应用已创建。选择应用后点击「运行当前应用」。':action==='run'?'服务端已接受运行请求；下方状态来自实际工作流。':'服务端已接受停止请求；请查看应用状态和资源释放账本。';
      $('action-message').hidden=false;if(action==='run'&&workflowId)switchTab('orchestration');
    } catch(error){state.errors.action=errorText(error);}
    finally {await refresh();state.mutating=false;state.action='';renderControls();renderErrors();}
  }
  function render() {renderOverview();renderTemplates();renderApps();renderCatalog();renderOrchestration();renderResources();renderResults();renderControls();renderErrors();}
  document.querySelectorAll('[data-tab]').forEach(tab=>{
    tab.addEventListener('click',()=>switchTab(tab.dataset.tab));
    tab.addEventListener('keydown',event=>{if(!['ArrowLeft','ArrowRight','Home','End'].includes(event.key))return;event.preventDefault();const tabs=[...document.querySelectorAll('[data-tab]')],index=tabs.indexOf(tab),next=event.key==='Home'?0:event.key==='End'?tabs.length-1:(index+(event.key==='ArrowRight'?1:-1)+tabs.length)%tabs.length;switchTab(tabs[next].dataset.tab);tabs[next].focus();});
  });
  $('create-form').addEventListener('submit',event=>{event.preventDefault();mutate('create','');});
  ['target-time','batch-size','app-name','deadline-seconds','priority'].forEach(id=>$(id).addEventListener('input',renderFramePreview));
  $('template-select').addEventListener('change',()=>{renderTemplates();renderFramePreview();});
  $('app-list').addEventListener('click',event=>{const button=event.target.closest('[data-action]');if(!button||button.disabled)return;if(button.dataset.action==='view')chooseApp(button.dataset.app,true);else mutate(button.dataset.action,button.dataset.app);});
  $('app-select').addEventListener('change',()=>chooseApp($('app-select').value));
  $('workflow-select').addEventListener('change',async()=>{state.workflowId=$('workflow-select').value;state.lastWorkflow=null;await loadWorkflow();render();});
  $('run-btn').addEventListener('click',()=>mutate('run',state.selectedApp));
  $('stop-btn').addEventListener('click',()=>mutate('stop',state.selectedApp));
  $('refresh-btn').addEventListener('click',()=>refresh());
  $('show-orchestration').addEventListener('click',()=>switchTab('orchestration'));
  ['result-time','result-task','result-channel'].forEach(id=>$(id).addEventListener('change',renderResults));
  document.addEventListener('visibilitychange',()=>{if(!document.hidden&&!state.mutating)refresh();});
  let pollTimer;
  async function poll() {
    if(!document.hidden&&!state.mutating)await refresh();
    pollTimer=setTimeout(poll,5000);
  }
  window.addEventListener('pagehide',()=>clearTimeout(pollTimer));
  renderFramePreview();render();poll();
})();
</script>
</body>
</html>'''
