// Telemetry view: cluster summary (from mock) plus journal rate charts fed by
// store.journalRates.buckets (acquire/renew/release per second), and the
// protocol charts fed by the /api/v1/telemetry/log series (the observability
// contract's JSON log lines: the heartbeat cadence, the timeout events, and
// the slot frontiers), with png/svg paper exports.

import { store } from "../lib/state.mjs";
import { api } from "../lib/api.mjs";
import { esc, fmtClock } from "../lib/util.mjs";

const AXIS = {
  axisLabel: { color: "#9397ab", fontFamily: "JetBrains Mono, monospace", fontSize: 10 },
  axisLine: { lineStyle: { color: "rgba(233,233,237,0.15)" } },
  splitLine: { lineStyle: { color: "rgba(233,233,237,0.07)" } },
};
const TOOLTIP = {
  trigger: "axis",
  backgroundColor: "#232532",
  borderColor: "rgba(233,233,237,0.16)",
  textStyle: { color: "#e9e9ed", fontFamily: "JetBrains Mono, monospace", fontSize: 11 },
};

// The charts the log series honestly supports, and the event names each is
// built from (the named events the series actually carries):
// - "arrivals": the commit-in heartbeat arrival intervals, per node, 1 s
//   candlestick buckets (a stall is a fat candle; a failover is a gap).
// - "failover": the leader-timeout-detect silences and the
//   election-wait-fire waits over time.
// - "frontier": the per-node slot progression from commit-in and
//   heartbeat-commit slots (the drift-window view).
const LOG_PLOTS = ["arrivals", "failover", "frontier"];

class LaCharts extends HTMLElement {
  connectedCallback() {
    this.innerHTML = `
      <div style="flex:1;display:flex;flex-direction:column;min-height:0">
        <div class="cluster-summary"></div>
        <div class="charts">
          <div class="chart-card"><div class="kicker">journal rates</div><div class="title">Acquire / Renew / Release per second</div><div class="plot" data-plot="rates"></div></div>
          <div class="chart-card">
            <div style="display:flex;align-items:baseline;gap:8px">
              <div style="flex:1"><div class="kicker">heartbeat arrivals</div><div class="title">commit-in interval (ms), 1 s buckets, per node</div></div>
              <button class="btn btn-secondary" style="min-height:0;padding:1px 8px;font-size:11px" data-log-reload>reload series</button>
              <button class="btn btn-secondary" style="min-height:0;padding:1px 8px;font-size:11px" data-export="arrivals:png">png</button>
              <button class="btn btn-secondary" style="min-height:0;padding:1px 8px;font-size:11px" data-export="arrivals:svg">svg</button>
            </div>
            <div class="plot" data-plot="arrivals"></div>
          </div>
          <div class="chart-card">
            <div style="display:flex;align-items:baseline;gap:8px">
              <div style="flex:1"><div class="kicker">failover timing</div><div class="title">detected silence and armed election wait (ms)</div></div>
              <button class="btn btn-secondary" style="min-height:0;padding:1px 8px;font-size:11px" data-export="failover:png">png</button>
              <button class="btn btn-secondary" style="min-height:0;padding:1px 8px;font-size:11px" data-export="failover:svg">svg</button>
            </div>
            <div class="plot" data-plot="failover"></div>
          </div>
          <div class="chart-card">
            <div style="display:flex;align-items:baseline;gap:8px">
              <div style="flex:1"><div class="kicker">slot frontier</div><div class="title">applied slot over time, per node</div></div>
              <button class="btn btn-secondary" style="min-height:0;padding:1px 8px;font-size:11px" data-export="frontier:png">png</button>
              <button class="btn btn-secondary" style="min-height:0;padding:1px 8px;font-size:11px" data-export="frontier:svg">svg</button>
            </div>
            <div class="plot" data-plot="frontier"></div>
          </div>
        </div>
      </div>`;
    this._charts = {};
    this._logOptions = {};
    this._logEpoch = null;
    this._unsub = store.subscribe(() => this.update());
    this._ro = new ResizeObserver(() => {
      for (const c of Object.values(this._charts)) c.resize();
    });
    for (const el of this.querySelectorAll(".plot")) this._ro.observe(el);
    this.addEventListener("click", (e) => this._onClick(e));
    this.update();
    this._loadLog();
  }
  disconnectedCallback() {
    this._unsub?.();
    this._ro?.disconnect();
    for (const c of Object.values(this._charts ?? {})) c.dispose();
  }

  _chart(key) {
    if (!window.echarts) return null;
    if (!this._charts[key]) {
      const el = this.querySelector(`[data-plot=${key}]`);
      if (!el) return null;
      this._charts[key] = window.echarts.init(el, null, { renderer: "canvas" });
    }
    return this._charts[key];
  }

  _onClick(e) {
    if (e.target.closest("[data-log-reload]")) {
      this._loadLog(true);
      return;
    }
    const exp = e.target.closest("[data-export]");
    if (exp) {
      const [key, kind] = exp.dataset.export.split(":");
      this._export(key, kind);
    }
  }

  // The log series fetch: once per panel mount, forceable with the reload
  // affordance. A failure names itself in the plots' fallbacks.
  async _loadLog(force) {
    try {
      const body = await api.telemetryLog({});
      this._logEpoch = body.span?.first_ms ?? null;
      this._renderLog(body.lines ?? []);
    } catch (err) {
      this._showLogError(String(err?.message ?? err));
    }
  }

  _showLogError(message) {
    for (const key of LOG_PLOTS) {
      const el = this.querySelector(`[data-plot=${key}]`);
      if (el) el.innerHTML = `<div class="chart-fallback">log series unavailable — ${esc(message)}</div>`;
    }
  }

  _renderLog(lines) {
    if (!window.echarts) return;

    // arrivals: per-node inter-arrival deltas of the commit-in events,
    // bucketed per second (open = first delta, close = last, low = min,
    // high = max).
    const perNode = new Map();
    for (const line of lines) {
      if (line.event !== "commit-in") continue;
      const node = String(line.node);
      const prev = perNode.get(node);
      if (!prev) {
        perNode.set(node, { lastTs: line.ts, deltas: [] });
      } else {
        const delta = line.ts - prev.lastTs;
        if (delta >= 0) prev.deltas.push([line.ts, delta]);
        prev.lastTs = line.ts;
      }
    }
    const buckets = new Map();
    for (const { deltas } of perNode.values()) {
      for (const [ts, delta] of deltas) {
        const k = Math.floor(ts / 1000) * 1000;
        const arr = buckets.get(k);
        if (arr) arr.push(delta); else buckets.set(k, [delta]);
      }
    }
    const keys = [...buckets.keys()].sort((a, b) => a - b);
    const labels = keys.map((k) => fmtClock(k));
    const tick = Math.max(1, Math.ceil(labels.length / 8));
    const candles = keys.map((k) => {
      const v = buckets.get(k);
      return [v[0], v[v.length - 1], Math.min(...v), Math.max(...v)];
    });
    const arrivalsOption = {
      animation: false,
      grid: { left: 44, right: 14, top: 16, bottom: 24 },
      tooltip: { ...TOOLTIP },
      xAxis: { type: "category", data: labels, ...AXIS, axisLabel: { ...AXIS.axisLabel, interval: tick } },
      yAxis: { type: "value", name: "dt ms", nameTextStyle: { color: "#9397ab", fontSize: 10 }, ...AXIS },
      series: [
        {
          type: "candlestick", data: candles,
          itemStyle: { color: "#5ba08f", color0: "#c47a6c", borderColor: "#5ba08f", borderColor0: "#c47a6c" },
        },
      ],
      ...(keys.length > 240 ? { dataZoom: [{ type: "inside" }] } : {}),
    };

    // failover: the measured silence at each leader-timeout-detect and the
    // armed wait at each election-wait-fire, over time.
    const detects = lines.filter((l) => l.event === "leader-timeout-detect" && l.ts != null && l.silence_ms != null);
    const fires = lines.filter((l) => l.event === "election-wait-fire" && l.ts != null && l.wait_ms != null);
    const failoverOption = {
      animation: false,
      grid: { left: 44, right: 14, top: 16, bottom: 24 },
      tooltip: {
        ...TOOLTIP,
        formatter: (params) => {
          const rows = (Array.isArray(params) ? params : [params]).map((p) => {
            const l = p.data.source;
            return `${p.marker} ${esc(p.seriesName)} node ${esc(String(l?.node ?? "—"))} · era ${esc(String(l?.era ?? "—"))} · view ${esc(String(l?.view ?? "—"))} · ${esc(fmtClock(p.value[0]))} · ${p.value[1]} ms`;
          });
          return rows.join("<br>");
        },
      },
      xAxis: { type: "time", ...AXIS, axisLabel: { ...AXIS.axisLabel, formatter: (v) => fmtClock(v) } },
      yAxis: { type: "value", name: "ms", nameTextStyle: { color: "#9397ab", fontSize: 10 }, ...AXIS },
      series: [
        {
          name: "detect silence", type: "scatter", symbolSize: 7,
          data: detects.map((l) => ({ value: [l.ts, l.silence_ms], source: l })),
          itemStyle: { color: "#c47a6c", opacity: 0.85 },
        },
        {
          name: "election wait", type: "scatter", symbolSize: 7,
          data: fires.map((l) => ({ value: [l.ts, l.wait_ms], source: l })),
          itemStyle: { color: "#7b74b8", opacity: 0.85 },
        },
      ],
    };

    // frontier: the applied slot each node's commit-in / heartbeat-commit
    // lines carry, as per-node step lines (the drift window at a glance).
    const frontierByNode = new Map();
    for (const line of lines) {
      if ((line.event !== "commit-in" && line.event !== "heartbeat-commit") || line.ts == null) continue;
      const node = String(line.node);
      if (!frontierByNode.has(node)) frontierByNode.set(node, []);
      frontierByNode.get(node).push([line.ts, line.slot]);
    }
    const frontierOption = {
      animation: false,
      grid: { left: 52, right: 14, top: 16, bottom: 24 },
      tooltip: { ...TOOLTIP },
      legend: { show: true, top: 0, textStyle: { color: "#9397ab", fontSize: 10 } },
      xAxis: { type: "time", ...AXIS, axisLabel: { ...AXIS.axisLabel, formatter: (v) => fmtClock(v) } },
      yAxis: { type: "value", name: "slot", nameTextStyle: { color: "#9397ab", fontSize: 10 }, ...AXIS },
      series: [...frontierByNode.entries()].map(([node, data]) => ({
        name: "node " + node, type: "line", step: "end", symbol: "none",
        data,
        lineStyle: { width: 2 },
      })),
    };

    this._logOptions = { arrivals: arrivalsOption, failover: failoverOption, frontier: frontierOption };
    this._chart("arrivals")?.setOption(arrivalsOption, { notMerge: true });
    this._chart("failover")?.setOption(failoverOption, { notMerge: true });
    this._chart("frontier")?.setOption(frontierOption, { notMerge: true });
  }

  _export(key, kind) {
    const opt = this._logOptions[key];
    if (!opt || !window.echarts) return;
    const name = "lunet-log-" + key + "." + (this._logEpoch ?? "unknown") + "." + kind;
    let href;
    let revoke = false;
    if (kind === "png") {
      const chart = this._charts[key];
      if (!chart) return;
      href = chart.getDataURL({ type: "png", pixelRatio: 2, backgroundColor: "#161826" });
    } else {
      const host = document.createElement("div");
      host.style.cssText = "position:absolute;left:-99999px;top:0;width:800px;height:400px";
      document.body.appendChild(host);
      const svg = window.echarts.init(host, null, { renderer: "svg" });
      svg.setOption(opt);
      const blob = new Blob([svg.renderToSVGString()], { type: "image/svg+xml" });
      href = URL.createObjectURL(blob);
      revoke = true;
      svg.dispose();
      host.remove();
    }
    const a = document.createElement("a");
    a.href = href;
    a.download = name;
    a.click();
    if (revoke) setTimeout(() => URL.revokeObjectURL(href), 1000);
  }

  update() {
    const { cluster, journalRates, journalLocks } = store.state;

    // Cluster header still comes from the mock source.
    if (cluster) {
      const held = journalLocks.length;
      this.querySelector(".cluster-summary").innerHTML =
        `<span>leader <b>${esc(cluster.leader)}</b></span>` +
        `<span>era <b>${cluster.era}</b></span>` +
        `<span>view <b>${cluster.view}</b></span>` +
        `<span>held <b>${held}</b></span>`;
    }

    if (!window.echarts) {
      for (const el of this.querySelectorAll(".plot")) {
        if (!el.dataset.fb) {
          el.dataset.fb = "1";
          el.innerHTML = '<div class="chart-fallback">echarts failed to load — check that ./vendor/echarts.min.js serves</div>';
        }
      }
      return;
    }

    const buckets = journalRates?.buckets ?? [];
    if (buckets.length) {
      const labels = buckets.map((b) => fmtClock(b.tsSec * 1000));
      const tick = Math.max(1, Math.ceil(labels.length / 8));
      const xAxis = { type: "category", data: labels, ...AXIS, axisLabel: { ...AXIS.axisLabel, interval: tick } };

      this._chart("rates")?.setOption({
        animation: false,
        grid: { left: 36, right: 14, top: 16, bottom: 24 },
        tooltip: { ...TOOLTIP },
        legend: { show: true, top: 0, textStyle: { color: "#9397ab", fontSize: 10 } },
        xAxis,
        yAxis: { type: "value", minInterval: 1, ...AXIS },
        series: [
          {
            name: "acquire", type: "line", smooth: true, symbol: "none",
            data: buckets.map((b) => b.acquire),
            lineStyle: { color: "#7b74b8", width: 2 },
            areaStyle: { color: "rgba(123,116,184,0.18)" },
          },
          {
            name: "renew", type: "line", smooth: true, symbol: "none",
            data: buckets.map((b) => b.renew),
            lineStyle: { color: "#5ba08f", width: 2 },
            areaStyle: { color: "rgba(91,160,143,0.12)" },
          },
          {
            name: "release", type: "line", smooth: true, symbol: "none",
            data: buckets.map((b) => b.release),
            lineStyle: { color: "#c47a6c", width: 2 },
            areaStyle: { color: "rgba(196,122,108,0.12)" },
          },
        ],
      }, { notMerge: true });
    }
  }
}

customElements.define("la-charts", LaCharts);
