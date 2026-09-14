// Telemetry view: cluster summary (from mock) plus journal rate charts fed by
// store.journalRates.buckets (acquire/renew/release per second), and the phi
// scatter + dt candlestick charts fed by the bulk /api/v1/telemetry/phi trace
// (sessionStorage-cached per trace span, with a reload-trace affordance).

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

// ---- bulk phi trace cache ----------------------------------------------------
// The trace body is large, so it is fetched once per browser session and kept
// lossless (raw text) in sessionStorage keyed by the trace span. Oversized
// bodies degrade to an in-memory cache for the session.

const PHI_KEY_PREFIX = "lunet.phi.";
const PHI_SPAN_KEY = "lunet.phi.span";
const PHI_MAX_BYTES = 4 * 1024 * 1024;
let phiMemCache = null;

function readPhiCache() {
  try {
    const span = JSON.parse(sessionStorage.getItem(PHI_SPAN_KEY) ?? "null");
    if (!span || span.first_ms == null) return null;
    const text = phiMemCache ?? sessionStorage.getItem(PHI_KEY_PREFIX + span.first_ms);
    if (text == null) return null;
    const body = JSON.parse(text);
    if (body.span?.first_ms !== span.first_ms || body.span?.last_ms !== span.last_ms) return null;
    return text;
  } catch {
    return null;
  }
}

function writePhiCache(text) {
  let span;
  try {
    span = JSON.parse(text).span;
  } catch {
    return;
  }
  if (!span || span.first_ms == null) return;
  try {
    const prev = JSON.parse(sessionStorage.getItem(PHI_SPAN_KEY) ?? "null");
    if (prev?.first_ms != null && prev.first_ms !== span.first_ms) {
      sessionStorage.removeItem(PHI_KEY_PREFIX + prev.first_ms);
    }
    if (text.length > PHI_MAX_BYTES) {
      phiMemCache = text;
    } else {
      phiMemCache = null;
      sessionStorage.setItem(PHI_KEY_PREFIX + span.first_ms, text);
    }
    sessionStorage.setItem(PHI_SPAN_KEY, JSON.stringify({ first_ms: span.first_ms, last_ms: span.last_ms }));
  } catch {
    phiMemCache = text;
  }
}

function clearPhiCache() {
  try {
    const span = JSON.parse(sessionStorage.getItem(PHI_SPAN_KEY) ?? "null");
    if (span?.first_ms != null) sessionStorage.removeItem(PHI_KEY_PREFIX + span.first_ms);
    sessionStorage.removeItem(PHI_SPAN_KEY);
  } catch {}
  phiMemCache = null;
}

class LaCharts extends HTMLElement {
  connectedCallback() {
    this.innerHTML = `
      <div style="flex:1;display:flex;flex-direction:column;min-height:0">
        <div class="cluster-summary"></div>
        <div class="charts">
          <div class="chart-card"><div class="kicker">journal rates</div><div class="title">Acquire / Renew / Release per second</div><div class="plot" data-plot="rates"></div></div>
          <div class="chart-card">
            <div style="display:flex;align-items:baseline;gap:8px">
              <div style="flex:1"><div class="kicker">phi detector</div><div class="title">φ estimate at heartbeat arrivals</div></div>
              <button class="btn btn-secondary" style="min-height:0;padding:1px 8px;font-size:11px" data-phi-reload>reload trace</button>
              <button class="btn btn-secondary" style="min-height:0;padding:1px 8px;font-size:11px" data-export="phi:png">png</button>
              <button class="btn btn-secondary" style="min-height:0;padding:1px 8px;font-size:11px" data-export="phi:svg">svg</button>
            </div>
            <div class="plot" data-plot="phi"></div>
          </div>
          <div class="chart-card">
            <div style="display:flex;align-items:baseline;gap:8px">
              <div style="flex:1"><div class="kicker">heartbeat interval</div><div class="title">heartbeat interval (ms), 1 s buckets</div></div>
              <button class="btn btn-secondary" style="min-height:0;padding:1px 8px;font-size:11px" data-export="dt:png">png</button>
              <button class="btn btn-secondary" style="min-height:0;padding:1px 8px;font-size:11px" data-export="dt:svg">svg</button>
            </div>
            <div class="plot" data-plot="dt"></div>
          </div>
        </div>
      </div>`;
    this._charts = {};
    this._phiOptions = {};
    this._phiEpoch = null;
    this._unsub = store.subscribe(() => this.update());
    this._ro = new ResizeObserver(() => {
      for (const c of Object.values(this._charts)) c.resize();
    });
    for (const el of this.querySelectorAll(".plot")) this._ro.observe(el);
    this.addEventListener("click", (e) => this._onClick(e));
    this.update();
    this._loadPhi();
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
    if (e.target.closest("[data-phi-reload]")) {
      this._loadPhi(true);
      return;
    }
    const exp = e.target.closest("[data-export]");
    if (exp) {
      const [key, kind] = exp.dataset.export.split(":");
      this._export(key, kind);
    }
  }

  async _loadPhi(force) {
    if (force) clearPhiCache();
    let text = force ? null : readPhiCache();
    if (text == null) {
      try {
        text = await api.phiRaw();
        writePhiCache(text);
      } catch (err) {
        this._showPhiError(String(err?.message ?? err));
        return;
      }
    }
    let body;
    try {
      body = JSON.parse(text);
    } catch {
      this._showPhiError("phi trace is not valid JSON");
      return;
    }
    this._phiEpoch = body.span?.first_ms ?? null;
    this._renderPhi(body);
  }

  _showPhiError(message) {
    for (const key of ["phi", "dt"]) {
      const el = this.querySelector(`[data-plot=${key}]`);
      if (el) el.innerHTML = `<div class="chart-fallback">phi trace unavailable — ${esc(message)}</div>`;
    }
  }

  _renderPhi(body) {
    if (!window.echarts) return;
    const samples = body.samples ?? [];
    const points = samples.map((s) => [s[0], s[4]]);
    const marks = (body.decisions ?? [])
      .filter((d) => d && d._ts_ms != null)
      .map((d) => ({ coord: [d._ts_ms, d.phi] }));

    const phiOption = {
      animation: false,
      grid: { left: 44, right: 14, top: 16, bottom: 24 },
      tooltip: { ...TOOLTIP },
      xAxis: { type: "time", ...AXIS, axisLabel: { ...AXIS.axisLabel, formatter: (v) => fmtClock(v) } },
      yAxis: { type: "value", name: "phi", nameTextStyle: { color: "#9397ab", fontSize: 10 }, ...AXIS },
      series: [
        {
          type: "scatter", symbolSize: 3,
          data: points,
          itemStyle: { color: "#7b74b8", opacity: 0.75 },
          markLine: {
            silent: true, symbol: "none",
            lineStyle: { color: "#c47a6c", type: "dashed", width: 1 },
            label: { color: "#9397ab", fontSize: 10, formatter: "threshold (default)" },
            data: [{ yAxis: 1.0 }],
          },
          ...(marks.length ? { markPoint: { symbolSize: 8, itemStyle: { color: "#c47a6c" }, data: marks } } : {}),
        },
      ],
    };

    const buckets = new Map();
    for (const s of samples) {
      const k = Math.floor(s[0] / 1000) * 1000;
      const arr = buckets.get(k);
      if (arr) arr.push(s[1]); else buckets.set(k, [s[1]]);
    }
    const keys = [...buckets.keys()].sort((a, b) => a - b);
    const labels = keys.map((k) => fmtClock(k));
    const tick = Math.max(1, Math.ceil(labels.length / 8));
    const candles = keys.map((k) => {
      const v = buckets.get(k);
      return [v[0], v[v.length - 1], Math.min(...v), Math.max(...v)];
    });
    const dtOption = {
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

    this._phiOptions = { phi: phiOption, dt: dtOption };
    this._chart("phi")?.setOption(phiOption, { notMerge: true });
    this._chart("dt")?.setOption(dtOption, { notMerge: true });
  }

  _export(key, kind) {
    const opt = this._phiOptions[key];
    if (!opt || !window.echarts) return;
    const name = (key === "phi" ? "lunet-phi-scatter" : "lunet-dt-candles") +
      "." + (this._phiEpoch ?? "unknown") + "." + kind;
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
