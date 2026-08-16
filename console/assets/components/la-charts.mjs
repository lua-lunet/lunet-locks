// Telemetry view: cluster summary (from mock) plus journal rate charts fed by
// store.journalRates.buckets (acquire/renew/release per second).

import { store } from "../lib/state.mjs";
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

class LaCharts extends HTMLElement {
  connectedCallback() {
    this.innerHTML = `
      <div style="flex:1;display:flex;flex-direction:column;min-height:0">
        <div class="cluster-summary"></div>
        <div class="charts">
          <div class="chart-card"><div class="kicker">journal rates</div><div class="title">Acquire / Renew / Release per second</div><div class="plot" data-plot="rates"></div></div>
        </div>
      </div>`;
    this._charts = {};
    this._unsub = store.subscribe(() => this.update());
    this._ro = new ResizeObserver(() => {
      for (const c of Object.values(this._charts)) c.resize();
    });
    for (const el of this.querySelectorAll(".plot")) this._ro.observe(el);
    this.update();
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
          el.innerHTML = '<div class="chart-fallback">echarts CDN unavailable — check network access to cdn.jsdelivr.net</div>';
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
