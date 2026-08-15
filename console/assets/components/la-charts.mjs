// Telemetry view: a one-line cluster summary plus a single held-locks chart
// fed by /metrics/series (merged with the IndexedDB history cache).

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
          <div class="chart-card"><div class="kicker">gauge</div><div class="title">Held locks</div><div class="plot" data-plot="held"></div></div>
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
    const { cluster, series } = store.state;

    if (cluster) {
      const held = (cluster.nodes ?? []).reduce((n, x) => n + x.locksHeld, 0);
      const acquirePerSec = (cluster.nodes ?? []).reduce((n, x) => n + x.acquirePerSec, 0);
      this.querySelector(".cluster-summary").innerHTML =
        `<span>leader <b>${esc(cluster.leader)}</b></span>` +
        `<span>era <b>${cluster.era}</b></span>` +
        `<span>view <b>${cluster.view}</b></span>` +
        `<span>held <b>${held}</b></span>` +
        `<span>acquire/s <b>${acquirePerSec.toFixed(2)}</b></span>`;
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

    if (series?.buckets?.length) {
      const labels = series.buckets.map((b) => fmtClock(b.tsMs));
      const tick = Math.max(1, Math.ceil(labels.length / 8));
      const xAxis = { type: "category", data: labels, ...AXIS, axisLabel: { ...AXIS.axisLabel, interval: tick } };

      this._chart("held")?.setOption({
        animation: false,
        grid: { left: 36, right: 14, top: 16, bottom: 24 },
        tooltip: { ...TOOLTIP, formatter: (p) => `${p[0].axisValue}<br/>held: ${p[0].value}` },
        xAxis,
        yAxis: { type: "value", minInterval: 1, ...AXIS },
        series: [{
          name: "held", type: "line", smooth: true, symbol: "none",
          data: series.buckets.map((b) => b.held),
          lineStyle: { color: "#7b74b8", width: 2 },
          areaStyle: { color: "rgba(123,116,184,0.18)" },
        }],
      }, { notMerge: true });
    }
  }
}

customElements.define("la-charts", LaCharts);
