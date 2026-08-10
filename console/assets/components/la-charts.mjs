// Telemetry view: per-node counter cards plus three echarts plots fed by
// /metrics/series (merged with the IndexedDB history cache).

import { store } from "../lib/state.mjs";
import { esc, fmtClock } from "../lib/util.mjs";

const KIND_COLORS = {
  acquire: "#9184d9",
  renew: "#75798c",
  release: "#a7a1db",
  cas: "#b5abfc",
  expire: "#e0b46a",
  break: "#c96f7e",
  deny: "#8a6f3c",
};

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
      <div style="flex:1;overflow:auto;min-height:0">
        <div class="node-cards"></div>
        <div class="charts">
          <div class="chart-card"><div class="kicker">gauge</div><div class="title">Held locks</div><div class="plot" data-plot="held"></div></div>
          <div class="chart-card"><div class="kicker">events</div><div class="title">Taken / renewed / released / CAS</div><div class="plot" data-plot="kinds"></div></div>
          <div class="chart-card wide"><div class="kicker">nodes</div><div class="title">Per-node lock rates (per second)</div><div class="plot" data-plot="nodes"></div></div>
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

    this.querySelector(".node-cards").innerHTML = (cluster?.nodes ?? []).map((n) => `
      <div class="node-card">
        <div class="nid">${esc(n.id)} <span class="role">${esc(n.role)}</span></div>
        <div class="m"><span>held</span><b>${n.locksHeld}</b></div>
        <div class="m"><span>acquire/s</span><b>${n.acquirePerSec}</b></div>
        <div class="m"><span>renew/s</span><b>${n.renewPerSec}</b></div>
        <div class="m"><span>cas/s</span><b>${n.casPerSec}</b></div>
        <div class="m"><span>applied</span><b>${n.appliedIndex}</b></div>
      </div>`).join("");

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
        grid: { left: 36, right: 14, top: 16, bottom: 24 },
        tooltip: TOOLTIP,
        xAxis,
        yAxis: { type: "value", minInterval: 1, ...AXIS },
        series: [{
          name: "held", type: "line", smooth: true, symbol: "none",
          data: series.buckets.map((b) => b.held),
          lineStyle: { color: "#9184d9", width: 2 },
          areaStyle: { color: "rgba(145,132,217,0.18)" },
        }],
      }, { notMerge: true });

      const kinds = ["acquire", "renew", "release", "cas", "expire", "break"];
      this._chart("kinds")?.setOption({
        grid: { left: 36, right: 14, top: 30, bottom: 24 },
        tooltip: TOOLTIP,
        legend: { top: 0, textStyle: { color: "#9397ab", fontSize: 10 }, itemWidth: 10, itemHeight: 8 },
        xAxis,
        yAxis: { type: "value", minInterval: 1, ...AXIS },
        series: kinds.map((k) => ({
          name: k, type: "bar", stack: "kinds", barMaxWidth: 14,
          itemStyle: { color: KIND_COLORS[k] },
          data: series.buckets.map((b) => b[k]),
        })),
      }, { notMerge: true });
    }

    if (cluster?.nodes?.length) {
      const rates = ["acquirePerSec", "renewPerSec", "releasePerSec", "casPerSec"];
      const colors = ["#9184d9", "#75798c", "#a7a1db", "#b5abfc"];
      this._chart("nodes")?.setOption({
        grid: { left: 40, right: 14, top: 30, bottom: 24 },
        tooltip: TOOLTIP,
        legend: { top: 0, textStyle: { color: "#9397ab", fontSize: 10 }, itemWidth: 10, itemHeight: 8 },
        xAxis: { type: "category", data: cluster.nodes.map((n) => n.id), ...AXIS },
        yAxis: { type: "value", ...AXIS },
        series: rates.map((r, i) => ({
          name: r.replace("PerSec", "/s"), type: "bar", barMaxWidth: 22,
          itemStyle: { color: colors[i] },
          data: cluster.nodes.map((n) => n[r]),
        })),
      }, { notMerge: true });
    }
  }
}

customElements.define("la-charts", LaCharts);
