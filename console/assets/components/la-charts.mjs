// Telemetry view: a one-line cluster summary plus a single held-locks chart
// fed by /metrics/series (merged with the IndexedDB history cache).

import { store } from "../lib/state.mjs";
import { esc, fmtClock } from "../lib/util.mjs";

/** @typedef {import("../lib/types.mjs").Bucket} Bucket */
/** @typedef {import("../lib/types.mjs").ClusterNode} ClusterNode */
/** @typedef {import("../lib/types.mjs").NodeMetric} NodeMetric */

// echarts arrives from a CDN <script> in index.html and ships no types here
// (there is no node_modules to resolve @types from), so this is the single
// narrow escape hatch — everything else in this module stays typed.
/** @returns {any} */
const echartsLib = () => /** @type {any} */ (/** @type {any} */ (window).echarts);

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
  /** @type {(() => void) | undefined} */
  _unsub;
  /** @type {ResizeObserver | undefined} */
  _ro;
  /** Live echarts instances, keyed by the plot's data-plot value. @type {Record<string, any>} */
  _charts = {};

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

  /**
   * Lazily create (and memoise) the echarts instance for one plot. Null when
   * the CDN script has not loaded or the plot element is gone.
   * @param {string} key
   * @returns {any}
   */
  _chart(key) {
    const echarts = echartsLib();
    if (!echarts) return null;
    if (!this._charts[key]) {
      const el = this.querySelector(`[data-plot=${key}]`);
      if (!el) return null;
      this._charts[key] = echarts.init(el, null, { renderer: "canvas" });
    }
    return this._charts[key];
  }

  /** @returns {void} */
  update() {
    const { cluster, series } = store.state;

    const summaryEl = this.querySelector(".cluster-summary");
    if (cluster && summaryEl) {
      const nodes = cluster.nodes ?? [];
      /** @param {NodeMetric} k */
      const sum = (k) => nodes.reduce((n, x) => n + (x[k] ?? 0), 0);
      const segBytes = sum("segmentBytes");
      /** @param {number} b */
      const fmtBytes = (b) =>
        b >= 1048576 ? (b / 1048576).toFixed(1) + " MiB" : b >= 1024 ? (b / 1024).toFixed(1) + " KiB" : b + " B";
      const perNode = nodes.map((x) =>
        `<span>${esc(x.id)} <b>${x.locksHeld}</b> held · ${(x.acquirePerSec ?? 0).toFixed(2)}/s acq · ${x.segmentCount ?? 0} seg</span>`
      ).join("");
      summaryEl.innerHTML =
        `<span>nodes <b>${nodes.length}</b></span>` +
        `<span>held <b>${sum("locksHeld")}</b></span>` +
        `<span>acquire/s <b>${sum("acquirePerSec").toFixed(2)}</b></span>` +
        `<span>renew/s <b>${sum("renewPerSec").toFixed(2)}</b></span>` +
        `<span>segments <b>${sum("segmentCount")} (${fmtBytes(segBytes)})</b></span>` +
        perNode;
    }

    if (!echartsLib()) {
      for (const el of this.querySelectorAll(".plot")) {
        if (el instanceof HTMLElement && !el.dataset.fb) {
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
        // The tooltip payload is echarts' own axis-trigger array shape, which
        // this project has no types for.
        tooltip: { ...TOOLTIP, formatter: (/** @type {any[]} */ p) => `${p[0].axisValue}<br/>held: ${p[0].value}` },
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
