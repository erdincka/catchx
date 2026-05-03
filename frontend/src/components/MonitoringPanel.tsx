"use client";

import { useEffect, useRef } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { useCluster } from "@/contexts/ClusterContext";
import { MONITORING_METRICS, type MetricKey } from "@/lib/constants";
import { NexusSectionDivider } from "@/components/nexus-core-components";
import { RiCloseLine, RiBarChartLine } from "@remixicon/react";

function metricLabel(key: MetricKey, short = false): string {
  const label = key.replace(/_/g, " ");
  if (!short) return label.replace(/\b\w/g, (c) => c.toUpperCase());
  return label
    .replace("transactions ", "txn ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

// ── Compact badge strip for header ────────────────────────────────────────────

const TICKER_METRICS: MetricKey[] = [
  "transactions_ingested",
  "transactions_processed",
  "bronze_transactions",
  "silver_transactions",
  "gold_fraud",
];

export function MonitoringTicker() {
  const { metrics } = useCluster();
  return (
    <div className="flex items-center gap-1.5">
      {TICKER_METRICS.map((k, i) => (
        <motion.div
          key={k}
          className="flex items-center gap-1 rounded px-2 py-0.5 metric-tick"
          style={{ background: "rgba(255,255,255,0.07)", animationDelay: `${i * 40}ms` }}
          initial={{ opacity: 0, scale: 0.9 }}
          animate={{ opacity: 1, scale: 1 }}
          transition={{ duration: 0.2, delay: i * 0.04 }}
        >
          <span className="font-sans font-light text-[10px] text-neutrals-medium">
            {metricLabel(k, true)}
          </span>
          <span className="font-sans font-bold text-[10px] font-mono text-brand-vivid">
            {metrics[k]}
          </span>
        </motion.div>
      ))}
    </div>
  );
}

// ── Monitoring side card ───────────────────────────────────────────────────────

export function MonitoringCard({ onOpenCharts }: { onOpenCharts?: () => void }) {
  const { metrics } = useCluster();

  return (
    <div
      className="rounded-lg p-3"
      style={{ background: "#121212", border: "1px solid #474747" }}
    >
      <div className="flex items-center justify-between mb-3">
        <NexusSectionDivider
          // @ts-ignore
          title="Live Metrics"
          style={{ paddingLeft: 0, marginBottom: 0, flex: 1 }}
        />
        {onOpenCharts && (
          <button
            onClick={onOpenCharts}
            title="Open analytics charts"
            className="ml-2 p-1 text-neutrals-medium hover:text-brand-vivid transition-colors duration-200"
          >
            <RiBarChartLine size={14} />
          </button>
        )}
      </div>
      <div className="flex flex-col gap-1">
        {MONITORING_METRICS.map((k) => (
          <div key={k} className="flex justify-between items-center gap-2">
            <span className="font-sans font-light text-[11px] text-neutrals-medium truncate">{metricLabel(k)}</span>
            <motion.span
              key={`${k}-${metrics[k]}`}
              className="font-sans font-semibold text-[11px] font-mono rounded px-1.5 py-0.5 shrink-0"
              style={{ background: "rgba(242,86,29,0.15)", color: metrics[k] > 0 ? "#F2561D" : "#474747" }}
              initial={{ scale: 1.15 }}
              animate={{ scale: 1 }}
              transition={{ duration: 0.2 }}
            >
              {metrics[k]}
            </motion.span>
          </div>
        ))}
      </div>
    </div>
  );
}

// ── ECharts panels ────────────────────────────────────────────────────────────

interface ChartDef { key: string; title: string; }

const CHART_DEFS: ChartDef[] = [
  { key: "consumer", title: "Consumer Lag" },
  { key: "incoming", title: "Incoming Stream" },
  { key: "bronze",   title: "Bronze Tier" },
  { key: "silver",   title: "Silver Tier" },
  { key: "gold",     title: "Gold Tier" },
];

export function MonitoringCharts() {
  const { metrics } = useCluster();
  const chartRefs        = useRef<Record<string, HTMLDivElement | null>>({});
  const echartsInstances = useRef<Record<string, unknown>>({});

  useEffect(() => {
    let disposed = false;
    import("echarts").then((echarts) => {
      if (disposed) return;
      for (const { key, title } of CHART_DEFS) {
        const el = chartRefs.current[key];
        if (!el) continue;
        const existing = echarts.getInstanceByDom(el);
        const chart    = existing ?? echarts.init(el, "dark");
        echartsInstances.current[key] = chart;
        chart.setOption({
          backgroundColor: "transparent",
          title: { text: title, textStyle: { fontSize: 11, color: "#BFBFBF", fontFamily: "inherit" }, left: 4 },
          tooltip: {
            trigger: "axis",
            backgroundColor: "rgba(18,18,18,0.92)",
            borderColor: "#474747",
            textStyle: { color: "#fff", fontSize: 11 },
          },
          xAxis: {
            type: "category", data: [], boundaryGap: false,
            axisLine: { lineStyle: { color: "#474747" } },
            axisLabel: { color: "#8C8C8C", fontSize: 9 },
          },
          yAxis: {
            type: "value",
            axisLine: { lineStyle: { color: "#474747" } },
            axisLabel: { color: "#8C8C8C", fontSize: 9 },
            splitLine: { lineStyle: { color: "#1a1a1a" } },
          },
          series: [],
        });
        chart.showLoading({ text: "Waiting…", textColor: "#8C8C8C", maskColor: "rgba(0,0,0,0.4)", color: "#F2561D" });
      }
    });
    return () => { disposed = true; };
  }, []);

  useEffect(() => {
    import("echarts").then((echarts) => {
      const now = new Date().toLocaleTimeString("en-GB", { hour12: false });

      const chartData: Record<string, { name: string; value: number }[]> = {
        bronze:   [{ name: "Customers",  value: metrics.bronze_customers },  { name: "Transactions", value: metrics.bronze_transactions }],
        silver:   [{ name: "Customers",  value: metrics.silver_customers },  { name: "Transactions", value: metrics.silver_transactions }, { name: "Profiles", value: metrics.silver_profiles }],
        gold:     [{ name: "Customers",  value: metrics.gold_customers },    { name: "Transactions", value: metrics.gold_transactions },   { name: "Fraud",    value: metrics.gold_fraud }],
        incoming: [{ name: "Ingested",   value: metrics.transactions_ingested }],
        consumer: [{ name: "Processed",  value: metrics.transactions_processed }],
      };

      const seriesColors = ["#F2561D", "#D9704A", "#008A8C", "#BFBFBF", "#8C8C8C"];

      for (const { key } of CHART_DEFS) {
        const el = chartRefs.current[key];
        if (!el) continue;
        const chart = echarts.getInstanceByDom(el);
        if (!chart) continue;

        const opt = chart.getOption() as {
          xAxis: { data: string[] }[];
          series: { name: string; type: string; data: number[]; smooth: boolean; showSymbol: boolean; lineStyle?: { color: string } }[];
        };

        const xData: string[] = (opt.xAxis?.[0]?.data as string[]) ?? [];
        const series = (opt.series as typeof opt.series) ?? [];

        xData.push(now);
        if (xData.length > 30) xData.shift();

        for (const [idx, point] of (chartData[key] ?? []).entries()) {
          let s = series.find((s) => s.name === point.name);
          if (!s) {
            s = { name: point.name, type: "line", data: [], smooth: true, showSymbol: false, lineStyle: { color: seriesColors[idx % seriesColors.length] } };
            series.push(s);
          }
          s.data.push(point.value);
          if (s.data.length > 30) s.data.shift();
        }

        chart.hideLoading();
        chart.setOption({ xAxis: [{ data: xData }], series });
      }
    });
  }, [metrics]);

  return (
    <div className="grid grid-cols-3 gap-3 w-full">
      {CHART_DEFS.map(({ key }) => (
        <div
          key={key}
          ref={(el) => { chartRefs.current[key] = el; }}
          className="rounded-lg"
          style={{ height: 160, background: "#121212", border: "1px solid #474747" }}
        />
      ))}
    </div>
  );
}

// ── Slide-up analytics panel ──────────────────────────────────────────────────

interface MonitoringChartsPanelProps {
  isOpen: boolean;
  onClose: () => void;
}

export function MonitoringChartsPanel({ isOpen, onClose }: MonitoringChartsPanelProps) {
  return (
    <AnimatePresence>
      {isOpen && (
        <motion.div
          className="fixed bottom-0 left-0 right-0 z-40"
          style={{
            background: "rgba(8,8,8,0.98)",
            backdropFilter: "blur(20px)",
            WebkitBackdropFilter: "blur(20px)",
            borderTop: "1px solid rgba(255,255,255,0.10)",
          }}
          initial={{ y: "100%" }}
          animate={{ y: 0 }}
          exit={{ y: "100%" }}
          transition={{ duration: 0.42, ease: [0.22, 1, 0.36, 1] }}
        >
          {/* Panel header */}
          <div
            className="flex items-center justify-between px-8 py-3"
            style={{ borderBottom: "1px solid rgba(255,255,255,0.07)" }}
          >
            <NexusSectionDivider
              // @ts-ignore
              title="Live Analytics"
              style={{ paddingLeft: 0, marginBottom: 0 }}
            />
            <button
              onClick={onClose}
              className="p-1 text-neutrals-medium hover:text-white transition-colors duration-200"
            >
              <RiCloseLine size={18} />
            </button>
          </div>

          {/* Charts grid */}
          <div className="px-8 py-4">
            <MonitoringCharts />
          </div>
        </motion.div>
      )}
    </AnimatePresence>
  );
}
