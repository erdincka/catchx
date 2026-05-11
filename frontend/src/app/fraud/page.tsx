"use client";

import { useState, useEffect } from "react";
import { motion, AnimatePresence } from "framer-motion";
import Header from "@/components/Header";
import Footer from "@/components/Footer";
import FraudDiagram from "@/components/FraudDiagram";
import ConnectDialog from "@/components/ConnectDialog";
import SettingsDialog from "@/components/SettingsDialog";
import CodeViewer from "@/components/CodeViewer";
import DataExplorer, { ExplorerTable, ExplorerFilesystem } from "@/components/DataExplorer";
import { MonitoringChartsPanel } from "@/components/MonitoringPanel";
import { useCluster } from "@/contexts/ClusterContext";
import { useSettings } from "@/contexts/SettingsContext";
import { useToast } from "@/contexts/ToastContext";
import {
  RiBarChartLine,
  RiDatabase2Line, RiRadarLine, RiDownloadLine,
  RiEqualLine, RiStackLine, RiShieldCheckLine,
  RiArrowRightSLine,
} from "@remixicon/react";
import {
  VOLUME_BRONZE, VOLUME_SILVER, VOLUME_GOLD,
  TABLE_CUSTOMERS, TABLE_TRANSACTIONS, TABLE_PROFILES,
} from "@/lib/constants";

// ── Demo sequence definition ──────────────────────────────────────────────────

interface DemoStep {
  num: number;
  label: string;
  icon: React.ReactNode;
  description: string;
  accent?: boolean;
}

const DEMO_STEPS: DemoStep[] = [
  { num: 1, label: "Generate", icon: <RiDatabase2Line size={14} />, description: "Create mock source data" },
  { num: 2, label: "Publish", icon: <RiRadarLine size={14} />, description: "Stream transactions to Kafka" },
  { num: 3, label: "Ingest", icon: <RiDownloadLine size={14} />, description: "Bronze tier ingestion" },
  { num: 4, label: "Refine", icon: <RiEqualLine size={14} />, description: "Enrich to Silver tier" },
  { num: 5, label: "Consolidate", icon: <RiStackLine size={14} />, description: "Merge Silver → Gold Delta Lake" },
  { num: 6, label: "Detect Fraud", icon: <RiShieldCheckLine size={14} />, description: "ML anomaly detection on Gold data" },
];

// ── Page ─────────────────────────────────────────────────────────────────────

export default function FraudDomainPage() {
  const { host, user, pass, metrics, settings, setHost, setUser, setPass, setMonitorActive } = useCluster();
  const { settings: backendSettings } = useSettings();
  const { notify } = useToast();

  const clusterHost = backendSettings?.cluster_host || host;

  // Enable monitoring on mount, disable on unmount
  useEffect(() => {
    setMonitorActive(true);
    return () => setMonitorActive(false);
  }, [setMonitorActive]);

  // Sync backend-persisted credentials into ClusterContext so monitoring polling fires
  useEffect(() => {
    if (!host && backendSettings?.cluster_host) {
      setHost(backendSettings.cluster_host);
      setUser(backendSettings.credentials.cluster_user ?? "");
      setPass(backendSettings.credentials.cluster_pass ?? "");
    }
  }, [backendSettings, host, setHost, setUser, setPass]);

  const [showConnect, setShowConnect] = useState(false);
  const [showSettings, setShowSettings] = useState(false);
  const [showCharts, setShowCharts] = useState(false);
  const [codeTarget, setCodeTarget] = useState<string | null>(null);
  const [customersCreated, setCustomersCreated] = useState(false);
  const [transactionsCreated, setTransactionsCreated] = useState(false);

  // DataExplorer state — table mode for data, filesystem mode for volumes
  const [explorer, setExplorer] = useState<{
    title: string;
    records?: Record<string, unknown>[];
    fsOutput?: string;
    fsPath?: string;
  } | null>(null);

  const headers = { "X-Mapr-Host": host, "X-Mapr-User": user, "X-Mapr-Pass": pass };

  async function post(path: string) {
    return fetch(path, { method: "POST", headers: { ...headers, "Content-Type": "application/json" } });
  }

  async function peekTier(tier: string, table: string) {
    const r = await fetch(`/api/data/peek/${tier}/${table}`, { headers });
    if (!r.ok) { notify("Failed to load data.", "negative"); return; }
    const d = await r.json();
    setExplorer({ title: `${tier} › ${table}`, records: d.records ?? [] });
  }

  async function peekIcebergTail(tier: string, table: string) {
    const r = await fetch(`/api/data/iceberg/${tier}/${table}/tail`, { headers });
    if (!r.ok) { notify("Failed to load data.", "negative"); return; }
    const d = await r.json();
    setExplorer({ title: `${tier} › ${table} (Iceberg)`, records: d.records ?? [] });
  }

  async function peekPreview(type: "customers" | "transactions") {
    const r = await fetch(`/api/data/${type}/preview`, { headers });
    if (!r.ok) { notify("Failed to load preview.", "negative"); return; }
    const d = await r.json();
    setExplorer({ title: `${type} preview`, records: d.records ?? [] });
  }

  async function handleVolumeExplore(label: string, path: string) {
    setExplorer({ title: `Exploring: ${label}`, fsOutput: "Loading…", fsPath: path });
    try {
      const r = await fetch(`/api/data/fs/list?path=${encodeURIComponent(path)}`, { headers });
      const d = await r.json();
      setExplorer({ title: `Exploring: ${label}`, fsOutput: d.output ?? "(empty)", fsPath: path });
    } catch {
      setExplorer({ title: `Exploring: ${label}`, fsOutput: "Failed to list path.", fsPath: path });
    }
  }

  // Unified action handler (shared with FraudDiagram)
  async function handleAction(id: string) {
    switch (id) {
      /* ── Source data preview ──────────────────────────────────────── */
      case "PreviewCustomers": await peekPreview("customers"); break;
      case "PreviewTransactions": await peekPreview("transactions"); break;
      case "CodeCustomers": setCodeTarget("create_customers"); break;
      case "CodeTransactions": setCodeTarget("create_transactions"); break;
      /* ── Generate ─────────────────────────────────────────────────── */
      case "CreateCustomers": {
        const r = await post("/api/data/customers/create");
        const d = await r.json();
        if (r.ok) { notify(`Created ${d.count ?? 0} customers.`, "positive"); setCustomersCreated(true); }
        else notify(d.detail ?? "Failed.", "negative");
        break;
      }
      case "CreateTransactions": {
        const r = await post("/api/data/transactions/create");
        const d = await r.json();
        if (r.ok) { notify(`Created ${d.count ?? 0} transactions.`, "positive"); setTransactionsCreated(true); }
        else notify(d.detail ?? "Failed.", "negative");
        break;
      }
      /* ── Publish ──────────────────────────────────────────────────── */
      case "PublishTransactions": {
        const r = await post("/api/data/transactions/publish");
        const d = await r.json();
        if (r.ok && d.status === "ok") notify(`Published ${d.count} transactions to stream.`, "positive");
        else notify(d.detail ?? d.message ?? "Failed.", "negative");
        break;
      }
      case "PublishTransactionsCode": setCodeTarget("publish_transactions"); break;
      /* ── Ingest ───────────────────────────────────────────────────── */
      case "IngestTransactions": {
        const r = await post("/api/data/ingest/transactions");
        const d = await r.json();
        if (r.ok && d.status === "ok") notify(`Ingested ${d.count ?? 0} transactions.`, "positive");
        else notify(d.detail ?? d.message ?? "Failed.", "negative");
        break;
      }
      case "IngestTransactionsCode": setCodeTarget("ingest_transactions"); break;
      case "IngestCustomersIceberg": {
        const r = await post("/api/data/ingest/customers");
        const d = await r.json();
        if (r.ok && d.status === "ok") notify(`Ingested ${d.count ?? 0} customers via Iceberg.`, "positive");
        else notify(d.detail ?? d.message ?? "Failed.", "negative");
        break;
      }
      case "IngestCustomersIcebergCode": setCodeTarget("ingest_customers_iceberg"); break;
      /* ── Peek tiers ───────────────────────────────────────────────── */
      case "BronzeTransactions": await peekTier(VOLUME_BRONZE, TABLE_TRANSACTIONS); break;
      case "BronzeCustomers": await peekIcebergTail(VOLUME_BRONZE, TABLE_CUSTOMERS); break;
      case "SilverCustomers": await peekTier(VOLUME_SILVER, TABLE_CUSTOMERS); break;
      case "SilverTransactions": await peekTier(VOLUME_SILVER, TABLE_TRANSACTIONS); break;
      case "SilverProfiles": await peekTier(VOLUME_SILVER, TABLE_PROFILES); break;
      case "GoldCustomers": await peekTier(VOLUME_GOLD, TABLE_CUSTOMERS); break;
      /* ── Profile builder ──────────────────────────────────────────── */
      case "ProfileBuilderCode": setCodeTarget("upsert_profile"); break;
      /* ── Refine ───────────────────────────────────────────────────── */
      case "RefineTransactions": {
        const r = await post("/api/data/refine/transactions");
        const d = await r.json();
        if (r.ok && d.status === "ok") notify(`Refined ${d.count ?? 0} transactions.`, "positive");
        else notify(d.detail ?? d.message ?? "Failed.", "negative");
        break;
      }
      case "RefineTransactionsCode": setCodeTarget("refine_transactions"); break;
      case "RefineCustomers": {
        const r = await post("/api/data/refine/customers");
        const d = await r.json();
        if (r.ok && d.status === "ok") notify(`Refined ${d.count ?? 0} customers.`, "positive");
        else notify(d.detail ?? d.message ?? "Failed.", "negative");
        break;
      }
      case "RefineCustomersCode": setCodeTarget("refine_customers"); break;
      /* ── Consolidate ──────────────────────────────────────────────── */
      case "Consolidate": {
        const r = await post("/api/data/consolidate");
        const d = await r.json();
        if (r.ok && d.status === "ok") notify(d.message ?? "Consolidation complete.", "positive");
        else notify(d.detail ?? d.message ?? "Failed.", "negative");
        break;
      }
      case "ConsolidateCode": setCodeTarget("create_golden"); break;
      /* ── Fraud detection ──────────────────────────────────────────── */
      case "CheckFraud": {
        const r = await post("/api/data/fraud");
        const d = await r.json();
        if (r.ok && d.status === "ok")
          notify(`Fraud detection complete — ${d.fraud_count} flagged, ${d.non_fraud_count} clean.`, "warning");
        else notify(d.detail ?? d.message ?? "Failed.", "negative");
        break;
      }
      case "CheckFraudCode": setCodeTarget("fraud_detection"); break;
      /* ── External tools ───────────────────────────────────────────── */
      case "ReportView": {
        const url = settings.dashboardUrl;
        if (!url) notify("Set Dashboard URL in Settings.", "warning");
        else window.open(url, "_blank");
        break;
      }
      case "Catalogue": {
        const url = settings.catalogueUrl;
        if (!url) notify("Set Catalogue URL in Settings.", "warning");
        else window.open(url, "_blank");
        break;
      }
      default:
        if (id) notify(`${id}: not configured.`, "info");
    }
  }

  return (
    <div className="flex flex-col h-screen bg-neutrals-deep overflow-hidden">
      <Header
        onConnectClick={() => setShowConnect(true)}
        onSettingsClick={() => setShowSettings(true)}
      />

      {/* ── Demo sequence strip ────────────────────────────────────────── */}
      <motion.div
        className="shrink-0 px-3 pt-[84px] pb-2"
        initial={{ opacity: 0, y: -10 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.45, ease: [0.22, 1, 0.36, 1] }}
      >
        <div
          className="flex items-stretch gap-0 rounded-xl overflow-hidden"
          style={{ background: "#0a0a0a", border: "1px solid rgba(255,255,255,0.07)" }}
        >
          {/* Strip label */}
          <div
            className="flex flex-col items-center justify-center px-3 shrink-0"
            style={{ background: "#121212", borderRight: "1px solid #2a2a2a", minWidth: 72 }}
          >
            <span className="font-sans font-medium text-[9px] text-neutrals-dark uppercase tracking-[0.18em] text-center leading-tight">
              Demo<br />Sequence
            </span>
          </div>

          {/* Steps */}
          {DEMO_STEPS.map((step, i) => (
            <div key={step.num} className="flex items-center flex-1">
              <DemoStepCard step={step} />
              {i < DEMO_STEPS.length - 1 && (
                <div className="shrink-0 flex items-center justify-center w-5">
                  <RiArrowRightSLine size={14} className="text-neutrals-dark" />
                </div>
              )}
            </div>
          ))}

          {/* Analytics shortcut */}
          <button
            onClick={() => setShowCharts((v) => !v)}
            title="Open live analytics charts"
            className="flex flex-col items-center justify-center px-3 shrink-0 gap-0.5 transition-colors duration-200"
            style={{
              borderLeft: "1px solid #2a2a2a",
              minWidth: 60,
              background: showCharts ? "rgba(242,86,29,0.12)" : "transparent",
            }}
            onMouseEnter={(e) => { if (!showCharts) (e.currentTarget as HTMLElement).style.background = "rgba(255,255,255,0.04)"; }}
            onMouseLeave={(e) => { if (!showCharts) (e.currentTarget as HTMLElement).style.background = "transparent"; }}
          >
            <RiBarChartLine size={14} className={showCharts ? "text-brand-vivid" : "text-neutrals-medium"} />
            <span className="font-sans text-[8px] uppercase tracking-wider text-neutrals-dark">Charts</span>
          </button>
        </div>
      </motion.div>

      {/* ── Main content area ──────────────────────────────────────────── */}
      <main className="flex-1 px-3 pb-2 min-h-0 overflow-hidden">
        <motion.div
          className="w-full h-full"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ duration: 0.5, delay: 0.1, ease: [0.22, 1, 0.36, 1] }}
        >
          <FraudDiagram
            onAction={handleAction}
            interactive={!!clusterHost}
            metrics={metrics}
            customersCreated={customersCreated}
            transactionsCreated={transactionsCreated}
          />
        </motion.div>
      </main>

      <Footer onVolumeExplore={handleVolumeExplore} />

      {/* ── Overlays & panels ─────────────────────────────────────────── */}

      {showConnect && <ConnectDialog onClose={() => setShowConnect(false)} />}
      {showSettings && <SettingsDialog onClose={() => setShowSettings(false)} />}
      {codeTarget && <CodeViewer functionName={codeTarget} onClose={() => setCodeTarget(null)} />}

      {/* Data Explorer — right slide-in for data table preview */}
      <DataExplorer
        title={explorer?.title ?? "Data Preview"}
        isOpen={!!explorer}
        onClose={() => setExplorer(null)}
      >
        {explorer && (
          explorer.fsOutput !== undefined
            ? <ExplorerFilesystem path={explorer.fsPath ?? ""} output={explorer.fsOutput} />
            : <ExplorerTable records={explorer.records ?? []} />
        )}
      </DataExplorer>

      {/* Analytics charts — slide up from bottom */}
      <MonitoringChartsPanel isOpen={showCharts} onClose={() => setShowCharts(false)} />
    </div>
  );
}

// ── Demo Sequence step card ───────────────────────────────────────────────────

function DemoStepCard({ step }: { step: DemoStep }) {
  return (
    <div className="flex flex-col gap-1 px-2.5 py-2 flex-1">
      <div className="flex items-center gap-1.5">
        <span
          className="w-4 h-4 rounded-full flex items-center justify-center font-sans font-bold text-[9px] text-white shrink-0"
          style={{ background: step.accent ? "#F2561D" : "#474747" }}
        >
          {step.num}
        </span>
        <span className={`font-sans text-[10px] font-semibold uppercase tracking-wider leading-none ${step.accent ? "text-brand-vivid" : "text-neutrals-light"}`}>
          {step.label}
        </span>
        <span className={`leading-none ${step.accent ? "text-brand-vivid" : "text-neutrals-medium"}`}>
          {step.icon}
        </span>
      </div>
      <p className="font-sans font-light text-[9px] text-neutrals-dark leading-tight">{step.description}</p>
    </div>
  );
}

