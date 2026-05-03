"use client";

import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import Header from "@/components/Header";
import Footer from "@/components/Footer";
import FraudDiagram from "@/components/FraudDiagram";
import ConnectDialog from "@/components/ConnectDialog";
import SettingsDialog from "@/components/SettingsDialog";
import CodeViewer from "@/components/CodeViewer";
import DataExplorer, { ExplorerTable } from "@/components/DataExplorer";
import { MonitoringCard, MonitoringChartsPanel } from "@/components/MonitoringPanel";
import { NexusCard, NexusSectionDivider } from "@/components/nexus-core-components";
import { useCluster } from "@/contexts/ClusterContext";
import { useToast } from "@/contexts/ToastContext";
import {
  RiEyeLine, RiCodeSSlashLine, RiAddLine,
  RiBarChartLine, RiLoader4Line,
  RiDatabase2Line, RiRadarLine, RiDownloadLine,
  RiEqualLine, RiStackLine, RiShieldCheckLine,
  RiArrowRightSLine,
} from "@remixicon/react";
import {
  VOLUME_BRONZE, VOLUME_SILVER, VOLUME_GOLD,
  TABLE_CUSTOMERS, TABLE_TRANSACTIONS, TABLE_PROFILES,
} from "@/lib/constants";

// ── Demo sequence definition ──────────────────────────────────────────────────

interface DemoAction {
  label: string;
  id: string;
}

interface DemoStep {
  num: number;
  label: string;
  icon: React.ReactNode;
  description: string;
  actions: DemoAction[];
  accent?: boolean;
}

const DEMO_STEPS: DemoStep[] = [
  {
    num: 1,
    label: "Generate",
    icon: <RiDatabase2Line size={14} />,
    description: "Create mock source data",
    actions: [
      { label: "Customers",    id: "CreateCustomers"    },
      { label: "Transactions", id: "CreateTransactions" },
    ],
  },
  {
    num: 2,
    label: "Publish",
    icon: <RiRadarLine size={14} />,
    description: "Stream transactions to Kafka",
    actions: [{ label: "Publish", id: "PublishTransactions" }],
  },
  {
    num: 3,
    label: "Ingest",
    icon: <RiDownloadLine size={14} />,
    description: "Bronze tier ingestion",
    actions: [
      { label: "Transactions", id: "IngestTransactions"      },
      { label: "Customers",    id: "IngestCustomersIceberg"  },
    ],
  },
  {
    num: 4,
    label: "Refine",
    icon: <RiEqualLine size={14} />,
    description: "Enrich to Silver tier",
    actions: [
      { label: "Transactions", id: "RefineTransactions" },
      { label: "Customers",    id: "RefineCustomers"    },
    ],
  },
  {
    num: 5,
    label: "Consolidate",
    icon: <RiStackLine size={14} />,
    description: "Merge Silver → Gold Delta Lake",
    actions: [{ label: "Run", id: "Consolidate" }],
  },
  {
    num: 6,
    label: "Detect Fraud",
    icon: <RiShieldCheckLine size={14} />,
    description: "ML anomaly detection on Gold data",
    actions: [{ label: "Detect", id: "CheckFraud" }],
    accent: true,
  },
];

// ── Page ─────────────────────────────────────────────────────────────────────

export default function FraudDomainPage() {
  const { host, user, pass, demoMode, monitorActive, metrics, settings } = useCluster();
  const { notify } = useToast();

  const [showConnect,   setShowConnect]   = useState(false);
  const [showSettings,  setShowSettings]  = useState(false);
  const [showCharts,    setShowCharts]    = useState(false);
  const [codeTarget,    setCodeTarget]    = useState<string | null>(null);
  const [runningStep,   setRunningStep]   = useState<string | null>(null);

  // DataExplorer state — replaces DataTable modal
  const [explorer, setExplorer] = useState<{
    title: string;
    records: Record<string, unknown>[];
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
    // filesystem explore — no records, handled separately
    // Re-use explorer with a sentinel to detect filesystem mode
    setExplorer({ title: `Exploring: ${label}`, records: [] });
  }

  // Demo sequence action handler
  async function runSequenceAction(id: string) {
    if (!demoMode) {
      notify("Enable the 'Live' toggle to run demo steps.", "info");
      return;
    }
    setRunningStep(id);
    try {
      await handleAction(id);
    } finally {
      setRunningStep(null);
    }
  }

  // Unified action handler (shared with FraudDiagram)
  async function handleAction(id: string) {
    if (!demoMode && !["BronzeTransactions","BronzeCustomers","SilverCustomers","SilverTransactions","SilverProfiles","GoldCustomers"].includes(id)) {
      notify("Enable the 'Live' toggle to interact.", "info");
      return;
    }

    switch (id) {
      /* ── Generate ─────────────────────────────────────────────────── */
      case "CreateCustomers": {
        const r = await post("/api/data/customers/create");
        const d = await r.json();
        if (r.ok) notify(`Created ${d.count ?? 0} customers.`, "positive");
        else notify(d.detail ?? "Failed.", "negative");
        break;
      }
      case "CreateTransactions": {
        const r = await post("/api/data/transactions/create");
        const d = await r.json();
        if (r.ok) notify(`Created ${d.count ?? 0} transactions.`, "positive");
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
      /* ── NiFi / Airflow ───────────────────────────────────────────── */
      case "NifiStreams": {
        if (host) window.open(`https://${host}:12443/nifi/`, "_blank");
        else notify("Connect to a cluster first.", "warning");
        break;
      }
      case "NifiStreamsCode":            setCodeTarget("nifi_template");             break;
      case "AirflowBatch": {
        if (host) window.open(`https://${host}:8780/home`, "_blank");
        else notify("Connect to a cluster first.", "warning");
        break;
      }
      case "AirflowBatchCode":           setCodeTarget("airflow_dag");               break;
      /* ── Ingest ───────────────────────────────────────────────────── */
      case "IngestTransactions": {
        const r = await post("/api/data/ingest/transactions");
        const d = await r.json();
        if (r.ok && d.status === "ok") notify(`Ingested ${d.count ?? 0} transactions.`, "positive");
        else notify(d.detail ?? d.message ?? "Failed.", "negative");
        break;
      }
      case "IngestTransactionsCode":     setCodeTarget("ingest_transactions");        break;
      case "IngestCustomersIceberg": {
        const r = await post("/api/data/ingest/customers");
        const d = await r.json();
        if (r.ok && d.status === "ok") notify(`Ingested ${d.count ?? 0} customers via Iceberg.`, "positive");
        else notify(d.detail ?? d.message ?? "Failed.", "negative");
        break;
      }
      case "IngestCustomersIcebergCode": setCodeTarget("ingest_customers_iceberg");  break;
      /* ── Peek tiers ───────────────────────────────────────────────── */
      case "BronzeTransactions":  await peekTier(VOLUME_BRONZE, TABLE_TRANSACTIONS); break;
      case "BronzeCustomers":     await peekIcebergTail(VOLUME_BRONZE, TABLE_CUSTOMERS); break;
      case "SilverCustomers":     await peekTier(VOLUME_SILVER, TABLE_CUSTOMERS); break;
      case "SilverTransactions":  await peekTier(VOLUME_SILVER, TABLE_TRANSACTIONS); break;
      case "SilverProfiles":      await peekTier(VOLUME_SILVER, TABLE_PROFILES); break;
      case "GoldCustomers":       await peekTier(VOLUME_GOLD,   TABLE_CUSTOMERS); break;
      /* ── Profile builder ──────────────────────────────────────────── */
      case "ProfileBuilderCode":  setCodeTarget("upsert_profile"); break;
      /* ── Refine ───────────────────────────────────────────────────── */
      case "RefineTransactions": {
        const r = await post("/api/data/refine/transactions");
        const d = await r.json();
        if (r.ok) notify(d.message ?? "Transactions refined.", "positive");
        else notify(d.detail ?? "Failed.", "negative");
        break;
      }
      case "RefineTransactionsCode": setCodeTarget("refine_transactions"); break;
      case "RefineCustomers": {
        const r = await post("/api/data/refine/customers");
        const d = await r.json();
        if (r.ok) notify(d.message ?? "Customers refined.", "positive");
        else notify(d.detail ?? "Failed.", "negative");
        break;
      }
      case "RefineCustomersCode":    setCodeTarget("refine_customers"); break;
      /* ── Consolidate ──────────────────────────────────────────────── */
      case "Consolidate": {
        const r = await post("/api/data/consolidate");
        const d = await r.json();
        if (r.ok) notify(d.message ?? "Consolidation complete.", "positive");
        else notify(d.detail ?? "Failed.", "negative");
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
              <DemoStepCard
                step={step}
                disabled={!demoMode}
                running={step.actions.some((a) => runningStep === a.id)}
                onAction={runSequenceAction}
              />
              {/* Connector arrow between steps */}
              {i < DEMO_STEPS.length - 1 && (
                <div className="shrink-0 flex items-center justify-center w-5">
                  <RiArrowRightSLine size={14} className="text-neutrals-dark" />
                </div>
              )}
            </div>
          ))}

          {/* Analytics shortcut */}
          {monitorActive && (
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
          )}
        </div>
      </motion.div>

      {/* ── Main content area ──────────────────────────────────────────── */}
      <main className="flex-1 flex gap-2 px-3 pb-2 min-h-0 overflow-hidden">

        {/* Left panel: Source Data + Monitoring */}
        <motion.div
          className="flex flex-col gap-2 shrink-0 w-[200px] overflow-y-auto"
          initial={{ opacity: 0, x: -16 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ duration: 0.45, delay: 0.1, ease: [0.22, 1, 0.36, 1] }}
        >
          <SourcePanel
            onPeekCustomers={()    => peekPreview("customers")}
            onCodeCustomers={()    => setCodeTarget("create_customers")}
            onCreateCustomers={async () => {
              await runSequenceAction("CreateCustomers");
            }}
            onPeekTransactions={()  => peekPreview("transactions")}
            onCodeTransactions={()  => setCodeTarget("create_transactions")}
            onCreateTransactions={async () => {
              await runSequenceAction("CreateTransactions");
            }}
          />
          {monitorActive && (
            <MonitoringCard onOpenCharts={() => setShowCharts((v) => !v)} />
          )}
        </motion.div>

        {/* Fraud pipeline diagram */}
        <motion.div
          className="flex-1 min-w-0 overflow-hidden"
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ duration: 0.5, delay: 0.15, ease: [0.22, 1, 0.36, 1] }}
        >
          <FraudDiagram
            onAction={handleAction}
            interactive={demoMode}
            metrics={metrics}
          />
        </motion.div>
      </main>

      <Footer onVolumeExplore={handleVolumeExplore} />

      {/* ── Overlays & panels ─────────────────────────────────────────── */}

      {showConnect  && <ConnectDialog  onClose={() => setShowConnect(false)} />}
      {showSettings && <SettingsDialog onClose={() => setShowSettings(false)} />}
      {codeTarget   && <CodeViewer functionName={codeTarget} onClose={() => setCodeTarget(null)} />}

      {/* Data Explorer — right slide-in for data table preview */}
      <DataExplorer
        title={explorer?.title ?? "Data Preview"}
        isOpen={!!explorer}
        onClose={() => setExplorer(null)}
      >
        {explorer && <ExplorerTable records={explorer.records} />}
      </DataExplorer>

      {/* Analytics charts — slide up from bottom */}
      <MonitoringChartsPanel isOpen={showCharts} onClose={() => setShowCharts(false)} />
    </div>
  );
}

// ── Demo Sequence step card ───────────────────────────────────────────────────

function DemoStepCard({
  step, disabled, running, onAction,
}: {
  step: DemoStep;
  disabled: boolean;
  running: boolean;
  onAction: (id: string) => void;
}) {
  return (
    <div
      className="flex flex-col gap-1 px-2.5 py-2 flex-1"
      style={{
        opacity: disabled ? 0.45 : 1,
        transition: "opacity 0.2s",
      }}
    >
      {/* Step number + label */}
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

      {/* Description */}
      <p className="font-sans font-light text-[9px] text-neutrals-dark leading-tight">{step.description}</p>

      {/* Action buttons */}
      <div className="flex flex-wrap gap-1 mt-auto">
        {step.actions.map((action) => (
          <button
            key={action.id}
            onClick={() => onAction(action.id)}
            disabled={disabled || running}
            className="flex items-center gap-1 font-sans font-semibold text-[9px] text-white rounded px-2 py-0.5 transition-colors duration-200 disabled:opacity-40 disabled:cursor-not-allowed"
            style={{
              background: step.accent ? "#F2561D" : "rgba(255,255,255,0.10)",
              border: step.accent ? "none" : "1px solid #474747",
            }}
            onMouseEnter={(e) => {
              if (!disabled && !running) {
                (e.currentTarget as HTMLElement).style.background = step.accent ? "#D9704A" : "rgba(255,255,255,0.16)";
              }
            }}
            onMouseLeave={(e) => {
              (e.currentTarget as HTMLElement).style.background = step.accent ? "#F2561D" : "rgba(255,255,255,0.10)";
            }}
          >
            {running && <RiLoader4Line size={9} className="animate-spin" />}
            {action.label}
          </button>
        ))}
      </div>
    </div>
  );
}

// ── Source data panel ─────────────────────────────────────────────────────────

function SourcePanel({
  onPeekCustomers, onCodeCustomers, onCreateCustomers,
  onPeekTransactions, onCodeTransactions, onCreateTransactions,
}: {
  onPeekCustomers:     () => void;
  onCodeCustomers:     () => void;
  onCreateCustomers:   () => void;
  onPeekTransactions:  () => void;
  onCodeTransactions:  () => void;
  onCreateTransactions:() => void;
}) {
  const items = [
    { label: "Customers",    onPeek: onPeekCustomers,    onCode: onCodeCustomers,    onCreate: onCreateCustomers },
    { label: "Transactions", onPeek: onPeekTransactions, onCode: onCodeTransactions, onCreate: onCreateTransactions },
  ];

  return (
    // @ts-ignore
    <NexusCard variant="status">
      <div className="p-3">
        <NexusSectionDivider
          // @ts-ignore
          title="Source Data"
          style={{ paddingLeft: 0, marginBottom: 10 }}
        />
        <div className="flex flex-col gap-2">
          {items.map(({ label, onPeek, onCode, onCreate }) => (
            <div key={label} className="rounded-lg overflow-hidden" style={{ border: "1px solid #474747" }}>
              <div
                className="px-2.5 py-1.5 font-sans font-semibold text-[10px] text-neutrals-light uppercase tracking-wider"
                style={{ background: "#0a0a0a", borderBottom: "1px solid #2a2a2a" }}
              >
                {label}
              </div>
              <div className="flex gap-1 p-1.5" style={{ background: "#000000" }}>
                <PanelBtn onClick={onPeek}   title="Preview data"><RiEyeLine size={12} /></PanelBtn>
                <PanelBtn onClick={onCode}   title="View source code"><RiCodeSSlashLine size={12} /></PanelBtn>
                <PanelBtn onClick={onCreate} title="Generate mock data"><RiAddLine size={12} /></PanelBtn>
              </div>
            </div>
          ))}
        </div>
      </div>
    </NexusCard>
  );
}

function PanelBtn({ onClick, title, children }: {
  onClick: () => void;
  title?: string;
  children: React.ReactNode;
}) {
  return (
    <button
      onClick={onClick}
      title={title}
      className="flex-1 flex items-center justify-center py-1.5 rounded text-neutrals-medium hover:text-brand-vivid transition-colors duration-200"
      style={{ border: "1px solid #2a2a2a" }}
      onMouseEnter={(e) => (e.currentTarget.style.borderColor = "#F2561D")}
      onMouseLeave={(e) => (e.currentTarget.style.borderColor = "#2a2a2a")}
    >
      {children}
    </button>
  );
}
