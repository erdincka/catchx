"use client";

import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import {
  RiHardDriveLine,
  RiCloudLine,
  RiShieldCheckLine,
  RiKeyLine,
  RiBarChartLine,
  RiFileSearchLine,
  RiBookOpenLine,
  RiCheckboxMultipleLine,
  RiUserLine,
} from "@remixicon/react";

// ── Nexus Financial — domain descriptions ─────────────────────────────────────

const DOMAIN_INFO: Record<string, { title: string; description: string; click?: boolean }> = {
  Fraud: {
    title: "Fraud & Risk",
    description:
      "Real-time transaction risk scoring, behavioural anomaly detection, and automated alert routing. " +
      "Owns the full ingest-to-decision pipeline — from batch customer profiles to live stream scoring. " +
      "Data products published to Compliance and Finance domains.",
    click: true,
  },
  Payments: {
    title: "Payments Processing",
    description:
      "Inbound and outbound payment processing, settlement batches, and FX rate snapshots. " +
      "Publishes authoritative transaction data consumed by Finance, Compliance, and Fraud domains " +
      "via versioned, governed data products on the Global Namespace.",
  },
  Customers: {
    title: "Customer 360",
    description:
      "Unified customer identity, KYC/AML status, and enriched profiles stored in a single-source-of-truth " +
      "volume accessible across all domains. Onboarding events stream into Fraud & Risk in real time.",
  },
  Finance: {
    title: "Finance & Treasury",
    description:
      "General ledger, P&L reporting, treasury positions, and regulatory capital calculations — " +
      "aggregated from Payments and Operations into Iceberg Gold tables for BI and audit consumption.",
  },
  Compliance: {
    title: "Compliance & Audit",
    description:
      "Regulatory filings, immutable audit trails in append-only volumes, and automated control testing. " +
      "Consumes versioned data products from every domain via policy-governed, time-bounded access.",
  },
  Operations: {
    title: "Platform Operations",
    description:
      "System observability, incident management, SLA tracking, and infrastructure telemetry. " +
      "Feeds the platform Observability surface with real-time cluster health and pipeline latency metrics.",
  },
  Marketing: {
    title: "Marketing & CX",
    description:
      "Customer segmentation, campaign performance measurement, and attribution modelling. " +
      "Consumes Customer 360 and Finance data products to personalise and measure marketing initiatives.",
  },
  DataSci: {
    title: "Data Science",
    description:
      "Feature engineering pipelines, model training datasets, and experiment tracking — all backed by " +
      "Iceberg tables managed through the Polaris catalog and shareable as governed data products.",
  },
};

// ── Governance / platform layer tool descriptions ─────────────────────────────

const GOV_INFO: Record<string, string> = {
  "Auth & SSO":      "Centralised identity and single sign-on — LDAP, SAML, and OAuth2 integration points managed at platform level for all domain teams.",
  "Access Control":  "Attribute- and role-based access enforced at volume, table, column, and field level. Policies defined once, applied across all data products.",
  "Log Analytics":   "Centralised log ingestion and search across all cluster nodes and domain services — feeds security alerting and operational diagnostics.",
  "Key Management":  "Platform-managed encryption key lifecycle — data at rest and in transit protected by centrally rotated keys without domain-level complexity.",
  "Observability":   "Unified metrics dashboards for cluster health, pipeline latency, and domain SLAs — single pane of glass for platform and domain engineers.",
  "Data Quality":    "Automated quality checks, SLA enforcement, and freshness monitoring applied consistently across all data products in the mesh.",
  "Data Catalogue":  "Discover, understand, and govern all data products — schema registry, data lineage, ownership, and business glossary in one place.",
};

// ── SVG: database cylinder ────────────────────────────────────────────────────

function DbCylinder({
  x, y, w = 34, h = 42, color = "#F2561D", label = "",
}: {
  x: number; y: number; w?: number; h?: number; color?: string; label?: string;
}) {
  const rx = w / 2;
  const ry = Math.round(rx * 0.28);
  return (
    <g>
      <rect x={x} y={y + ry} width={w} height={h - ry * 2} rx={2} fill={color} />
      <ellipse cx={x + rx} cy={y + ry}     rx={rx} ry={ry} fill={color} />
      <ellipse cx={x + rx} cy={y + h - ry} rx={rx} ry={ry} fill={color} fillOpacity={0.6} />
      {label && (
        <text
          x={x + rx} y={y + h + 13}
          textAnchor="middle"
          fontSize={7.5}
          fill="#7A7A7A"
          fontFamily="system-ui,sans-serif"
          letterSpacing={0.3}
        >
          {label}
        </text>
      )}
    </g>
  );
}

// ── Domain box ────────────────────────────────────────────────────────────────

const DB_COLORS = ["#F2561D", "#D9704A", "#008A8C"];

interface Domain {
  id: string;
  label: string;
  tables: string[];
  interactive?: boolean;
}

function DomainBox({
  id, label, tables, interactive, onHover, onClick,
}: Domain & { onHover: (id: string | null) => void; onClick: (id: string) => void }) {
  const W = 32, H = 40;
  const svgW = 210;
  const n = tables.length;
  const step = svgW / (n + 1);

  return (
    <div
      className="relative rounded-xl select-none flex-1 transition-all duration-200"
      style={{
        background: "#121212",
        border: interactive ? "2px solid #F2561D" : "2px solid rgba(255,255,255,0.10)",
        boxShadow: interactive ? "0 4px 28px rgba(242,86,29,0.15)" : "none",
        cursor: interactive ? "pointer" : "default",
      }}
      onClick={() => interactive && onClick(id)}
      onMouseEnter={() => onHover(id)}
      onMouseLeave={() => onHover(null)}
    >
      <svg width="100%" height={108} viewBox={`0 0 ${svgW} 108`} preserveAspectRatio="xMidYMid meet">
        {tables.map((t, i) => (
          <DbCylinder
            key={t}
            x={step * (i + 1) - W / 2}
            y={10}
            w={W} h={H}
            color={DB_COLORS[i % DB_COLORS.length]}
            label={t}
          />
        ))}
      </svg>
      <div className="px-3 pb-3 flex items-center gap-2">
        <span className="font-sans font-semibold text-[13px] text-white leading-tight">{label}</span>
        {interactive && (
          <span className="w-2 h-2 rounded-full shrink-0 animate-pulse ml-0.5" style={{ background: "#F2561D" }} />
        )}
      </div>
    </div>
  );
}

// ── Domain tooltip ────────────────────────────────────────────────────────────

function DomainTooltip({ id }: { id: string }) {
  const info = DOMAIN_INFO[id];
  if (!info) return null;
  return (
    <motion.div
      className="absolute inset-0 flex items-center justify-center pointer-events-none z-50"
      initial={{ opacity: 0, scale: 0.95 }}
      animate={{ opacity: 1, scale: 1 }}
      exit={{ opacity: 0, scale: 0.95 }}
      transition={{ duration: 0.18, ease: "easeOut" }}
    >
      <div
        className="rounded-2xl p-6 shadow-2xl max-w-sm text-center"
        style={{
          background: "rgba(14, 14, 14, 0.96)",
          backdropFilter: "blur(24px)",
          WebkitBackdropFilter: "blur(24px)",
          border: "1px solid rgba(255,255,255,0.10)",
        }}
      >
        <p className="font-sans font-medium text-brand-vivid mb-1 text-[10px] uppercase tracking-[0.18em]">
          Data Domain
        </p>
        <p className="font-sans font-bold text-white text-base mb-3">{info.title}</p>
        <p className="font-sans font-light text-sm text-neutrals-light leading-relaxed">{info.description}</p>
        {info.click && (
          <p className="mt-4 font-sans font-semibold text-[11px] text-brand-vivid uppercase tracking-wider">
            Click to explore the live pipeline →
          </p>
        )}
      </div>
    </motion.div>
  );
}

// ── Governance tool chip ──────────────────────────────────────────────────────

function GovTool({
  icon, label, onClick, tooltipText,
}: {
  icon?: React.ReactNode;
  label: string;
  onClick?: () => void;
  tooltipText?: string;
}) {
  const [tip, setTip] = useState(false);
  return (
    <div className="relative">
      <div
        className="flex items-center gap-1.5 rounded px-2.5 py-1.5 whitespace-nowrap font-sans text-[11px] font-medium text-white transition-colors duration-200"
        style={{
          background: "rgba(255,255,255,0.06)",
          cursor: onClick ? "pointer" : "default",
        }}
        onMouseEnter={(e) => {
          if (onClick) (e.currentTarget as HTMLElement).style.background = "rgba(242,86,29,0.16)";
          if (tooltipText) setTip(true);
        }}
        onMouseLeave={(e) => {
          (e.currentTarget as HTMLElement).style.background = "rgba(255,255,255,0.06)";
          setTip(false);
        }}
        onClick={onClick}
      >
        {icon && <span className="text-sm leading-none text-neutrals-medium">{icon}</span>}
        {label}
      </div>

      <AnimatePresence>
        {tip && tooltipText && (
          <motion.div
            className="absolute bottom-full mb-2 left-1/2 -translate-x-1/2 z-50 pointer-events-none"
            style={{ width: 230 }}
            initial={{ opacity: 0, y: 4 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: 4 }}
            transition={{ duration: 0.14 }}
          >
            <div
              className="rounded-xl p-3 text-center"
              style={{
                background: "rgba(14,14,14,0.97)",
                backdropFilter: "blur(12px)",
                WebkitBackdropFilter: "blur(12px)",
                border: "1px solid rgba(255,255,255,0.09)",
              }}
            >
              <p className="font-sans font-semibold text-white text-[11px] mb-1">{label}</p>
              <p className="font-sans font-light text-neutrals-light text-[11px] leading-relaxed">{tooltipText}</p>
            </div>
            <div className="flex justify-center">
              <div className="w-2 h-2 rotate-45 -mt-[5px]"
                style={{ background: "rgba(14,14,14,0.97)", border: "1px solid rgba(255,255,255,0.09)", borderTop: "none", borderLeft: "none" }} />
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

// ── External source node (NFS / S3) ───────────────────────────────────────────

function SourceNode({
  icon, label, sublabel, onClick,
}: {
  icon: React.ReactNode;
  label: string;
  sublabel: string;
  onClick?: () => void;
}) {
  return (
    <div
      className="flex flex-col items-center justify-center gap-1 rounded-xl px-3 py-3 shrink-0 w-[88px] transition-all duration-200"
      style={{
        background: "#0e0e0e",
        border: "1px dashed #474747",
        cursor: onClick ? "pointer" : "default",
      }}
      onMouseEnter={(e) => { if (onClick) (e.currentTarget as HTMLElement).style.borderColor = "#F2561D"; }}
      onMouseLeave={(e) => { (e.currentTarget as HTMLElement).style.borderColor = "#474747"; }}
      onClick={onClick}
    >
      <span className="text-neutrals-medium">{icon}</span>
      <span className="font-sans font-semibold text-[11px] text-neutrals-light text-center leading-none">{label}</span>
      <span className="font-sans text-[9px] text-neutrals-dark text-center leading-snug">{sublabel}</span>
    </div>
  );
}

// ── Spacer matching SourceNode width ──────────────────────────────────────────

function SourceSpacer() {
  return <div className="shrink-0 w-[88px]" />;
}

// ── Domains ───────────────────────────────────────────────────────────────────

const DOMAINS_TOP: Domain[] = [
  { id: "Fraud",     label: "Fraud & Risk",    tables: ["Risk Scores", "Transactions", "Alert Rules"], interactive: true },
  { id: "Payments",  label: "Payments",         tables: ["Payments",    "Settlements",  "FX Rates"] },
  { id: "Customers", label: "Customer 360",     tables: ["Profiles",    "KYC / AML",    "Identity"] },
  { id: "Finance",   label: "Finance",          tables: ["Accounts",    "P & L",        "Positions"] },
];

const DOMAINS_BOTTOM: Domain[] = [
  { id: "Compliance", label: "Compliance",      tables: ["Audit Trail", "Filings",    "Controls"] },
  { id: "Operations", label: "Operations",      tables: ["Incidents",   "SLAs",       "Telemetry"] },
  { id: "Marketing",  label: "Marketing & CX",  tables: ["Segments",    "Campaigns",  "Attribution"] },
  { id: "DataSci",    label: "Data Science",    tables: ["Features",    "Models",     "Experiments"] },
];

// ── Main component ────────────────────────────────────────────────────────────

export interface MeshDiagramProps {
  onRegionClick: (id: string) => void;
  children?: React.ReactNode;
}

export default function MeshDiagram({ onRegionClick, children }: MeshDiagramProps) {
  const [hovered, setHovered] = useState<string | null>(null);

  return (
    <div className="relative w-full h-full flex flex-col bg-neutrals-deep overflow-hidden px-4 py-3 gap-2">

      {/* ── Top row: external sources + top-tier domains ── */}
      <div className="flex-1 flex items-center gap-3 min-h-0">
        <SourceNode
          icon={<RiHardDriveLine size={20} />}
          label="NFS / POSIX"
          sublabel="File Sources"
          onClick={() => onRegionClick("NFS")}
        />
        {DOMAINS_TOP.map((d, i) => (
          <motion.div
            key={d.id}
            className="flex-1 min-w-0"
            initial={{ opacity: 0, y: 16 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, delay: i * 0.07, ease: [0.22, 1, 0.36, 1] }}
          >
            <DomainBox {...d} onHover={setHovered} onClick={onRegionClick} />
          </motion.div>
        ))}
        <SourceNode
          icon={<RiCloudLine size={20} />}
          label="S3 / Object"
          sublabel="Object Store"
          onClick={() => onRegionClick("S3")}
        />
      </div>

      {/* ── Governance / platform bar ── */}
      <motion.div
        className="flex items-center gap-1.5 px-4 py-2.5 rounded-xl flex-wrap shrink-0"
        style={{
          background: "#111111",
          border: "1px solid rgba(255,255,255,0.08)",
          borderLeft: "3px solid #F2561D",
        }}
        initial={{ opacity: 0, scaleX: 0.97 }}
        animate={{ opacity: 1, scaleX: 1 }}
        transition={{ duration: 0.5, delay: 0.32, ease: [0.22, 1, 0.36, 1] }}
      >
        {/* Identity & access */}
        <GovTool icon={<RiUserLine size={13} />}         label="Auth & SSO"     tooltipText={GOV_INFO["Auth & SSO"]}     onClick={() => onRegionClick("IAM")} />
        <GovTool icon={<RiShieldCheckLine size={13} />}  label="Access Control" tooltipText={GOV_INFO["Access Control"]} onClick={() => onRegionClick("IAM")} />
        <div className="w-px h-4 mx-0.5 shrink-0" style={{ background: "#333" }} />
        {/* Security & ops */}
        <GovTool icon={<RiFileSearchLine size={13} />}   label="Log Analytics"  tooltipText={GOV_INFO["Log Analytics"]} />
        <GovTool icon={<RiKeyLine size={13} />}          label="Key Management" tooltipText={GOV_INFO["Key Management"]} />
        {/* Centre label */}
        <div className="flex-1 flex items-center justify-center px-2">
          <span className="font-sans font-semibold text-[12px] text-white uppercase tracking-[0.22em] whitespace-nowrap">
            Data Fabric · Platform
          </span>
        </div>
        {/* Observability & governance */}
        <GovTool icon={<RiBarChartLine size={13} />}           label="Observability"  tooltipText={GOV_INFO["Observability"]} />
        <div className="w-px h-4 mx-0.5 shrink-0" style={{ background: "#333" }} />
        <GovTool icon={<RiCheckboxMultipleLine size={13} />}   label="Data Quality"   tooltipText={GOV_INFO["Data Quality"]}   onClick={() => onRegionClick("Policies")} />
        <GovTool icon={<RiBookOpenLine size={13} />}           label="Data Catalogue" tooltipText={GOV_INFO["Data Catalogue"]} onClick={() => onRegionClick("Catalogue")} />
      </motion.div>

      {/* ── Bottom row: lower-tier domains + spacers aligned to top sources ── */}
      <div className="flex-1 flex items-center gap-3 min-h-0">
        <SourceSpacer />
        {DOMAINS_BOTTOM.map((d, i) => (
          <motion.div
            key={d.id}
            className="flex-1 min-w-0"
            initial={{ opacity: 0, y: -16 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, delay: 0.38 + i * 0.07, ease: [0.22, 1, 0.36, 1] }}
          >
            <DomainBox {...d} onHover={setHovered} onClick={onRegionClick} />
          </motion.div>
        ))}
        <SourceSpacer />
      </div>

      {/* Domain hover tooltip */}
      <AnimatePresence>
        {hovered && <DomainTooltip key={hovered} id={hovered} />}
      </AnimatePresence>

      {children}
    </div>
  );
}
