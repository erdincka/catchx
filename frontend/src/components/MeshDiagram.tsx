"use client";

import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { SiSplunk, SiGrafana, SiVault } from "react-icons/si";
import { FaAws } from "react-icons/fa";
import { MdStorage, MdDeviceHub } from "react-icons/md";
import { NexusSectionDivider } from "@/components/nexus-core-components";

// ── Domain descriptions for hover tooltips ────────────────────────────────────

const DOMAIN_INFO: Record<string, { title: string; description: string }> = {
  Fraud: {
    title: "Fraud Detection & Prevention",
    description:
      "Autonomous domain managing real-time transaction monitoring, customer profiling, and ML-based anomaly detection. " +
      "Owns its full ingestion-to-exposure pipeline independently. Click to explore the live pipeline.",
  },
  Finance: {
    title: "Financial Analytics",
    description:
      "Payments processing, revenue reporting, and compliance data products — shared across the mesh as governed, " +
      "versioned data products consumed by Audit and Sales domains.",
  },
  Operations: {
    title: "Operational Intelligence",
    description:
      "IoT device event streams, equipment monitoring telemetry, and real-time operational metrics " +
      "ingested via MapR Streams and stored in DocumentDB for low-latency queries.",
  },
  HR: {
    title: "Human Resources",
    description:
      "Employee lifecycle management, payroll processing, and people analytics data products — " +
      "protected by fine-grained RBAC policies and stored with field-level encryption.",
  },
  Audit: {
    title: "Compliance & Audit",
    description:
      "Immutable audit trails stored in append-only volumes, regulatory reporting data products, " +
      "and policy enforcement logs consumed by legal and governance teams.",
  },
  Logistics: {
    title: "Supply Chain & Logistics",
    description:
      "Route optimisation data, merchant tracking, delivery performance metrics, and " +
      "real-time shipment status — joined across domains via Global Namespace.",
  },
  Sales: {
    title: "Sales & Marketing",
    description:
      "Transaction analytics, product performance tracking, and marketing attribution data products " +
      "aggregated into Delta Lake for BI reporting via standard JDBC/ODBC connections.",
  },
};

// ── Governance bar tool descriptions ─────────────────────────────────────────

const GOV_INFO: Record<string, string> = {
  ADaaS:               "Authentication Directory as a Service — centralised identity management for all mesh participants with SSO and MFA.",
  RBAC:                "Role-Based Access Control — fine-grained policy enforcement at volume, table, and field level across all domains.",
  Splunk:              "Centralised log aggregation, SIEM, and operational intelligence across all Data Fabric nodes and services.",
  Vault:               "HashiCorp Vault — secret management, encryption key lifecycle, and data protection at rest and in transit.",
  Grafana:             "Unified observability — cluster metrics, pipeline latency, and domain health dashboards in one pane of glass.",
  Policies:            "Data governance policies applied consistently at platform level — retention schedules, quality SLAs, data classification.",
  "Metadata Catalogue": "OpenMetadata — discover, understand, and govern all data products across the mesh with lineage and quality scores.",
};

// ── Inline SVG: database cylinder ────────────────────────────────────────────

function DbCylinder({
  x, y, w = 33, h = 42, color = "#F2561D", label = "",
}: {
  x: number; y: number; w?: number; h?: number; color?: string; label?: string;
}) {
  const rx = w / 2;
  const ry = Math.round(rx * 0.3);
  return (
    <g>
      <rect x={x} y={y + ry} width={w} height={h - ry * 2} rx={2} fill={color} />
      <ellipse cx={x + rx} cy={y + ry}     rx={rx} ry={ry} fill={color} />
      <ellipse cx={x + rx} cy={y + h - ry} rx={rx} ry={ry} fill={color} fillOpacity={0.65} />
      {label && (
        <text
          x={x + rx} y={y + h + 12}
          textAnchor="middle"
          fontSize={8}
          fill="#8C8C8C"
          fontFamily="system-ui,sans-serif"
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
  id, label, tables, interactive,
  onHover, onClick,
}: Domain & {
  onHover: (id: string | null) => void;
  onClick: (id: string) => void;
}) {
  const W = 31, H = 39;
  const svgW = 200;
  const n    = tables.length;
  const step = svgW / (n + 1);

  return (
    <div
      className={[
        "relative rounded-xl select-none flex-1 min-w-[140px] max-w-[230px] transition-all duration-200",
        interactive ? "cursor-pointer" : "cursor-default",
      ].join(" ")}
      style={{
        background: "#121212",
        border: interactive ? "2px solid #F2561D" : "2px solid rgba(255,255,255,0.55)",
        boxShadow: interactive ? "0 4px 24px rgba(242,86,29,0.14)" : "none",
      }}
      onClick={() => interactive && onClick(id)}
      onMouseEnter={() => onHover(id)}
      onMouseLeave={() => onHover(null)}
    >
      <svg
        width="100%"
        height={106}
        viewBox={`0 0 ${svgW} 106`}
        preserveAspectRatio="xMidYMid meet"
      >
        {tables.map((t, i) => (
          <DbCylinder
            key={t}
            x={step * (i + 1) - W / 2}
            y={10}
            w={W}
            h={H}
            color={DB_COLORS[i % DB_COLORS.length]}
            label={t}
          />
        ))}
      </svg>
      <div className="px-3 pb-2.5 flex items-center gap-1.5">
        <span className="font-sans font-bold text-sm text-white">{label}</span>
        {interactive && (
          <span className="w-2 h-2 rounded-full shrink-0 animate-pulse" style={{ background: "#F2561D" }} />
        )}
        {!interactive && (
          <span className="ml-auto font-sans text-[9px] text-neutrals-dark uppercase tracking-wider">Hover</span>
        )}
      </div>
    </div>
  );
}

// ── Domain tooltip card ───────────────────────────────────────────────────────

function DomainTooltip({ id }: { id: string }) {
  const info = DOMAIN_INFO[id];
  if (!info) return null;

  return (
    <motion.div
      className="absolute inset-0 flex items-center justify-center pointer-events-none z-50"
      initial={{ opacity: 0, scale: 0.95 }}
      animate={{ opacity: 1, scale: 1 }}
      exit={{ opacity: 0, scale: 0.95 }}
      transition={{ duration: 0.2, ease: "easeOut" }}
    >
      <div
        className="rounded-xl p-5 shadow-2xl max-w-sm text-center"
        style={{
          background: "rgba(18, 18, 18, 0.95)",
          backdropFilter: "blur(24px)",
          WebkitBackdropFilter: "blur(24px)",
          border: "1px solid rgba(255,255,255,0.12)",
        }}
      >
        <p className="font-sans font-semibold text-brand-vivid mb-1 text-[11px] uppercase tracking-[0.15em]">
          Data Domain
        </p>
        <p className="font-sans font-bold text-white text-base mb-2">{info.title}</p>
        <p className="font-sans font-light text-sm text-neutrals-light leading-relaxed">{info.description}</p>
        {DOMAIN_INFO[id]?.description.endsWith("pipeline.") && (
          <p className="mt-3 font-sans font-semibold text-[11px] text-brand-vivid uppercase tracking-wider">
            Click to explore →
          </p>
        )}
      </div>
    </motion.div>
  );
}

// ── Governance bar tool chip ──────────────────────────────────────────────────

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
        className={[
          "flex items-center gap-1.5 rounded px-2.5 py-1.5 whitespace-nowrap font-sans text-[11px] font-medium text-white transition-colors duration-200",
          onClick ? "cursor-pointer" : "cursor-default",
        ].join(" ")}
        style={{ background: "rgba(255,255,255,0.07)" }}
        onMouseEnter={(e) => {
          if (onClick) e.currentTarget.style.background = "rgba(242,86,29,0.18)";
          if (tooltipText) setTip(true);
        }}
        onMouseLeave={(e) => {
          e.currentTarget.style.background = "rgba(255,255,255,0.07)";
          setTip(false);
        }}
        onClick={onClick}
      >
        {icon && <span className="text-sm leading-none">{icon}</span>}
        {label}
      </div>

      <AnimatePresence>
        {tip && tooltipText && (
          <motion.div
            className="absolute bottom-full mb-2 left-1/2 -translate-x-1/2 z-50 pointer-events-none"
            style={{ width: 220 }}
            initial={{ opacity: 0, y: 4 }}
            animate={{ opacity: 1, y: 0 }}
            exit={{ opacity: 0, y: 4 }}
            transition={{ duration: 0.15 }}
          >
            <div
              className="rounded-lg p-3 text-center"
              style={{
                background: "rgba(18,18,18,0.96)",
                backdropFilter: "blur(12px)",
                WebkitBackdropFilter: "blur(12px)",
                border: "1px solid rgba(255,255,255,0.10)",
              }}
            >
              <p className="font-sans font-semibold text-white text-[11px] mb-1">{label}</p>
              <p className="font-sans font-light text-neutrals-light text-[11px] leading-relaxed">{tooltipText}</p>
            </div>
            {/* Arrow */}
            <div className="flex justify-center">
              <div className="w-2 h-2 rotate-45 -mt-1" style={{ background: "rgba(18,18,18,0.96)", border: "1px solid rgba(255,255,255,0.10)", borderTop: "none", borderLeft: "none" }} />
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

// ── Corner node ───────────────────────────────────────────────────────────────

function CornerNode({
  icon, label, sublabel, onClick,
}: {
  icon: React.ReactNode;
  label: string;
  sublabel?: string;
  onClick?: () => void;
}) {
  return (
    <div
      className={[
        "flex items-center gap-1.5 rounded-lg px-2.5 py-1.5 font-sans font-light text-xs text-neutrals-light transition-all duration-200",
        onClick ? "cursor-pointer hover:text-brand-vivid" : "",
      ].join(" ")}
      style={{ background: "#121212", border: "1px solid #474747" }}
      onMouseEnter={(e) => { if (onClick) (e.currentTarget as HTMLElement).style.borderColor = "#F2561D"; }}
      onMouseLeave={(e) => { (e.currentTarget as HTMLElement).style.borderColor = "#474747"; }}
      onClick={onClick}
    >
      {icon}
      <div>
        <div className="font-semibold leading-none">{label}</div>
        {sublabel && <div className="text-[9px] text-neutrals-dark mt-0.5">{sublabel}</div>}
      </div>
    </div>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

export interface MeshDiagramProps {
  onRegionClick: (id: string) => void;
  children?: React.ReactNode;
}

const DOMAINS_TOP: Domain[] = [
  { id: "Fraud",      label: "Fraud",      tables: ["Activities", "Customers", "Companies"], interactive: true },
  { id: "Finance",    label: "Finance",    tables: ["Payments",   "Customers", "Companies"] },
  { id: "Operations", label: "Operations", tables: ["Events",     "Devices"] },
  { id: "HR",         label: "HR",         tables: ["Payroll",    "People",   "Companies"] },
];

const DOMAINS_BOTTOM: Domain[] = [
  { id: "Audit",     label: "Audit",     tables: ["Audit",      "Reports"] },
  { id: "Logistics", label: "Logistics", tables: ["Activities", "RouteMng", "Merchants"] },
  { id: "Sales",     label: "Sales",     tables: ["Transactions","Products", "Marketing"] },
];

export default function MeshDiagram({ onRegionClick, children }: MeshDiagramProps) {
  const [hovered, setHovered] = useState<string | null>(null);

  return (
    <div className="relative w-full h-full flex flex-col bg-neutrals-deep overflow-hidden px-3 py-3 gap-2">

      {/* NFS — top-left */}
      <div className="absolute top-2 left-2 z-10">
        <CornerNode
          icon={<MdStorage size={15} />}
          label="NFS"
          sublabel="External Storage"
          onClick={() => onRegionClick("NFS")}
        />
      </div>

      {/* S3 — top-right */}
      <div className="absolute top-2 right-2 z-10">
        <CornerNode
          icon={<FaAws size={14} className="text-brand-soft" />}
          label="S3 / Minio"
          sublabel="Object Store"
          onClick={() => onRegionClick("S3")}
        />
      </div>

      {/* Top domain row — staggered entrance */}
      <div className="flex-1 flex items-center gap-3 px-16 min-h-0">
        {DOMAINS_TOP.map((d, i) => (
          <motion.div
            key={d.id}
            className="flex-1 min-w-[140px] max-w-[230px]"
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, delay: i * 0.08, ease: [0.22, 1, 0.36, 1] }}
          >
            <DomainBox {...d} onHover={setHovered} onClick={onRegionClick} />
          </motion.div>
        ))}
      </div>

      {/* Governance / centralised-services bar */}
      <motion.div
        className="flex items-center gap-2 px-4 py-2.5 rounded-xl flex-wrap shrink-0"
        style={{
          background: "#121212",
          border: "1px solid #474747",
          borderLeft: "3px solid #F2561D",
        }}
        initial={{ opacity: 0, scaleX: 0.96 }}
        animate={{ opacity: 1, scaleX: 1 }}
        transition={{ duration: 0.5, delay: 0.35, ease: [0.22, 1, 0.36, 1] }}
      >
        <GovTool label="ADaaS"  tooltipText={GOV_INFO["ADaaS"]}  onClick={() => onRegionClick("IAM")} />
        <GovTool label="RBAC"   tooltipText={GOV_INFO["RBAC"]}   onClick={() => onRegionClick("IAM")} />
        <div className="w-px h-4 mx-1" style={{ background: "#474747" }} />
        <GovTool icon={<SiSplunk />} label="Splunk" tooltipText={GOV_INFO["Splunk"]} />
        <GovTool icon={<SiVault />}  label="Vault"  tooltipText={GOV_INFO["Vault"]} />
        <div className="flex-1 text-center">
          <span className="font-sans font-medium text-sm text-white uppercase tracking-[0.18em]">
            Data Board · Governance
          </span>
        </div>
        <GovTool icon={<SiGrafana />} label="Grafana"            tooltipText={GOV_INFO["Grafana"]} />
        <div className="w-px h-4 mx-1" style={{ background: "#474747" }} />
        <GovTool label="Policies"            tooltipText={GOV_INFO["Policies"]}            onClick={() => onRegionClick("Policies")} />
        <GovTool label="Metadata Catalogue"  tooltipText={GOV_INFO["Metadata Catalogue"]}  onClick={() => onRegionClick("Catalogue")} />
      </motion.div>

      {/* Bottom domain row — staggered entrance */}
      <div className="flex-1 flex items-center justify-center gap-3 px-16 min-h-0">
        {DOMAINS_BOTTOM.map((d, i) => (
          <motion.div
            key={d.id}
            className="flex-1 min-w-[140px] max-w-[230px]"
            initial={{ opacity: 0, y: -20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.5, delay: 0.4 + i * 0.08, ease: [0.22, 1, 0.36, 1] }}
          >
            <DomainBox {...d} onHover={setHovered} onClick={onRegionClick} />
          </motion.div>
        ))}
      </div>

      {/* Edge — bottom-left */}
      <div className="absolute bottom-3 left-2 z-10">
        <CornerNode
          icon={<MdDeviceHub size={14} />}
          label="Edge"
          sublabel="IoT Gateway"
          onClick={() => onRegionClick("Edge")}
        />
      </div>

      {/* Domain hover tooltip */}
      <AnimatePresence>
        {hovered && <DomainTooltip key={hovered} id={hovered} />}
      </AnimatePresence>

      {children}
    </div>
  );
}
