"use client";

import { useState } from "react";
import {
  SiApachenifi,
  SiApacheairflow,
  SiApachespark,
  SiDelta,
} from "react-icons/si";
import { FaWifi, FaFileAlt, FaTable, FaLayerGroup } from "react-icons/fa";
import { MdOutlineManageSearch } from "react-icons/md";
import { RiDatabaseLine } from "react-icons/ri";
import type { MetricKey } from "@/lib/constants";

// ── Metric badge ──────────────────────────────────────────────────────────────

function Badge({ value }: { value: number }) {
  if (!value) return null;
  return (
    <span
      className="absolute -top-2 -right-2 text-white text-[9px] font-mono font-bold rounded-full min-w-[18px] h-[18px] flex items-center justify-center px-1 shadow"
      style={{ background: "#F2561D" }}
    >
      {value > 9999 ? "9k+" : value}
    </span>
  );
}

// ── Action / Code buttons ─────────────────────────────────────────────────────

function ActionBtn({ label, onClick, disabled }: { label: string; onClick: (e: React.MouseEvent) => void; disabled?: boolean }) {
  return (
    <button
      onClick={(e) => { e.stopPropagation(); onClick(e); }}
      disabled={disabled}
      className="text-[9px] text-white rounded px-1.5 py-0.5 font-sans font-semibold leading-tight transition-colors duration-200 disabled:opacity-40 disabled:cursor-not-allowed"
      style={{ background: "#F2561D" }}
      onMouseEnter={(e) => { if (!disabled) (e.currentTarget.style.background = "#D9704A"); }}
      onMouseLeave={(e) => (e.currentTarget.style.background = "#F2561D")}
    >
      {label}
    </button>
  );
}

function CodeBtn({ onClick }: { onClick: (e: React.MouseEvent) => void }) {
  return (
    <button
      onClick={(e) => { e.stopPropagation(); onClick(e); }}
      className="text-[9px] text-white rounded px-1.5 py-0.5 font-sans font-mono leading-tight transition-colors duration-200"
      style={{ background: "#008A8C" }}
      onMouseEnter={(e) => (e.currentTarget.style.background = "#006E70")}
      onMouseLeave={(e) => (e.currentTarget.style.background = "#008A8C")}
      title="View source code"
    >
      {"</>"}
    </button>
  );
}

// ── Data node card ────────────────────────────────────────────────────────────

function DataNode({
  icon, label, badge, peekId, actionLabel, actionId, codeId, onAction, interactive,
}: {
  icon: React.ReactNode;
  label: string;
  badge?: number;
  peekId?: string;
  actionLabel?: string;
  actionId?: string;
  codeId?: string;
  onAction: (id: string) => void;
  interactive: boolean;
}) {
  const canPeek = !!(peekId && interactive && badge && badge > 0);
  return (
    <div
      className={[
        "relative flex-1 rounded-lg p-1.5 flex flex-col items-center justify-center gap-0.5 w-full transition-all duration-200",
        canPeek ? "cursor-pointer" : "",
      ].join(" ")}
      style={{
        background: "#121212",
        border: canPeek ? "1px solid #F2561D" : "1px solid #474747",
      }}
      onClick={() => canPeek && onAction(peekId!)}
      onMouseEnter={(e) => {
        if (canPeek)
          (e.currentTarget as HTMLElement).style.boxShadow = "0 0 10px rgba(242,86,29,0.25)";
      }}
      onMouseLeave={(e) => {
        (e.currentTarget as HTMLElement).style.boxShadow = "none";
      }}
    >
      {badge !== undefined && <Badge value={badge} />}
      <div className="text-xl leading-none py-0.5">{icon}</div>
      <div className="font-sans text-[10px] font-semibold text-center text-neutrals-light leading-tight">{label}</div>
      {(actionId || codeId) && interactive && (
        <div className="flex gap-1 mt-0.5 flex-wrap justify-center">
          {actionId && <ActionBtn label={actionLabel ?? "Run"} onClick={() => onAction(actionId)} />}
          {codeId && <CodeBtn onClick={() => onAction(codeId)} />}
        </div>
      )}
    </div>
  );
}

// ── Tier column ───────────────────────────────────────────────────────────────

function TierColumn({
  accentColor, header, footer, children, shrink,
}: {
  accentColor: string;
  header: string;
  footer: string;
  children: React.ReactNode;
  shrink?: boolean;
}) {
  return (
    <div
      className={["flex flex-col rounded-lg overflow-hidden min-w-0", shrink ? "w-[150px] shrink-0" : "flex-1"].join(" ")}
      style={{
        background: "#0a0a0a",
        border: `1px solid #474747`,
        borderTop: `2px solid ${accentColor}`,
      }}
    >
      <div
        className="text-center py-1 font-sans text-xs font-semibold uppercase tracking-wider"
        style={{ color: accentColor, borderBottom: "1px solid #2a2a2a" }}
      >
        {header}
      </div>
      <div className="flex-1 flex flex-col gap-2 p-2">{children}</div>
      <div
        className="text-center py-1 font-sans text-[9px] font-medium uppercase tracking-widest"
        style={{ color: "#474747", borderTop: "1px solid #1a1a1a" }}
      >
        {footer}
      </div>
    </div>
  );
}

// ── Animated flow arrow ───────────────────────────────────────────────────────

function FlowArrow({ label, active }: { label: string; active: boolean }) {
  return (
    <div className="flex flex-col items-center justify-center w-10 shrink-0 gap-0.5 self-stretch">
      {/* Top connector line */}
      <div className="flex-1 flex items-center justify-center">
        <div className="w-px flex-1" style={{ background: "linear-gradient(to bottom, transparent, #474747)" }} />
      </div>

      {/* Animated SVG arrow */}
      <div className="flex flex-col items-center gap-0.5 shrink-0">
        <svg width="28" height="22" viewBox="0 0 28 22" overflow="visible">
          {/* Arrow shaft */}
          <line
            x1="2" y1="11" x2="20" y2="11"
            stroke={active ? "#F2561D" : "#474747"}
            strokeWidth="1.5"
            strokeLinecap="round"
          />
          {/* Arrowhead */}
          <polyline
            points="14,5 21,11 14,17"
            fill="none"
            stroke={active ? "#F2561D" : "#474747"}
            strokeWidth="1.5"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
          {/* Animated travel dot — only when active */}
          {active && (
            <circle r="3" fill="#F2561D" opacity="0.85">
              <animateMotion
                dur="1.6s"
                repeatCount="indefinite"
                path="M2 11 L20 11"
                calcMode="linear"
              />
            </circle>
          )}
        </svg>
        <span className="font-sans text-[8px] text-neutrals-medium font-semibold uppercase tracking-wide whitespace-nowrap">
          {label}
        </span>
      </div>

      {/* Bottom connector line */}
      <div className="flex-1 flex items-center justify-center">
        <div className="w-px flex-1" style={{ background: "linear-gradient(to bottom, #474747, transparent)" }} />
      </div>
    </div>
  );
}

// ── Inline SVG: DocumentDB cylinders (2-stack) ───────────────────────────────

function DocDbIcon({ color = "#F2561D" }: { color?: string }) {
  return (
    <svg width={36} height={30} viewBox="0 0 36 30">
      {[0, 16].map((xOff) => {
        const rx = 8; const ry = 3;
        return (
          <g key={xOff}>
            <rect x={xOff + 1} y={ry + 1} width={16} height={18} rx={1} fill={color} />
            <ellipse cx={xOff + 9} cy={ry + 1} rx={rx} ry={ry} fill={color} />
            <ellipse cx={xOff + 9} cy={19 + 1} rx={rx} ry={ry} fill={color} fillOpacity={0.7} />
          </g>
        );
      })}
    </svg>
  );
}

// ── Inline SVG: Table grid ────────────────────────────────────────────────────

function TableIcon({ color = "#008A8C" }: { color?: string }) {
  const cols = 3; const rows = 3;
  const cw = 10; const rh = 7;
  return (
    <svg width={cols * cw + 2} height={rows * rh + 2} viewBox={`0 0 ${cols * cw + 2} ${rows * rh + 2}`}>
      {Array.from({ length: rows }, (_, r) =>
        Array.from({ length: cols }, (_, c) => (
          <rect
            key={`${r}-${c}`}
            x={c * cw + 1} y={r * rh + 1}
            width={cw - 1} height={rh - 1}
            fill={r === 0 ? color : "#1a1a1a"}
            stroke={color} strokeWidth={0.5} rx={1}
          />
        ))
      )}
    </svg>
  );
}

// ── Main component ────────────────────────────────────────────────────────────

export interface FraudDiagramProps {
  onAction: (id: string) => void;
  interactive: boolean;
  metrics: Record<MetricKey, number>;
  customersCreated?: boolean;
  transactionsCreated?: boolean;
}

export default function FraudDiagram({ onAction, interactive, metrics, customersCreated, transactionsCreated }: FraudDiagramProps) {
  return (
    <div className="w-full h-full flex flex-col gap-1 overflow-hidden">

      {/* Central Governance metadata bar */}
      <div
        className="flex items-center justify-center gap-4 py-1 px-4 rounded-lg shrink-0"
        style={{ background: "#121212", border: "1px solid #474747", borderLeft: "3px solid #D9704A" }}
      >
        <span className="font-sans font-bold text-xs text-brand-soft uppercase tracking-wider">Central Governance</span>
        {["Discovery", "Policy", "Lineage"].map((t, i) => (
          <span key={t} className="flex items-center gap-2 font-sans text-xs text-neutrals-medium">
            {i > 0 && <span style={{ color: "#474747" }}>|</span>}
            {t}
          </span>
        ))}
        {interactive && (
          <span className="ml-4 font-sans text-[9px] text-neutrals-dark italic">
            Click any data node to preview — action buttons run the pipeline step
          </span>
        )}
      </div>

      {/* Main diagram row */}
      <div className="flex-1 flex gap-1 min-h-0 max-h-[520px] my-auto">

        {/* ── Source section ──────────────────────────────────────────────── */}
        <div
          className="flex flex-col gap-2 rounded-lg p-2 shrink-0 w-[120px]"
          style={{ background: "#0d0d0d", border: "1px solid #474747", borderTop: "2px solid #8C8C8C" }}
        >
          <div className="font-sans text-[10px] text-neutrals-medium font-semibold text-center uppercase tracking-wider">
            Batch and Streaming
          </div>

          {/* Streaming / Transactions */}
          <div
            className="flex flex-col gap-1 rounded-lg p-1.5"
            style={{ background: "rgba(255,255,255,0.04)", border: "1px solid #2a2a2a" }}
          >
            <div className="font-sans text-[10px] text-neutrals-light font-medium">Transactions</div>
            <div className="flex gap-1.5 items-center justify-around">
              <div className="text-neutrals-medium flex flex-col items-center gap-0.5" title="Apache NiFi">
                <SiApachenifi size={16} />
                <span className="font-sans text-[8px]">NiFi</span>
              </div>
              <div className="text-neutrals-medium flex flex-col items-center gap-0.5">
                <SiApachespark size={16} />
                <span className="font-sans text-[8px]">Spark</span>
              </div>
              <div className="text-neutrals-medium flex flex-col items-center gap-0.5">
                <FaWifi size={14} />
                <span className="font-sans text-[8px]">Stream</span>
              </div>
            </div>
            {interactive && (
              <div className="flex gap-1 justify-center flex-wrap mt-0.5">
                <ActionBtn label="Create" onClick={() => onAction("CreateTransactions")} />
                <ActionBtn label="Publish" onClick={() => onAction("PublishTransactions")} disabled={!transactionsCreated} />
                <CodeBtn onClick={() => onAction("CodeTransactions")} />
              </div>
            )}
            {interactive && (
              <div className="flex gap-1 justify-center mt-0.5">
                <button
                  onClick={(e) => { e.stopPropagation(); if (transactionsCreated) onAction("PreviewTransactions"); }}
                  disabled={!transactionsCreated}
                  className="text-[9px] text-neutrals-medium rounded px-1.5 py-0.5 font-sans leading-tight transition-colors duration-200 hover:text-white disabled:opacity-40 disabled:cursor-not-allowed"
                  style={{ border: "1px solid #474747" }}
                  onMouseEnter={(e) => { if (transactionsCreated) (e.currentTarget.style.borderColor = "#F2561D"); }}
                  onMouseLeave={(e) => (e.currentTarget.style.borderColor = "#474747")}
                >
                  Preview
                </button>
              </div>
            )}
          </div>

          {/* Batch / Customers */}
          <div
            className="flex flex-col gap-1 rounded-lg p-1.5 mt-auto"
            style={{ background: "rgba(255,255,255,0.04)", border: "1px solid #2a2a2a" }}
          >
            <div className="font-sans text-[10px] text-neutrals-light font-medium">Customers</div>
            <div className="flex gap-1.5 items-center justify-around">
              <div className="text-neutrals-medium flex flex-col items-center gap-0.5">
                <FaFileAlt size={14} />
                <span className="font-sans text-[8px]">CSV</span>
              </div>
              <div className="text-neutrals-medium flex flex-col items-center gap-0.5" title="Apache Airflow">
                <SiApacheairflow size={16} />
                <span className="font-sans text-[8px]">Airflow</span>
              </div>
            </div>
            {interactive && (
              <div className="flex gap-1 justify-center flex-wrap">
                <ActionBtn label="Create" onClick={() => onAction("CreateCustomers")} />
                <CodeBtn onClick={() => onAction("CodeCustomers")} />
              </div>
            )}
            {interactive && (
              <div className="flex gap-1 justify-center">
                <button
                  onClick={(e) => { e.stopPropagation(); if (customersCreated) onAction("PreviewCustomers"); }}
                  disabled={!customersCreated}
                  className="text-[9px] text-neutrals-medium rounded px-1.5 py-0.5 font-sans leading-tight transition-colors duration-200 hover:text-white disabled:opacity-40 disabled:cursor-not-allowed"
                  style={{ border: "1px solid #474747" }}
                  onMouseEnter={(e) => { if (customersCreated) (e.currentTarget.style.borderColor = "#F2561D"); }}
                  onMouseLeave={(e) => (e.currentTarget.style.borderColor = "#474747")}
                >
                  Preview
                </button>
              </div>
            )}
          </div>
        </div>

        <FlowArrow label="Ingest" active={interactive} />

        {/* ── Bronze Tier ─────────────────────────────────────────────────── */}
        <TierColumn accentColor="#D9704A" header="Bronze Tier" footer="Warehouse">
          <DataNode
            icon={<FaLayerGroup size={14} className="text-neutrals-medium" />}
            label="Profile Builder"
            codeId="ProfileBuilderCode"
            onAction={onAction}
            interactive={interactive}
          />
          <DataNode
            icon={<DocDbIcon color="#F2561D" />}
            label="Transactions"
            badge={metrics.transactions_ingested}
            peekId="BronzeTransactions"
            actionLabel="Ingest"
            actionId="IngestTransactions"
            codeId="IngestTransactionsCode"
            onAction={onAction}
            interactive={interactive}
          />
          <DataNode
            icon={<FaLayerGroup size={20} className="text-brand-soft" />}
            label="Customers (Iceberg)"
            badge={metrics.bronze_customers}
            peekId="BronzeCustomers"
            actionLabel="Ingest"
            actionId="IngestCustomersIceberg"
            codeId="IngestCustomersIcebergCode"
            onAction={onAction}
            interactive={interactive}
          />
        </TierColumn>

        <FlowArrow label="Refine" active={interactive} />

        {/* ── Silver Tier ─────────────────────────────────────────────────── */}
        <TierColumn accentColor="#008A8C" header="Silver Tier" footer="Lakehouse">
          <DataNode
            icon={<TableIcon color="#008A8C" />}
            label="Profiles"
            badge={metrics.silver_profiles}
            peekId="SilverProfiles"
            codeId="ProfileBuilderCode"
            onAction={onAction}
            interactive={interactive}
          />
          <DataNode
            icon={<TableIcon color="#F2561D" />}
            label="Transactions"
            badge={metrics.silver_transactions}
            peekId="SilverTransactions"
            actionLabel="Refine"
            actionId="RefineTransactions"
            codeId="RefineTransactionsCode"
            onAction={onAction}
            interactive={interactive}
          />
          <DataNode
            icon={<FaTable size={18} className="text-brand-contrast" />}
            label="Customers"
            badge={metrics.silver_customers}
            peekId="SilverCustomers"
            actionLabel="Refine"
            actionId="RefineCustomers"
            codeId="RefineCustomersCode"
            onAction={onAction}
            interactive={interactive}
          />
        </TierColumn>

        <FlowArrow label="Aggregate" active={interactive} />

        {/* ── Gold Tier ───────────────────────────────────────────────────── */}
        <TierColumn accentColor="#F2561D" header="Gold Tier" footer="Lake" shrink>
          <DataNode
            icon={<MdOutlineManageSearch size={22} className="text-status-failed" />}
            label="Fraud Detection"
            badge={metrics.gold_fraud}
            actionLabel="Detect"
            actionId="CheckFraud"
            codeId="CheckFraudCode"
            onAction={onAction}
            interactive={interactive}
          />
          <DataNode
            icon={<SiDelta size={50} className="text-brand-soft" />}
            label="Data Lake (Delta)"
            badge={metrics.gold_customers}
            peekId="GoldCustomers"
            actionLabel="Consolidate"
            actionId="Consolidate"
            codeId="ConsolidateCode"
            onAction={onAction}
            interactive={interactive}
          />
          <div className="text-center font-sans text-[9px] text-neutrals-dark font-mono mt-auto">JDBC / ODBC</div>
        </TierColumn>

        {/* Flow arrow to reports */}
        <FlowArrow label="Expose" active={interactive} />

        {/* ── Reports section ─────────────────────────────────────────────── */}
        <div className="flex flex-col gap-2 shrink-0 w-[110px] justify-around py-2">
          <ReportCard
            icon={<RiDatabaseLine size={18} className="text-brand-contrast" />}
            title="Product Catalogue"
            subtitle="Data Catalogue"
            interactive={interactive}
            onClick={() => onAction("Catalogue")}
          />
          <ReportCard
            icon={<SiDelta size={40} className="text-brand-soft" />}
            title="Consumers"
            subtitle="Reports &amp; Dashboards"
            interactive={interactive}
            onClick={() => onAction("ReportView")}
          />
        </div>
      </div>

      {/* Bottom tier / pipeline labels */}
      <div className="flex items-center gap-1 shrink-0">
        <div className="w-[120px] shrink-0" />
        <div className="flex-1 flex rounded overflow-hidden text-center font-sans text-[9px] font-semibold tracking-wide h-4">
          <div className="flex-1 flex items-center justify-center" style={{ background: "rgba(217,112,74,0.15)", color: "#D9704A" }}>Warehouse</div>
          <div className="flex-1 flex items-center justify-center" style={{ background: "rgba(0,138,140,0.15)", color: "#008A8C" }}>Lakehouse</div>
          <div className="w-[150px] shrink-0 flex items-center justify-center" style={{ background: "rgba(242,86,29,0.15)", color: "#F2561D" }}>Lake</div>
        </div>
        <div className="w-[130px] shrink-0" />
      </div>
    </div>
  );
}

// ── Report card ───────────────────────────────────────────────────────────────

function ReportCard({
  icon, title, subtitle, interactive, onClick,
}: {
  icon: React.ReactNode;
  title: string;
  subtitle: string;
  interactive: boolean;
  onClick: () => void;
}) {
  return (
    <div
      className={[
        "rounded-lg p-2 flex flex-col items-center gap-1 transition-all duration-200",
        interactive ? "cursor-pointer" : "",
      ].join(" ")}
      style={{ background: "#121212", border: "1px solid #474747" }}
      onMouseEnter={(e) => { if (interactive) (e.currentTarget as HTMLElement).style.borderColor = "#F2561D"; }}
      onMouseLeave={(e) => { (e.currentTarget as HTMLElement).style.borderColor = "#474747"; }}
      onClick={() => interactive && onClick()}
    >
      {icon}
      <span className="font-sans font-semibold text-[10px] text-center text-white">{title}</span>
      <span
        className="font-sans font-light text-[8px] text-neutrals-medium text-center"
        dangerouslySetInnerHTML={{ __html: subtitle }}
      />
    </div>
  );
}
