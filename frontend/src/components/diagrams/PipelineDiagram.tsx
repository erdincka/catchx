"use client";

import React from "react";
import {
  RiFileTextLine, RiBroadcastLine, RiBracesLine, RiStackLine,
  RiTriangleLine, RiShieldUserLine, RiShieldFlashLine,
  type RemixiconComponentType,
} from "@remixicon/react";
import type { Metrics, StepId } from "@/lib/pipeline";
import { cx } from "@/lib/cx";

/**
 * The medallion pipeline, drawn from live metrics.
 *
 * Colours come from CSS variables so the diagram follows the theme, and node
 * counts come from the metrics poll so it shows the actual state of the
 * cluster rather than a static illustration.
 */

const W = 900;
const H = 400;

const COL_X = [70, 245, 420, 595, 770];
const NODE_W = 128;
const NODE_H = 56;

type TierKey = "source" | "stream" | "bronze" | "silver" | "gold";

/* What kind of thing each node holds. The icon says the storage format at a
   glance — a CSV on NFS, a Kafka topic, a JSON document table, an Iceberg
   table, a Delta table — which is the part of the story the labels alone
   under-sell. */
type Kind = "file" | "stream" | "documentdb" | "iceberg" | "delta" | "profile" | "flagged";

const KIND_ICON: Record<Kind, RemixiconComponentType> = {
  file:       RiFileTextLine,
  stream:     RiBroadcastLine,
  documentdb: RiBracesLine,
  iceberg:    RiStackLine,
  delta:      RiTriangleLine,
  profile:    RiShieldUserLine,
  flagged:    RiShieldFlashLine,
};

const KIND_TITLE: Record<Kind, string> = {
  file:       "CSV file in the global namespace",
  stream:     "Fabric stream, Kafka API",
  documentdb: "DocumentDB JSON table (OJAI)",
  iceberg:    "Apache Iceberg table",
  delta:      "Delta Lake table",
  profile:    "Derived customer risk profiles",
  flagged:    "Transactions flagged as suspected fraud",
};

interface Node {
  id: string;
  col: number;
  /** Vertical slot within the column. */
  row: number;
  rows: number;
  label: string;
  sublabel: string;
  tier: TierKey;
  kind: Kind;
  metric?: string;
  /** Peek target, when the node holds inspectable data. */
  peek?: { tier: string; table: string; iceberg?: boolean; fraudOnly?: boolean };
}

const NODES: Node[] = [
  { id: "src-cust", col: 0, row: 0, rows: 2, label: "customers.csv", sublabel: "NFS", tier: "source", kind: "file", metric: "source_customers", peek: { tier: "preview", table: "customers" } },
  { id: "src-txn",  col: 0, row: 1, rows: 2, label: "transactions.csv", sublabel: "NFS", tier: "source", kind: "file", metric: "source_transactions", peek: { tier: "preview", table: "transactions" } },

  { id: "stream",   col: 1, row: 0, rows: 1, label: "incoming", sublabel: "Stream · Kafka API", tier: "stream", kind: "stream", metric: "transactions_ingested" },

  { id: "bz-txn",   col: 2, row: 0, rows: 2, label: "transactions", sublabel: "DocumentDB", tier: "bronze", kind: "documentdb", metric: "bronze_transactions", peek: { tier: "bronze", table: "transactions" } },
  { id: "bz-cust",  col: 2, row: 1, rows: 2, label: "customers", sublabel: "Iceberg", tier: "bronze", kind: "iceberg", metric: "bronze_customers", peek: { tier: "bronze", table: "customers", iceberg: true } },

  { id: "sv-txn",   col: 3, row: 0, rows: 3, label: "transactions", sublabel: "DocumentDB", tier: "silver", kind: "documentdb", metric: "silver_transactions", peek: { tier: "silver", table: "transactions" } },
  { id: "sv-cust",  col: 3, row: 1, rows: 3, label: "customers", sublabel: "DocumentDB", tier: "silver", kind: "documentdb", metric: "silver_customers", peek: { tier: "silver", table: "customers" } },
  { id: "sv-prof",  col: 3, row: 2, rows: 3, label: "profiles", sublabel: "risk scores", tier: "silver", kind: "profile", metric: "silver_profiles", peek: { tier: "silver", table: "profiles" } },

  { id: "gd-cust",  col: 4, row: 0, rows: 3, label: "customers", sublabel: "Delta Lake", tier: "gold", kind: "delta", metric: "gold_customers", peek: { tier: "gold", table: "customers" } },
  { id: "gd-txn",   col: 4, row: 1, rows: 3, label: "transactions", sublabel: "Delta Lake", tier: "gold", kind: "delta", metric: "gold_transactions", peek: { tier: "gold", table: "transactions" } },
    // Same Delta table as gd-txn, filtered to fraud == true: gold transactions
  // carry a flag rather than living in two tables.
  { id: "gd-fraud", col: 4, row: 2, rows: 3, label: "flagged", sublabel: "suspected fraud", tier: "gold", kind: "flagged", metric: "gold_fraud", peek: { tier: "gold", table: "transactions", fraudOnly: true } },
];

interface Edge { from: string; to: string; step: StepId }

const EDGES: Edge[] = [
  { from: "src-txn",  to: "stream",   step: "publish" },
  { from: "stream",   to: "bz-txn",   step: "ingest" },
  { from: "src-cust", to: "bz-cust",  step: "ingest" },
  { from: "bz-txn",   to: "sv-txn",   step: "refine" },
  { from: "bz-cust",  to: "sv-cust",  step: "refine" },
  { from: "bz-txn",   to: "sv-prof",  step: "refine" },
  { from: "sv-cust",  to: "gd-cust",  step: "consolidate" },
  { from: "sv-prof",  to: "gd-cust",  step: "consolidate" },
  { from: "sv-txn",   to: "gd-txn",   step: "consolidate" },
  { from: "bz-txn",   to: "gd-fraud", step: "detect" },
];

const TIER_VAR: Record<TierKey, string> = {
  source: "var(--text-subtle)",
  stream: "var(--info)",
  bronze: "var(--bronze)",
  silver: "var(--silver)",
  gold:   "var(--gold)",
};

const COLUMN_LABELS = ["Source", "Stream", "Bronze", "Silver", "Gold"];

function nodeBox(node: Node) {
  const x = COL_X[node.col];
  const span = H - 96;
  const slot = span / node.rows;
  const y = 70 + slot * node.row + (slot - NODE_H) / 2;
  return { x, y, cx: x + NODE_W / 2, cy: y + NODE_H / 2 };
}

const BY_ID = new Map(NODES.map((n) => [n.id, n]));

function edgePath(from: Node, to: Node): string {
  const a = nodeBox(from);
  const b = nodeBox(to);
  const x1 = a.x + NODE_W;
  const x2 = b.x;
  const mid = x1 + (x2 - x1) / 2;
  return `M ${x1} ${a.cy} C ${mid} ${a.cy}, ${mid} ${b.cy}, ${x2} ${b.cy}`;
}

function displayValue(node: Node, metrics: Metrics): string {
  if (!node.metric) return "";
  const v = metrics[node.metric];
  if (typeof v === "boolean") return v ? "ready" : "—";
  if (typeof v === "number") return v > 0 ? v.toLocaleString() : "—";
  return "—";
}

function hasData(node: Node, metrics: Metrics): boolean {
  if (!node.metric) return false;
  const v = metrics[node.metric];
  return typeof v === "boolean" ? v : typeof v === "number" && v > 0;
}

export default function PipelineDiagram({
  metrics,
  activeStep,
  onPeek,
  className,
}: {
  metrics: Metrics;
  /** Step currently running — its edges animate. */
  activeStep: StepId | null;
  onPeek?: (peek: NonNullable<Node["peek"]>, label: string) => void;
  className?: string;
}) {
  return (
    <div className={cx("w-full overflow-x-auto", className)}>
      <svg
        viewBox={`0 0 ${W} ${H}`}
        className="w-full min-w-[720px] h-auto"
        role="img"
        aria-label="Medallion pipeline from source files through stream, bronze, silver and gold tiers"
      >
        {/* Column headers */}
        {COLUMN_LABELS.map((label, i) => (
          <g key={label}>
            <text
              x={COL_X[i] + NODE_W / 2}
              y={30}
              textAnchor="middle"
              className="text-[11px] font-semibold uppercase"
              style={{ fill: "var(--text-subtle)", letterSpacing: "0.14em" }}
            >
              {label}
            </text>
            <line
              x1={COL_X[i]} y1={44} x2={COL_X[i] + NODE_W} y2={44}
              stroke={i >= 2 ? TIER_VAR[["", "", "bronze", "silver", "gold"][i] as TierKey] : "var(--border)"}
              strokeWidth={2}
              opacity={i >= 2 ? 0.7 : 1}
            />
          </g>
        ))}

        {/* Edges first so nodes paint over them */}
        <g fill="none">
          {EDGES.map((e) => {
            const from = BY_ID.get(e.from)!;
            const to = BY_ID.get(e.to)!;
            const flowing = activeStep === e.step;
            const carried = hasData(to, metrics);
            return (
              <path
                key={`${e.from}-${e.to}`}
                d={edgePath(from, to)}
                stroke={flowing ? "var(--accent)" : carried ? TIER_VAR[to.tier] : "var(--border-strong)"}
                strokeWidth={flowing ? 2 : 1.5}
                opacity={flowing ? 1 : carried ? 0.55 : 0.35}
                className={flowing ? "flowing" : undefined}
              />
            );
          })}
        </g>

        {/* Nodes */}
        {NODES.map((node) => {
          const { x, y } = nodeBox(node);
          const filled = hasData(node, metrics);
          const tint = TIER_VAR[node.tier];
          const clickable = Boolean(node.peek && onPeek && filled);
          const Icon = KIND_ICON[node.kind];

          return (
            <g
              key={node.id}
              transform={`translate(${x},${y})`}
              className={clickable ? "cursor-pointer" : undefined}
              onClick={clickable ? () => onPeek!(node.peek!, `${node.sublabel} · ${node.label}`) : undefined}
              role={clickable ? "button" : undefined}
              tabIndex={clickable ? 0 : undefined}
              onKeyDown={
                clickable
                  ? (e) => {
                      if (e.key === "Enter" || e.key === " ") {
                        e.preventDefault();
                        onPeek!(node.peek!, `${node.sublabel} · ${node.label}`);
                      }
                    }
                  : undefined
              }
            >
              <rect
                width={NODE_W}
                height={NODE_H}
                rx={8}
                fill="var(--surface)"
                stroke={filled ? tint : "var(--border)"}
                strokeWidth={filled ? 1.5 : 1}
              />
              {/* Tier stripe */}
              <rect width={3} height={NODE_H} rx={1.5} fill={tint} opacity={filled ? 1 : 0.3} />

              {/* Record-type icon. A nested <svg> is valid inside SVG, so the
                  same icon set as the rest of the UI can be reused here. */}
              <g
                transform="translate(11, 9)"
                style={{ color: filled ? tint : "var(--text-subtle)" }}
              >
                <Icon size={13} />
                <title>{KIND_TITLE[node.kind]}</title>
              </g>

              <text
                x={30} y={19}
                className="text-[11.5px] font-medium"
                style={{ fill: "var(--text)" }}
              >
                {node.label}
              </text>
              <text
                x={12} y={34}
                className="text-[9.5px]"
                style={{ fill: "var(--text-subtle)" }}
              >
                {node.sublabel}
              </text>
              <text
                x={12} y={47}
                className="text-[12px] font-mono"
                style={{ fill: filled ? tint : "var(--text-subtle)" }}
              >
                {displayValue(node, metrics)}
              </text>

              {clickable && (
                <title>{`Inspect ${node.sublabel} ${node.label}`}</title>
              )}
            </g>
          );
        })}
      </svg>
    </div>
  );
}
