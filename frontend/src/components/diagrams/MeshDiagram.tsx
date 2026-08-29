"use client";

import Link from "next/link";
import {
  RiShieldCheckLine, RiArrowRightLine, RiLock2Line, RiGlobalLine,
  RiDatabase2Line, RiExchangeLine, RiStackLine, RiRobot2Line, RiHardDrive2Line,
} from "@remixicon/react";
import { cx } from "@/lib/cx";

/**
 * Layered view: business domains on top, the fabric platform beneath them.
 *
 * Built with flex rather than SVG — it is a layered box diagram, so HTML gives
 * better text wrapping, real focus handling and screen-reader order for free.
 */

interface Domain {
  name: string;
  product: string;
  live?: boolean;
}

const DOMAINS: Domain[] = [
  { name: "Fraud & Risk", product: "Flagged transactions", live: true },
  { name: "Retail Banking", product: "Account activity" },
  { name: "Payments", product: "Settlement events" },
  { name: "Customer 360", product: "Unified profiles" },
  { name: "Lending", product: "Credit exposure" },
  { name: "Compliance", product: "Audit trail" },
  { name: "Treasury", product: "Liquidity positions" },
  { name: "Marketing", product: "Segment models" },
];

const CAPABILITIES = [
  { icon: RiGlobalLine,     label: "Global Namespace", detail: "One POSIX path across the cluster" },
  { icon: RiExchangeLine,   label: "Streams",          detail: "Kafka-compatible, in-fabric" },
  { icon: RiDatabase2Line,  label: "DocumentDB",       detail: "JSON tables via OJAI" },
  { icon: RiStackLine,      label: "Iceberg + Delta",  detail: "Open table formats" },
  { icon: RiHardDrive2Line, label: "S3 Object Store",  detail: "Native object access" },
  { icon: RiRobot2Line,     label: "MCP",              detail: "Agent-callable fabric tools" },
];

export default function MeshDiagram({ className }: { className?: string }) {
  return (
    <div className={cx("flex flex-col gap-3", className)}>
      {/* Domains */}
      <div>
        <div className="flex items-baseline justify-between mb-2">
          <span className="text-[11px] font-semibold uppercase tracking-[0.14em] text-subtle">
            Data domains
          </span>
          <span className="text-[11px] text-subtle">
            Each owns its data products
          </span>
        </div>

        <div className="grid gap-2 grid-cols-2 sm:grid-cols-3 lg:grid-cols-4">
          {DOMAINS.map((d) =>
            d.live ? (
              <Link
                key={d.name}
                href="/pipeline"
                className="group relative flex flex-col gap-0.5 p-3 rounded-lg text-left
                           bg-accent-soft border border-accent/40
                           hover:border-accent transition-colors"
              >
                <span className="flex items-center gap-1.5 text-[13px] font-semibold text-accent-text">
                  <RiShieldCheckLine size={14} />
                  {d.name}
                </span>
                <span className="text-[11px] text-muted">{d.product}</span>
                <span className="flex items-center gap-1 text-[11px] font-medium text-accent-text mt-1">
                  Open the live pipeline
                  <RiArrowRightLine
                    size={12}
                    className="transition-transform group-hover:translate-x-0.5"
                  />
                </span>
              </Link>
            ) : (
              <div
                key={d.name}
                title="Illustrative — this demo implements the Fraud & Risk domain"
                className="flex flex-col gap-0.5 p-3 rounded-lg border border-dashed border-border
                           bg-surface-sunk/60"
              >
                <span className="text-[13px] font-medium text-muted">{d.name}</span>
                <span className="text-[11px] text-subtle">{d.product}</span>
              </div>
            ),
          )}
        </div>
      </div>

      {/* Connector */}
      <div className="flex items-center gap-3 px-1" aria-hidden>
        <div className="h-px flex-1 bg-border" />
        <span className="text-[10px] uppercase tracking-[0.16em] text-subtle">
          all served by one platform
        </span>
        <div className="h-px flex-1 bg-border" />
      </div>

      {/* Platform */}
      <div className="rounded-lg border border-border bg-surface p-3">
        <div className="flex items-center gap-2 mb-3">
          <span className="text-[13px] font-semibold text-text">HPE Ezmeral Data Fabric</span>
          <span className="flex items-center gap-1 text-[11px] text-muted">
            <RiLock2Line size={12} />
            governed access, one security model
          </span>
        </div>

        <div className="grid gap-2 grid-cols-2 sm:grid-cols-3 lg:grid-cols-6">
          {CAPABILITIES.map(({ icon: Icon, label, detail }) => (
            <div
              key={label}
              className="flex flex-col gap-1 p-2.5 rounded-md bg-surface-sunk border border-border"
            >
              <span className="flex items-center gap-1.5 text-[12px] font-medium text-text">
                <Icon size={13} className="text-accent shrink-0" />
                {label}
              </span>
              <span className="text-[10.5px] text-subtle leading-snug">{detail}</span>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}
