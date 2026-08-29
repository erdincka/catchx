"use client";

import Link from "next/link";
import {
  RiArrowRightLine, RiSettings3Line, RiGitBranchLine, RiShieldCheckLine,
} from "@remixicon/react";
import AppShell from "@/components/AppShell";
import MeshDiagram from "@/components/diagrams/MeshDiagram";
import { Button, Card, StatusDot } from "@/components/ui";
import { useSettings } from "@/contexts/SettingsContext";

const FLOW = [
  { tier: "Source",  detail: "CSV written over NFS", colour: "var(--text-subtle)" },
  { tier: "Stream",  detail: "Kafka-compatible", colour: "var(--info)" },
  { tier: "Bronze",  detail: "DocumentDB + Iceberg", colour: "var(--bronze)" },
  { tier: "Silver",  detail: "Enriched, PII masked", colour: "var(--silver)" },
  { tier: "Gold",    detail: "Delta Lake product", colour: "var(--gold)" },
];

export default function OverviewPage() {
  const { configured, ready } = useSettings();

  return (
    <AppShell>
      <div className="max-w-[1200px] mx-auto px-4 py-6 flex flex-col gap-5">
        {/* Hero */}
        <div className="flex flex-wrap items-end justify-between gap-4">
          <div className="max-w-2xl">
            <h1 className="text-[22px] font-semibold tracking-tight text-text">
              A hybrid data mesh on HPE Data Fabric
            </h1>
            <p className="text-[13px] text-muted mt-1.5 leading-relaxed">
              CatchX implements one domain of a financial-services data mesh end to end:
              fraud detection over streaming and batch data, using the fabric&apos;s own
              streams, document store, table formats and global namespace. Nothing else
              is required — no external catalog, metrics stack or log pipeline.
            </p>
          </div>

          <div className="flex items-center gap-2">
            {configured ? (
              <Link href="/pipeline">
                <Button variant="primary" icon={<RiShieldCheckLine size={14} />}>
                  Open the pipeline
                </Button>
              </Link>
            ) : (
              <Link href="/setup">
                <Button variant="primary" icon={<RiSettings3Line size={14} />}>
                  Set up a cluster
                </Button>
              </Link>
            )}
          </div>
        </div>

        {/* Status strip */}
        <Card className="px-3 py-2.5 flex flex-wrap items-center gap-x-5 gap-y-2">
          <span className="flex items-center gap-2 text-[12px]">
            <StatusDot tone={ready ? "good" : configured ? "warn" : "bad"} pulse={ready} />
            <span className="text-muted">
              {ready ? "Cluster ready" : configured ? "Setup incomplete" : "No cluster configured"}
            </span>
          </span>
          <span className="h-4 w-px bg-border hidden sm:block" />
          <span className="flex items-center gap-1.5 text-[12px] text-muted">
            <RiGitBranchLine size={13} className="text-subtle" />
            Medallion architecture, six steps
          </span>
          {!configured && (
            <>
              <div className="flex-1" />
              <Link
                href="/setup"
                className="text-[12px] font-medium text-accent-text hover:underline flex items-center gap-1"
              >
                Configure now <RiArrowRightLine size={12} />
              </Link>
            </>
          )}
        </Card>

        {/* Flow summary */}
        <div>
          <h2 className="text-[11px] font-semibold uppercase tracking-[0.14em] text-subtle mb-2">
            How data moves
          </h2>
          <div className="grid gap-2 grid-cols-2 sm:grid-cols-3 lg:grid-cols-5">
            {FLOW.map((s, i) => (
              <div
                key={s.tier}
                className="relative p-3 rounded-lg border border-border bg-surface overflow-hidden"
              >
                <span
                  className="absolute left-0 top-0 bottom-0 w-[3px]"
                  style={{ background: s.colour }}
                  aria-hidden
                />
                <div className="text-[10px] font-mono text-subtle">{i + 1}</div>
                <div className="text-[13px] font-semibold text-text mt-0.5">{s.tier}</div>
                <div className="text-[11px] text-muted leading-snug mt-0.5">{s.detail}</div>
              </div>
            ))}
          </div>
        </div>

        {/* Mesh */}
        <MeshDiagram />
      </div>
    </AppShell>
  );
}
