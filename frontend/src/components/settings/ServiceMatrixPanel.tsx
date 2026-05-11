"use client";

import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { RiArrowDownSLine, RiArrowUpSLine, RiRefreshLine } from "@remixicon/react";
import { StatusDot, type ServiceStatus, TealButton } from "@/components/nexus";
import { SERVICE_NAMES, SERVICE_META, type ServiceMatrix } from "@/lib/settings";
import { cx } from "@/lib/cx";

interface Props {
  services: ServiceMatrix;
  resolvedEndpoints: Record<string, string>;
  onTest: () => void;
  testing: boolean;
}

const ENDPOINT_KEY: Record<string, string> = {
  cluster:  "cluster_host",   // special — derived from settings
  s3:       "s3_endpoint",
  polaris:  "polaris_url",
  livy:     "livy_url",
  grafana:  "grafana_url",
  opentsdb: "opentsdb_url",
  fluentd:  "fluentd_host",
  mcp:      "mcp_server_url",
};

function StatusBadge({ status }: { status: ServiceStatus }) {
  const labels: Record<ServiceStatus, string> = {
    good:     "Reachable",
    degraded: "Degraded",
    failed:   "Unreachable",
    unknown:  "Not probed",
  };
  const colours: Record<ServiceStatus, string> = {
    good:     "text-status-good",
    degraded: "text-status-degraded",
    failed:   "text-status-failed",
    unknown:  "text-neutrals-medium",
  };
  return (
    <span className={cx("text-[11px] font-medium uppercase tracking-[0.12em]", colours[status])}>
      {labels[status]}
    </span>
  );
}

function ServiceRow({
  name,
  probe,
  url,
  portHint,
  idx,
}: {
  name: string;
  probe?: { status: ServiceStatus; latency_ms: number; detail: string; url: string };
  url: string;
  portHint: string;
  idx: number;
}) {
  const [expanded, setExpanded] = useState(false);
  const status: ServiceStatus = probe?.status ?? "unknown";
  const hasError = status === "failed" || status === "degraded";
  const probeUrl = probe?.url || url;

  return (
    <div
      className="rounded-lg overflow-hidden"
      style={{ background: idx % 2 === 0 ? "#000000" : "#0d0d0d" }}
    >
      {/* Main row */}
      <div
        className={cx(
          "flex items-center gap-3 px-4 py-3",
          (hasError || probe?.detail) && "cursor-pointer"
        )}
        onClick={() => (hasError || probe?.detail) && setExpanded((v) => !v)}
      >
        <StatusDot status={status} pulse={status === "good"} size={10} className="shrink-0" />

        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2">
            <span className="text-sm text-white">{SERVICE_META[name as keyof typeof SERVICE_META]?.label ?? name}</span>
            {probe && (
              <span className="text-[10px] text-neutrals-dark font-mono ml-auto shrink-0">
                {probe.latency_ms}ms
              </span>
            )}
          </div>
          <span className="text-[11px] text-neutrals-medium font-mono truncate block" title={probeUrl}>
            {probeUrl || portHint}
          </span>
        </div>

        <div className="flex items-center gap-2 shrink-0">
          <StatusBadge status={status} />
          {(hasError || probe?.detail) && (
            <span className="text-neutrals-dark">
              {expanded ? <RiArrowUpSLine size={14} /> : <RiArrowDownSLine size={14} />}
            </span>
          )}
        </div>
      </div>

      {/* Expanded detail */}
      <AnimatePresence>
        {expanded && probe && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.2 }}
            className="overflow-hidden"
          >
            <div className="px-4 pb-3 pt-0 flex flex-col gap-1.5">
              {probe.url && (
                <div className="flex gap-2 text-[11px]">
                  <span className="text-neutrals-dark uppercase tracking-wider shrink-0">Probed URL</span>
                  <span className="font-mono text-neutrals-light break-all">{probe.url}</span>
                </div>
              )}
              <div className="flex gap-2 text-[11px]">
                <span className="text-neutrals-dark uppercase tracking-wider shrink-0">Detail</span>
                <span
                  className={cx(
                    "font-mono break-all",
                    status === "failed" ? "text-status-failed" :
                    status === "degraded" ? "text-status-degraded" :
                    "text-neutrals-light"
                  )}
                >
                  {probe.detail || "—"}
                </span>
              </div>
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

export function ServiceMatrixPanel({ services, resolvedEndpoints, onTest, testing }: Props) {
  const hasResults = Object.keys(services).length > 0;
  const failCount = Object.values(services).filter((s) => s.status === "failed").length;
  const degradedCount = Object.values(services).filter((s) => s.status === "degraded").length;

  return (
    <div className="bg-[#121212] border border-neutrals-slate rounded-3xl p-6">
      <div className="flex items-center justify-between mb-4">
        <div>
          <h3 className="font-medium text-base text-white uppercase tracking-[0.15em]">
            Service reachability
          </h3>
          {hasResults && (
            <p className="text-[11px] text-neutrals-medium mt-0.5">
              {failCount > 0 ? `${failCount} unreachable` : degradedCount > 0 ? `${degradedCount} degraded` : "All services reachable"}
              {" · click a row to expand"}
            </p>
          )}
        </div>
        <TealButton size="sm" onClick={onTest} disabled={testing} className="flex items-center gap-1.5">
          <RiRefreshLine size={13} className={testing ? "animate-spin" : ""} />
          {testing ? "Probing…" : "Run probes"}
        </TealButton>
      </div>

      {!hasResults && (
        <p className="text-sm text-neutrals-medium text-center py-8">
          Click "Run probes" to check service reachability.
        </p>
      )}

      <div className="flex flex-col gap-1">
        {SERVICE_NAMES.map((name, idx) => {
          const epKey = ENDPOINT_KEY[name];
          const url = epKey === "cluster_host"
            ? (resolvedEndpoints["s3_endpoint"]?.split(":")[1]?.slice(2) ? `https://${resolvedEndpoints["s3_endpoint"]?.split(":")[1]?.slice(2)}:8443` : "")
            : (resolvedEndpoints[epKey] ?? "");
          const probe = services[name] as { status: ServiceStatus; latency_ms: number; detail: string; url: string } | undefined;
          return (
            <ServiceRow
              key={name}
              name={name}
              probe={probe}
              url={url}
              portHint={SERVICE_META[name]?.portHint ?? ""}
              idx={idx}
            />
          );
        })}
      </div>
    </div>
  );
}
