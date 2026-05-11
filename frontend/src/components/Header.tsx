"use client";

import { useRouter } from "next/navigation";
import { useEffect, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { useCluster } from "@/contexts/ClusterContext";
import { useSettings } from "@/contexts/SettingsContext";
import { NexusGlobalNav } from "@/components/nexus-core-components";
import {
  RiCheckDoubleLine,
  RiErrorWarningLine,
  RiWifiOffLine,
} from "@remixicon/react";

interface HeaderProps {
  onConnectClick: () => void;
  onSettingsClick: () => void;
}

const NAV_ITEMS = [
  { id: "mesh",  label: "Enterprise Mesh" },
  { id: "fraud", label: "Fraud & Risk Domain" },
];

const ROUTE_MAP: Record<string, string> = {
  mesh:  "/",
  fraud: "/fraud",
};

export default function Header({ onConnectClick: _onConnectClick, onSettingsClick: _onSettingsClick }: HeaderProps) {
  const router = useRouter();
  const [mounted, setMounted] = useState(false);
  const { clusterInfo, host, user, pass } = useCluster();
  const { settings, isReady, services } = useSettings();

  useEffect(() => { setMounted(true); }, []);

  function openMCS() {
    const h = settings?.cluster_host || host;
    if (!h) return;
    const u = settings?.credentials.cluster_user || user;
    const p = settings?.credentials.cluster_pass || pass;
    window.open(`https://${u}:${p}@${h}:8443/app/mcs/`, "_blank");
  }

  const configuredHost = settings?.cluster_host || host;
  const clusterName = clusterInfo?.name ?? (configuredHost ? configuredHost : null);

  const servicesProbed = Object.keys(services).length > 0;
  const someServicesFailed = servicesProbed && Object.values(services).some((s) => s.status !== "good");

  const leftSlot = (
    <div className="flex items-center gap-3">
      <div>
        <div className="font-sans font-semibold text-xl text-white leading-none">NexMesh</div>
        <div className="font-sans font-light text-[10px] text-neutrals-medium leading-none mt-0.5 uppercase tracking-[0.2em]">
          Nexus Data Mesh
        </div>
      </div>
    </div>
  );

  const rightSlot = (
    <div className="flex items-center gap-4">

      {/* Readiness status — four states; mounted guard prevents SSR/client mismatch */}
      {mounted && (
        <AnimatePresence mode="wait">
          {isReady ? (
            <motion.button
              key="ready"
              onClick={openMCS}
              className="flex items-center gap-2 font-sans font-light text-xs tracking-wide group"
              title="Open Management Console"
              initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
            >
              <span className="w-2 h-2 rounded-full bg-status-good shrink-0 status-pulse-dot" />
              <span className="text-brand-contrast group-hover:text-white transition-colors duration-200">
                {clusterName ?? "Ready"}
              </span>
            </motion.button>
          ) : someServicesFailed ? (
            <motion.button
              key="degraded"
              onClick={() => router.push("/settings")}
              className="flex items-center gap-1.5 text-status-degraded text-xs font-sans font-light hover:text-white transition-colors duration-200"
              title="Some services unreachable — open Settings"
              initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
            >
              <RiErrorWarningLine size={14} />
              {clusterName ?? "Not ready"}
            </motion.button>
          ) : configuredHost ? (
            <motion.button
              key="configured"
              onClick={() => router.push("/settings")}
              className="flex items-center gap-1.5 text-neutrals-medium text-xs font-sans font-light hover:text-white transition-colors duration-200"
              title="Probes not run — open Settings to verify"
              initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
            >
              <RiCheckDoubleLine size={14} />
              {clusterName}
            </motion.button>
          ) : (
            <motion.button
              key="disconnected"
              onClick={() => router.push("/settings")}
              className="flex items-center gap-1.5 text-status-failed text-xs font-sans font-light hover:text-white transition-colors duration-200"
              title="No cluster configured — open Settings"
              initial={{ opacity: 0 }} animate={{ opacity: 1 }} exit={{ opacity: 0 }}
            >
              <RiWifiOffLine size={14} />
              Not configured
            </motion.button>
          )}
        </AnimatePresence>
      )}

    </div>
  );

  return (
    <NexusGlobalNav
      navItems={NAV_ITEMS}
      leftSlot={leftSlot}
      rightSlot={rightSlot}
      onItemClick={(id: string) => router.push(ROUTE_MAP[id] ?? "/")}
    />
  );
}

