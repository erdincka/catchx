"use client";

import { useRouter } from "next/navigation";
import { motion, AnimatePresence } from "framer-motion";
import { useCluster } from "@/contexts/ClusterContext";
import { NexusGlobalNav } from "@/components/nexus-core-components";
import { MonitoringTicker } from "@/components/MonitoringPanel";
import {
  RiLinkM,
  RiSettings3Line,
  RiWifiOffLine,
  RiPulseLine,
} from "@remixicon/react";

interface HeaderProps {
  onConnectClick: () => void;
  onSettingsClick: () => void;
}

const NAV_ITEMS = [
  { id: "mesh",  label: "Data Mesh" },
  { id: "fraud", label: "Fraud Domain" },
];

const ROUTE_MAP: Record<string, string> = {
  mesh:  "/",
  fraud: "/fraud",
};

export default function Header({ onConnectClick, onSettingsClick }: HeaderProps) {
  const router = useRouter();
  const {
    clusterInfo,
    demoMode, setDemoMode,
    monitorActive, setMonitorActive,
    host, user, pass,
  } = useCluster();

  function openMCS() {
    if (!host) return;
    window.open(`https://${user}:${pass}@${host}:8443/app/mcs/`, "_blank");
  }

  const leftSlot = (
    <div className="flex items-center gap-3">
      <div>
        <div className="font-serif text-[22px] text-white leading-none tracking-tight">
          NexMesh
        </div>
        <div className="font-sans font-light text-[10px] text-neutrals-medium leading-none mt-0.5 uppercase tracking-[0.2em]">
          Nexus Data Mesh
        </div>
      </div>
    </div>
  );

  const rightSlot = (
    <div className="flex items-center gap-4">

      {/* Live metric ticker — only when monitoring */}
      <AnimatePresence>
        {monitorActive && (
          <motion.div
            initial={{ opacity: 0, x: 10 }}
            animate={{ opacity: 1, x: 0 }}
            exit={{ opacity: 0, x: 10 }}
            transition={{ duration: 0.3, ease: [0.22, 1, 0.36, 1] }}
          >
            <MonitoringTicker />
          </motion.div>
        )}
      </AnimatePresence>

      {/* Toggles — only when cluster is connected */}
      <AnimatePresence>
        {clusterInfo && (
          <motion.div
            className="flex items-center gap-3"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.2 }}
          >
            <Toggle label="Live"    value={demoMode}      onChange={setDemoMode} />
            {demoMode && (
              <Toggle label="Monitor" value={monitorActive} onChange={setMonitorActive} />
            )}
          </motion.div>
        )}
      </AnimatePresence>

      {/* Cluster status */}
      <AnimatePresence mode="wait">
        {clusterInfo ? (
          <motion.button
            key="connected"
            onClick={openMCS}
            className="flex items-center gap-2 font-sans font-light text-xs tracking-wide group"
            title="Open Management Console"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
          >
            <span className="w-2 h-2 rounded-full bg-status-good shrink-0 status-pulse-dot" />
            <span className="text-brand-contrast group-hover:text-white transition-colors duration-200">
              {clusterInfo.name}
            </span>
          </motion.button>
        ) : (
          <motion.span
            key="disconnected"
            className="flex items-center gap-1.5 text-status-failed text-xs font-sans font-light"
            title="Cluster not connected"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
          >
            <RiWifiOffLine size={14} />
            Not connected
          </motion.span>
        )}
      </AnimatePresence>

      {/* Analytics icon — subtle indicator when monitoring is active */}
      {monitorActive && (
        <RiPulseLine size={16} className="text-brand-vivid animate-pulse" />
      )}

      <button
        onClick={onConnectClick}
        title="Connect to Data Fabric cluster"
        className="text-neutrals-light hover:text-brand-vivid transition-colors duration-200 p-1"
      >
        <RiLinkM size={20} />
      </button>
      <button
        onClick={onSettingsClick}
        title="Settings"
        className="text-neutrals-light hover:text-brand-vivid transition-colors duration-200 p-1"
      >
        <RiSettings3Line size={20} />
      </button>
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

function Toggle({
  label,
  value,
  onChange,
}: {
  label: string;
  value: boolean;
  onChange: (v: boolean) => void;
}) {
  return (
    <label className="flex items-center gap-2 cursor-pointer select-none">
      <span className="font-sans text-[11px] text-neutrals-medium uppercase tracking-[0.15em]">
        {label}
      </span>
      <div
        className="relative w-9 h-5 rounded-full transition-colors duration-200"
        style={{ background: value ? "#F2561D" : "#474747" }}
        onClick={() => onChange(!value)}
      >
        <div
          className="absolute top-0.5 w-4 h-4 bg-white rounded-full shadow transition-transform duration-200"
          style={{ transform: value ? "translateX(18px)" : "translateX(2px)" }}
        />
      </div>
    </label>
  );
}
