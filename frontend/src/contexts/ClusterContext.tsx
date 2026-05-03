"use client";

import React, { createContext, useCallback, useContext, useEffect, useState } from "react";
import type { MetricKey } from "@/lib/constants";
import { MONITORING_METRICS } from "@/lib/constants";

export interface ClusterInfo {
  name: string;
  ip: string;
  version?: string;
}

export interface Settings {
  s3Server: string;
  nfsPath: string;
  s3AccessKey: string;
  s3SecretKey: string;
  dashboardUrl: string;
  catalogueUrl: string;
}

interface ClusterState {
  host: string;
  user: string;
  pass: string;
  clusterInfo: ClusterInfo | null;
  demoMode: boolean;
  monitorActive: boolean;
  metrics: Record<MetricKey, number>;
  settings: Settings;

  setHost: (v: string) => void;
  setUser: (v: string) => void;
  setPass: (v: string) => void;
  setClusterInfo: (v: ClusterInfo | null) => void;
  setDemoMode: (v: boolean) => void;
  setMonitorActive: (v: boolean) => void;
  setMetrics: (patch: Partial<Record<MetricKey, number>>) => void;
  setSettings: (patch: Partial<Settings>) => void;
}

const defaultMetrics = Object.fromEntries(
  MONITORING_METRICS.map((k) => [k, 0])
) as Record<MetricKey, number>;

const defaultSettings: Settings = {
  s3Server: "",
  nfsPath: "",
  s3AccessKey: "",
  s3SecretKey: "",
  dashboardUrl: "",
  catalogueUrl: "",
};

const ClusterContext = createContext<ClusterState | null>(null);

function load<T>(key: string, fallback: T): T {
  if (typeof window === "undefined") return fallback;
  try {
    const raw = sessionStorage.getItem(key);
    return raw ? (JSON.parse(raw) as T) : fallback;
  } catch {
    return fallback;
  }
}

function save(key: string, value: unknown) {
  if (typeof window !== "undefined") sessionStorage.setItem(key, JSON.stringify(value));
}

export function ClusterProvider({ children }: { children: React.ReactNode }) {
  const [host, _setHost] = useState(() => load("mapr_host", ""));
  const [user, _setUser] = useState(() => load("mapr_user", ""));
  const [pass, _setPass] = useState(() => load("mapr_pass", ""));
  const [clusterInfo, _setClusterInfo] = useState<ClusterInfo | null>(() =>
    load("cluster_info", null)
  );
  const [demoMode, _setDemoMode] = useState(() => load("demo_mode", false));
  const [monitorActive, setMonitorActive] = useState(false);
  const [metrics, _setMetrics] = useState<Record<MetricKey, number>>(defaultMetrics);
  const [settings, _setSettings] = useState<Settings>(() => load("settings", defaultSettings));

  const setHost = useCallback((v: string) => { _setHost(v); save("mapr_host", v); }, []);
  const setUser = useCallback((v: string) => { _setUser(v); save("mapr_user", v); }, []);
  const setPass = useCallback((v: string) => { _setPass(v); save("mapr_pass", v); }, []);
  const setClusterInfo = useCallback((v: ClusterInfo | null) => {
    _setClusterInfo(v);
    save("cluster_info", v);
  }, []);
  const setDemoMode = useCallback((v: boolean) => { _setDemoMode(v); save("demo_mode", v); }, []);
  const setMetrics = useCallback((patch: Partial<Record<MetricKey, number>>) => {
    _setMetrics((prev) => ({ ...prev, ...patch }));
  }, []);
  const setSettings = useCallback((patch: Partial<Settings>) => {
    _setSettings((prev) => { const next = { ...prev, ...patch }; save("settings", next); return next; });
  }, []);

  // Polling timer for monitoring
  useEffect(() => {
    if (!monitorActive || !host) return;
    const poll = async () => {
      try {
        const r = await fetch(`/api/monitoring/metrics`, {
          headers: { "X-Mapr-Host": host, "X-Mapr-User": user, "X-Mapr-Pass": pass },
        });
        if (!r.ok) return;
        const data = await r.json();
        const patch: Partial<Record<MetricKey, number>> = {};
        for (const k of MONITORING_METRICS) {
          if (k in data) patch[k] = data[k] as number;
        }
        _setMetrics((prev) => ({ ...prev, ...patch }));
      } catch {
        // silent
      }
    };
    poll();
    const id = setInterval(poll, 3000);
    return () => clearInterval(id);
  }, [monitorActive, host, user, pass]);

  return (
    <ClusterContext.Provider
      value={{
        host, user, pass, clusterInfo, demoMode, monitorActive, metrics, settings,
        setHost, setUser, setPass, setClusterInfo, setDemoMode,
        setMonitorActive, setMetrics, setSettings,
      }}
    >
      {children}
    </ClusterContext.Provider>
  );
}

export function useCluster(): ClusterState {
  const ctx = useContext(ClusterContext);
  if (!ctx) throw new Error("useCluster must be used within ClusterProvider");
  return ctx;
}
