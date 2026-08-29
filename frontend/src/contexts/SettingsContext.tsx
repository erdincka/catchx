"use client";

import React, {
  createContext, useCallback, useContext, useEffect, useMemo, useState,
} from "react";
import { apiGet, apiPost, apiPut } from "@/lib/api";
import type {
  ClusterInfo, Readiness, ServiceMatrix, Settings,
} from "@/lib/settings";

interface SettingsEnvelope {
  settings: Settings;
  resolved_endpoints: Record<string, string>;
  configured: boolean;
}

interface SettingsState {
  settings: Settings | null;
  resolvedEndpoints: Record<string, string>;
  /** Backend has a host and a username persisted. */
  configured: boolean;

  services: ServiceMatrix;
  readiness: Readiness | null;
  clusterInfo: ClusterInfo | null;

  loading: boolean;
  saving: boolean;
  testing: boolean;
  checkingReadiness: boolean;

  /** Everything the demo needs is configured, reachable and provisioned. */
  ready: boolean;

  reload: () => Promise<void>;
  save: (next: Settings) => Promise<void>;
  resetDefaults: () => Promise<void>;
  testServices: () => Promise<void>;
  refreshReadiness: () => Promise<void>;
  refreshClusterInfo: () => Promise<void>;
}

const SettingsContext = createContext<SettingsState | null>(null);

export function SettingsProvider({ children }: { children: React.ReactNode }) {
  const [settings, setSettings] = useState<Settings | null>(null);
  const [resolvedEndpoints, setResolvedEndpoints] = useState<Record<string, string>>({});
  const [configured, setConfigured] = useState(false);
  const [services, setServices] = useState<ServiceMatrix>({});
  const [readiness, setReadiness] = useState<Readiness | null>(null);
  const [clusterInfo, setClusterInfo] = useState<ClusterInfo | null>(null);

  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [checkingReadiness, setCheckingReadiness] = useState(false);

  const absorb = useCallback((data: SettingsEnvelope) => {
    setSettings(data.settings);
    setResolvedEndpoints(data.resolved_endpoints ?? {});
    setConfigured(Boolean(data.configured));
  }, []);

  const reload = useCallback(async () => {
    setLoading(true);
    try {
      absorb(await apiGet<SettingsEnvelope>("/api/settings"));
    } finally {
      setLoading(false);
    }
  }, [absorb]);

  const save = useCallback(async (next: Settings) => {
    setSaving(true);
    try {
      absorb(await apiPut<SettingsEnvelope>("/api/settings", next));
    } finally {
      setSaving(false);
    }
  }, [absorb]);

  const resetDefaults = useCallback(async () => {
    setSaving(true);
    try {
      absorb(await apiPost<SettingsEnvelope>("/api/settings/reset"));
      setServices({});
      setReadiness(null);
      setClusterInfo(null);
    } finally {
      setSaving(false);
    }
  }, [absorb]);

  const testServices = useCallback(async () => {
    setTesting(true);
    try {
      const d = await apiPost<{ services: ServiceMatrix }>("/api/settings/test");
      setServices(d.services ?? {});
    } finally {
      setTesting(false);
    }
  }, []);

  const refreshReadiness = useCallback(async () => {
    setCheckingReadiness(true);
    try {
      setReadiness(await apiGet<Readiness>("/api/cluster/readiness"));
    } catch {
      setReadiness(null);
    } finally {
      setCheckingReadiness(false);
    }
  }, []);

  const refreshClusterInfo = useCallback(async () => {
    try {
      const d = await apiGet<{ status: string; cluster?: ClusterInfo }>("/api/cluster/info");
      setClusterInfo(d.cluster && d.status !== "error" ? d.cluster : null);
    } catch {
      setClusterInfo(null);
    }
  }, []);

  // Initial load.
  useEffect(() => {
    reload().catch(() => setLoading(false));
  }, [reload]);

  // Once a host is configured, fetch what depends on it. Probes are not run
  // automatically — they are a deliberate action on the Setup page.
  useEffect(() => {
    if (!configured) return;
    refreshClusterInfo();
    refreshReadiness();
  }, [configured, refreshClusterInfo, refreshReadiness]);

  const ready = useMemo(() => {
    if (!configured || !readiness) return false;
    if (!readiness.nfs_mounted || !readiness.client_configured) return false;
    const artefacts = [
      ...Object.values(readiness.volumes ?? {}),
      ...Object.values(readiness.streams ?? {}),
    ];
    if (artefacts.length === 0 || !artefacts.every((s) => s === "ok")) return false;
    // Required services must be good; MCP is optional so it is not gating.
    const required = ["cluster", "s3"];
    return required.every((k) => services[k]?.status === "good");
  }, [configured, readiness, services]);

  const value: SettingsState = {
    settings, resolvedEndpoints, configured,
    services, readiness, clusterInfo,
    loading, saving, testing, checkingReadiness, ready,
    reload, save, resetDefaults, testServices, refreshReadiness, refreshClusterInfo,
  };

  return <SettingsContext.Provider value={value}>{children}</SettingsContext.Provider>;
}

export function useSettings(): SettingsState {
  const ctx = useContext(SettingsContext);
  if (!ctx) throw new Error("useSettings must be used within SettingsProvider");
  return ctx;
}
