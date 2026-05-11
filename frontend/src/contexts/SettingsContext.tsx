"use client";

import React, {
  createContext,
  useCallback,
  useContext,
  useEffect,
  useMemo,
  useState,
} from "react";
import type { Settings, ServiceMatrix } from "@/lib/settings";

export type ArtefactStatus = "ok" | "missing" | "error" | "unknown";

export interface ReadinessArtefacts {
  client_configured: boolean;
  nfs_mounted: boolean;
  volumes: Record<string, ArtefactStatus>;
  streams: Record<string, ArtefactStatus>;
}

interface SettingsState {
  settings: Settings | null;
  resolvedEndpoints: Record<string, string>;
  services: ServiceMatrix;
  artefacts: ReadinessArtefacts | null;
  loadingArtefacts: boolean;
  isReady: boolean;
  loading: boolean;
  saving: boolean;
  testing: boolean;
  reload: () => Promise<void>;
  save: (next: Settings) => Promise<void>;
  test: () => Promise<void>;
  resetDefaults: () => Promise<void>;
  fetchArtefacts: () => Promise<void>;
}

const SettingsContext = createContext<SettingsState | null>(null);

export function SettingsProvider({ children }: { children: React.ReactNode }) {
  const [settings, setSettings] = useState<Settings | null>(null);
  const [resolvedEndpoints, setResolvedEndpoints] = useState<Record<string, string>>({});
  const [services, setServices] = useState<ServiceMatrix>({});
  const [artefacts, setArtefacts] = useState<ReadinessArtefacts | null>(null);
  const [loading, setLoading] = useState(true);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [loadingArtefacts, setLoadingArtefacts] = useState(false);

  const reload = useCallback(async () => {
    setLoading(true);
    try {
      const r = await fetch("/api/settings");
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const data = await r.json();
      setSettings(data.settings as Settings);
      setResolvedEndpoints(data.resolved_endpoints ?? {});
    } finally {
      setLoading(false);
    }
  }, []);

  const save = useCallback(async (next: Settings) => {
    setSaving(true);
    try {
      const r = await fetch("/api/settings", {
        method: "PUT",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(next),
      });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const data = await r.json();
      setSettings(data.settings as Settings);
      setResolvedEndpoints(data.resolved_endpoints ?? {});
    } finally {
      setSaving(false);
    }
  }, []);

  const test = useCallback(async () => {
    setTesting(true);
    try {
      const r = await fetch("/api/settings/test", { method: "POST" });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const data = await r.json();
      setServices((data.services ?? {}) as ServiceMatrix);
    } finally {
      setTesting(false);
    }
  }, []);

  const resetDefaults = useCallback(async () => {
    setSaving(true);
    try {
      const r = await fetch("/api/settings/reset", { method: "POST" });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const data = await r.json();
      setSettings(data.settings as Settings);
      setResolvedEndpoints(data.resolved_endpoints ?? {});
    } finally {
      setSaving(false);
    }
  }, []);

  const fetchArtefacts = useCallback(async () => {
    setLoadingArtefacts(true);
    try {
      const r = await fetch("/api/cluster/readiness");
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      setArtefacts(await r.json() as ReadinessArtefacts);
    } catch {
      /* silent */
    } finally {
      setLoadingArtefacts(false);
    }
  }, []);

  // isReady: all service probes good AND all volumes/streams exist
  const isReady = useMemo(() => {
    if (!settings?.cluster_host) return false;
    const svcValues = Object.values(services);
    if (svcValues.length === 0) return false;
    if (!svcValues.every((s) => s.status === "good")) return false;
    if (!artefacts) return false;
    return (
      Object.values(artefacts.volumes).every((s) => s === "ok") &&
      Object.values(artefacts.streams).every((s) => s === "ok")
    );
  }, [settings, services, artefacts]);

  useEffect(() => {
    reload().catch(() => {});
  }, [reload]);

  return (
    <SettingsContext.Provider
      value={{
        settings, resolvedEndpoints, services, artefacts, loadingArtefacts, isReady,
        loading, saving, testing,
        reload, save, test, resetDefaults, fetchArtefacts,
      }}
    >
      {children}
    </SettingsContext.Provider>
  );
}

export function useSettings(): SettingsState {
  const ctx = useContext(SettingsContext);
  if (!ctx) throw new Error("useSettings must be used within SettingsProvider");
  return ctx;
}
