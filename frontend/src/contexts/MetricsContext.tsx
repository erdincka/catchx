"use client";

import React, {
  createContext, useCallback, useContext, useEffect, useRef, useState,
} from "react";
import { apiGet } from "@/lib/api";
import type { Metrics } from "@/lib/pipeline";

const POLL_MS = 3000;

interface MetricsState {
  metrics: Metrics;
  /** True once a poll has completed — successfully or not.
   *  Callers use this to stop showing a spinner: an unreachable cluster
   *  should render an empty pipeline plus the error, not spin forever. */
  settled: boolean;
  /** True once a poll actually returned metrics. */
  hasData: boolean;
  live: boolean;
  setLive: (v: boolean) => void;
  /** Poll immediately — call after an action that changes cluster state. */
  refresh: () => Promise<void>;
  lastError: string | null;
}

const MetricsContext = createContext<MetricsState | null>(null);

/**
 * Polls /api/monitoring/metrics.
 *
 * Kept separate from SettingsProvider so a 3-second tick re-renders only the
 * components that actually read metrics.
 */
export function MetricsProvider({
  children,
  enabled,
}: {
  children: React.ReactNode;
  enabled: boolean;
}) {
  const [metrics, setMetrics] = useState<Metrics>({});
  const [settled, setSettled] = useState(false);
  const [hasData, setHasData] = useState(false);
  const [live, setLive] = useState(true);
  const [lastError, setLastError] = useState<string | null>(null);

  // Guards against overlapping polls when the backend is slow.
  const inFlight = useRef(false);

  const poll = useCallback(async () => {
    if (inFlight.current) return;
    inFlight.current = true;
    try {
      const data = await apiGet<Record<string, unknown>>("/api/monitoring/metrics");
      const next: Metrics = {};
      for (const [k, v] of Object.entries(data)) {
        if (typeof v === "number" || typeof v === "boolean") next[k] = v;
      }
      setMetrics(next);
      setHasData(true);
      setLastError(null);
    } catch (e) {
      setLastError(e instanceof Error ? e.message : "Metrics unavailable");
    } finally {
      inFlight.current = false;
      setSettled(true);
    }
  }, []);

  useEffect(() => {
    if (!enabled || !live) return;
    poll();
    const id = window.setInterval(poll, POLL_MS);
    return () => window.clearInterval(id);
  }, [enabled, live, poll]);

  // Pause polling while the tab is hidden — no point hammering the cluster
  // while the presenter is on another screen.
  useEffect(() => {
    const onVisibility = () => {
      if (document.visibilityState === "visible" && enabled && live) poll();
    };
    document.addEventListener("visibilitychange", onVisibility);
    return () => document.removeEventListener("visibilitychange", onVisibility);
  }, [enabled, live, poll]);

  return (
    <MetricsContext.Provider
      value={{ metrics, settled, hasData, live, setLive, refresh: poll, lastError }}
    >
      {children}
    </MetricsContext.Provider>
  );
}

export function useMetrics(): MetricsState {
  const ctx = useContext(MetricsContext);
  if (!ctx) throw new Error("useMetrics must be used within MetricsProvider");
  return ctx;
}
