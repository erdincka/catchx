"use client";

import React, { createContext, useCallback, useContext, useRef, useState } from "react";
import {
  RiCloseLine, RiCheckLine, RiErrorWarningLine, RiAlertLine, RiInformationLine,
} from "@remixicon/react";
import { cx } from "@/lib/cx";

export type ToastTone = "positive" | "negative" | "warning" | "info";

interface Toast {
  id: number;
  message: string;
  tone: ToastTone;
}

/** A durable record of what the demo did — toasts disappear, this does not. */
export interface LogEntry {
  id: number;
  at: string;
  label: string;
  detail?: string;
  tone: ToastTone;
}

interface ToastCtx {
  notify: (message: string, tone?: ToastTone) => void;
  /** Append to the run log (and optionally toast the same thing). */
  log: (label: string, detail?: string, tone?: ToastTone) => void;
  entries: LogEntry[];
  clearLog: () => void;
}

const ToastContext = createContext<ToastCtx | null>(null);

const TONE_STYLES: Record<ToastTone, string> = {
  positive: "border-good/40 bg-good-soft text-good",
  negative: "border-bad/40 bg-bad-soft text-bad",
  warning:  "border-warn/40 bg-warn-soft text-warn",
  info:     "border-info/40 bg-info-soft text-info",
};

const TONE_ICONS: Record<ToastTone, React.ReactNode> = {
  positive: <RiCheckLine size={15} />,
  negative: <RiErrorWarningLine size={15} />,
  warning:  <RiAlertLine size={15} />,
  info:     <RiInformationLine size={15} />,
};

/** Errors stay up longer — they carry information worth reading. */
const DURATION: Record<ToastTone, number> = {
  positive: 3500, info: 4000, warning: 6000, negative: 9000,
};

export function ToastProvider({ children }: { children: React.ReactNode }) {
  const [toasts, setToasts] = useState<Toast[]>([]);
  const [entries, setEntries] = useState<LogEntry[]>([]);
  const counter = useRef(0);

  const dismiss = useCallback((id: number) => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
  }, []);

  const notify = useCallback((message: string, tone: ToastTone = "info") => {
    const id = ++counter.current;
    setToasts((prev) => [...prev.slice(-4), { id, message, tone }]);
    window.setTimeout(() => dismiss(id), DURATION[tone]);
  }, [dismiss]);

  const log = useCallback((label: string, detail?: string, tone: ToastTone = "positive") => {
    const id = ++counter.current;
    const at = new Date().toLocaleTimeString(undefined, {
      hour: "2-digit", minute: "2-digit", second: "2-digit",
    });
    setEntries((prev) => [{ id, at, label, detail, tone }, ...prev].slice(0, 100));
  }, []);

  const clearLog = useCallback(() => setEntries([]), []);

  return (
    <ToastContext.Provider value={{ notify, log, entries, clearLog }}>
      {children}
      <div
        className="fixed bottom-4 right-4 z-[100] flex flex-col gap-2 w-[min(24rem,calc(100vw-2rem))]"
        role="status"
        aria-live="polite"
      >
        {toasts.map((t) => (
          <div
            key={t.id}
            className={cx(
              "rise flex items-start gap-2.5 px-3 py-2.5 rounded-lg border shadow-[var(--shadow-md)]",
              "backdrop-blur-sm",
              TONE_STYLES[t.tone],
            )}
          >
            <span className="shrink-0 mt-px">{TONE_ICONS[t.tone]}</span>
            <span className="flex-1 text-[12.5px] leading-snug text-text">{t.message}</span>
            <button
              onClick={() => dismiss(t.id)}
              aria-label="Dismiss"
              className="shrink-0 -mr-1 -mt-0.5 p-1 rounded text-subtle hover:text-text transition-colors"
            >
              <RiCloseLine size={14} />
            </button>
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  );
}

export function useToast(): ToastCtx {
  const ctx = useContext(ToastContext);
  if (!ctx) throw new Error("useToast must be used within ToastProvider");
  return ctx;
}
