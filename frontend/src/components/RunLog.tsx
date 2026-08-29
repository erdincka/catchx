"use client";

import { RiDeleteBinLine, RiHistoryLine } from "@remixicon/react";
import { useToast, type ToastTone } from "@/contexts/ToastContext";
import { Button, EmptyState, SectionTitle } from "@/components/ui";
import { cx } from "@/lib/cx";

const DOT: Record<ToastTone, string> = {
  positive: "bg-good",
  negative: "bg-bad",
  warning:  "bg-warn",
  info:     "bg-info",
};

/**
 * Durable record of everything the demo has run.
 *
 * Toasts vanish after a few seconds, which is fine for acknowledgement but
 * loses the numbers that matter — "23 flagged of 1,000 scanned" is the punch
 * line of the demo and needs to stay on screen.
 */
export default function RunLog({ className }: { className?: string }) {
  const { entries, clearLog } = useToast();

  return (
    <div className={cx("flex flex-col min-h-0", className)}>
      <div className="flex items-center justify-between px-3 py-2 border-b border-border shrink-0">
        <SectionTitle className="flex items-center gap-1.5">
          <RiHistoryLine size={12} />
          Run log
        </SectionTitle>
        {entries.length > 0 && (
          <Button
            variant="ghost"
            size="sm"
            onClick={clearLog}
            icon={<RiDeleteBinLine size={12} />}
            title="Clear the log"
          >
            Clear
          </Button>
        )}
      </div>

      <div className="flex-1 min-h-0 overflow-auto">
        {entries.length === 0 ? (
          <EmptyState
            title="Nothing run yet"
            hint="Each step you run is recorded here with its result."
            className="py-8"
          />
        ) : (
          <ul className="divide-y divide-border">
            {entries.map((e) => (
              <li key={e.id} className="px-3 py-2 flex items-start gap-2.5 rise">
                <span
                  className={cx("mt-1.5 w-1.5 h-1.5 rounded-full shrink-0", DOT[e.tone])}
                  aria-hidden
                />
                <div className="min-w-0 flex-1">
                  <div className="flex items-baseline justify-between gap-2">
                    <span className="text-[12px] font-medium text-text truncate">{e.label}</span>
                    <span className="font-mono text-[10px] text-subtle shrink-0">{e.at}</span>
                  </div>
                  {e.detail && (
                    <div className="text-[11.5px] text-muted mt-0.5 leading-snug break-words">
                      {e.detail}
                    </div>
                  )}
                </div>
              </li>
            ))}
          </ul>
        )}
      </div>
    </div>
  );
}
