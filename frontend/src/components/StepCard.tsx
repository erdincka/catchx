"use client";

import {
  RiCheckLine, RiLock2Line, RiPlayLine, RiCodeSSlashLine, RiArrowRightSLine,
} from "@remixicon/react";
import { Badge, Button, Spinner, StatusDot } from "@/components/ui";
import type { Metrics, PipelineStep } from "@/lib/pipeline";
import { stepState } from "@/lib/pipeline";
import { cx } from "@/lib/cx";

/**
 * One step of the guided flow.
 *
 * Whether a step is done comes from live metrics, not from click history —
 * so a reload, a backend restart, or someone else having run half the demo
 * all leave the flow showing the truth.
 */
export default function StepCard({
  step,
  metrics,
  expanded,
  onToggle,
  running,
  onRun,
  onViewCode,
  expertMode,
}: {
  step: PipelineStep;
  metrics: Metrics;
  expanded: boolean;
  onToggle: () => void;
  /** Action id currently in flight for this step, if any. */
  running: string | null;
  onRun: (step: PipelineStep, actionId: string) => void;
  onViewCode: (fn: string) => void;
  /** Expert mode ignores ordering and enables everything. */
  expertMode: boolean;
}) {
  const state = stepState(step, metrics);
  const blockedReason = step.blockedBy(metrics);
  const locked = state === "blocked" && !expertMode;
  const busy = running !== null;

  return (
    <div
      className={cx(
        "rounded-lg border transition-colors",
        state === "done"
          ? "border-good/35 bg-good-soft/30"
          : locked
            ? "border-border bg-surface-sunk/40"
            : "border-accent/40 bg-surface",
      )}
    >
      {/* Header — click to expand */}
      <button
        onClick={onToggle}
        aria-expanded={expanded}
        className="w-full flex items-center gap-3 px-3 py-2.5 text-left"
      >
        {/* Step marker */}
        <span
          className={cx(
            "w-6 h-6 rounded-full grid place-items-center shrink-0 text-[11px] font-semibold",
            state === "done"
              ? "bg-good text-white"
              : locked
                ? "bg-surface-sunk text-subtle border border-border"
                : "bg-accent text-on-accent",
          )}
        >
          {busy ? <Spinner size={12} />
            : state === "done" ? <RiCheckLine size={13} />
            : locked ? <RiLock2Line size={11} />
            : step.n}
        </span>

        <span className="min-w-0 flex-1">
          <span className="flex items-center gap-2">
            <span className={cx(
              "text-[13px] font-semibold truncate",
              locked ? "text-muted" : "text-text",
            )}>
              {step.title}
            </span>
            {state === "done" && <Badge tone="good">done</Badge>}
            {busy && <Badge tone="accent">running</Badge>}
          </span>
          <span className="block text-[11.5px] text-muted truncate mt-0.5">
            {step.blurb}
          </span>
        </span>

        <RiArrowRightSLine
          size={16}
          className={cx(
            "shrink-0 text-subtle transition-transform",
            expanded && "rotate-90",
          )}
        />
      </button>

      {/* Body */}
      {expanded && (
        <div className="px-3 pb-3 pt-0 flex flex-col gap-2.5 rise">
          <div className="flex items-center gap-1.5 text-[11px] text-muted">
            <StatusDot tone="accent" size={5} />
            <span className="font-medium text-accent-text">{step.capability}</span>
          </div>

          <p className="text-[12px] text-muted leading-relaxed">{step.detail}</p>

          {locked && blockedReason && (
            <div className="flex items-start gap-1.5 text-[11.5px] text-warn
                            bg-warn-soft border border-warn/30 rounded-md px-2 py-1.5">
              <RiLock2Line size={12} className="mt-0.5 shrink-0" />
              <span>{blockedReason}</span>
            </div>
          )}

          <div className="flex flex-wrap items-center gap-1.5">
            {step.actions.map((a) => (
              <span key={a.id} className="inline-flex">
                <Button
                  size="sm"
                  variant={state === "done" ? "secondary" : "primary"}
                  disabled={locked || busy}
                  loading={running === a.id}
                  onClick={() => onRun(step, a.id)}
                  icon={running === a.id ? undefined : <RiPlayLine size={12} />}
                  title={locked ? (blockedReason ?? "") : `Run: ${a.label}`}
                  className={a.code ? "rounded-r-none border-r-0" : undefined}
                >
                  {a.label}
                </Button>
                {a.code && (
                  <Button
                    size="sm"
                    variant="secondary"
                    onClick={() => onViewCode(a.code!)}
                    title={`View the source of ${a.code}`}
                    aria-label={`View the source of ${a.code}`}
                    className="rounded-l-none px-1.5"
                  >
                    <RiCodeSSlashLine size={12} />
                  </Button>
                )}
              </span>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
