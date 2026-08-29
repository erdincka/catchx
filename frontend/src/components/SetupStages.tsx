"use client";

import { useCallback, useRef, useState } from "react";
import { RiCheckLine, RiErrorWarningLine, RiPlayLine, RiStopLine } from "@remixicon/react";
import { Button, Spinner, StatusDot } from "@/components/ui";
import { readSSE, type StepStatus } from "@/lib/sse";
import { cx } from "@/lib/cx";

/** Backend step ids, in the order the streams emit them. */
export const CONFIGURE_STEPS: Array<[string, string]> = [
  ["connect",  "Reach the cluster REST API"],
  ["user",     "Ensure the local user exists"],
  ["ssh",      "Deploy an SSH key to the cluster"],
  ["ssl",      "Fetch the SSL truststore"],
  ["configure","Run configure.sh"],
  ["keycreds", "Copy key credentials"],
  ["ticket",   "Create a Data Fabric login ticket"],
  ["nfs",      "Mount /mapr over NFS"],
];

export const PROVISION_STEPS: Array<[string, string]> = [
  ["volumes", "Create bronze, silver and gold volumes"],
  ["tables",  "Create DocumentDB tables"],
  ["streams", "Create the fabric streams"],
];

type State = Record<string, { status: StepStatus | "pending"; message?: string }>;

function initial(steps: Array<[string, string]>): State {
  return Object.fromEntries(steps.map(([id]) => [id, { status: "pending" as const }]));
}

/**
 * Runs one of the backend's SSE setup streams and shows every step.
 *
 * The step ids here must match what the backend emits — when they drifted
 * apart previously, rows sat on "pending" through a completely successful run.
 */
export default function SetupStage({
  title,
  description,
  steps,
  endpoint,
  runLabel,
  disabled,
  disabledReason,
  beforeRun,
  onFinished,
}: {
  title: string;
  description: string;
  steps: Array<[string, string]>;
  endpoint: string;
  runLabel: string;
  disabled?: boolean;
  disabledReason?: string;
  /** Persist pending edits before the backend reads settings. */
  beforeRun?: () => Promise<void>;
  onFinished?: (hadError: boolean) => void;
}) {
  const [state, setState] = useState<State>(() => initial(steps));
  const [running, setRunning] = useState(false);
  const abort = useRef<AbortController | null>(null);

  const run = useCallback(async () => {
    if (running) return;
    await beforeRun?.();

    setState(initial(steps));
    setRunning(true);
    const ac = new AbortController();
    abort.current = ac;

    let hadError = false;
    try {
      await readSSE(
        endpoint,
        (e) => {
          if (e.status === "error") hadError = true;
          setState((prev) => ({ ...prev, [e.name]: { status: e.status, message: e.message } }));
        },
        { signal: ac.signal },
      );
    } catch (e) {
      if (!ac.signal.aborted) {
        hadError = true;
        setState((prev) => ({
          ...prev,
          [steps[0][0]]: {
            status: "error",
            message: e instanceof Error ? e.message : "Stream failed",
          },
        }));
      }
    } finally {
      setRunning(false);
      abort.current = null;
      if (!ac.signal.aborted) onFinished?.(hadError);
    }
  }, [running, beforeRun, steps, endpoint, onFinished]);

  return (
    <div className="flex flex-col gap-3">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0">
          <h3 className="text-[13px] font-semibold text-text">{title}</h3>
          <p className="text-[11.5px] text-muted mt-0.5 leading-snug">{description}</p>
        </div>
        {running ? (
          <Button
            size="sm"
            variant="danger"
            onClick={() => abort.current?.abort()}
            icon={<RiStopLine size={13} />}
          >
            Stop
          </Button>
        ) : (
          <Button
            size="sm"
            variant="primary"
            onClick={run}
            disabled={disabled}
            title={disabled ? disabledReason : undefined}
            icon={<RiPlayLine size={13} />}
          >
            {runLabel}
          </Button>
        )}
      </div>

      <ul className="flex flex-col gap-1">
        {steps.map(([id, label]) => {
          const s = state[id] ?? { status: "pending" as const };
          return (
            <li key={id} className="flex items-start gap-2.5 py-0.5">
              <span className="mt-0.5 w-4 h-4 grid place-items-center shrink-0">
                {s.status === "running" ? (
                  <Spinner size={12} className="text-accent" />
                ) : s.status === "check" ? (
                  <RiCheckLine size={13} className="text-good" />
                ) : s.status === "error" ? (
                  <RiErrorWarningLine size={13} className="text-bad" />
                ) : (
                  <StatusDot tone="neutral" size={5} className="opacity-50" />
                )}
              </span>
              <span className="min-w-0 flex-1">
                <span
                  className={cx(
                    "text-[12px] block",
                    s.status === "pending" ? "text-subtle"
                      : s.status === "error" ? "text-bad"
                      : "text-text",
                  )}
                >
                  {label}
                </span>
                {s.message && s.status !== "pending" && (
                  <span
                    className={cx(
                      "block text-[11px] mt-0.5 leading-snug break-words",
                      s.status === "error" ? "text-bad" : "text-muted",
                    )}
                  >
                    {s.message}
                  </span>
                )}
              </span>
            </li>
          );
        })}
      </ul>
    </div>
  );
}
