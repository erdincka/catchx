"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import Link from "next/link";
import {
  RiFlashlightLine, RiPauseLine, RiPlayLine, RiRefreshLine,
  RiSettings3Line, RiToggleLine, RiToggleFill, RiAlertLine,
} from "@remixicon/react";
import AppShell from "@/components/AppShell";
import PipelineDiagram from "@/components/diagrams/PipelineDiagram";
import StepCard from "@/components/StepCard";
import RunLog from "@/components/RunLog";
import DataTable from "@/components/DataTable";
import CodeViewer from "@/components/CodeViewer";
import VolumeBar from "@/components/VolumeBar";
import { Dialog } from "@/components/ui/Dialog";
import { Button, Card, EmptyState, Spinner } from "@/components/ui";
import { MetricsProvider, useMetrics } from "@/contexts/MetricsContext";
import { useSettings } from "@/contexts/SettingsContext";
import { useToast } from "@/contexts/ToastContext";
import { apiGet, apiPost } from "@/lib/api";
import { PIPELINE, nextStepIndex, type PipelineStep, type StepId } from "@/lib/pipeline";

interface PeekTarget {
  tier: string;
  table: string;
  iceberg?: boolean;
  /** Gold transactions filtered to suspected fraud only. */
  fraudOnly?: boolean;
}

function PipelineInner() {
  const { configured } = useSettings();
  const { metrics, settled, hasData, live, setLive, refresh, lastError } = useMetrics();
  const { notify, log } = useToast();

  const [expertMode, setExpertMode] = useState(false);
  const [openStep, setOpenStep] = useState<StepId | null>(null);
  const [running, setRunning] = useState<{ step: StepId; action: string } | null>(null);
  const [codeTarget, setCodeTarget] = useState<string | null>(null);

  const [peek, setPeek] = useState<{
    label: string;
    loading: boolean;
    records: Record<string, unknown>[];
    total?: number;
    error?: string;
  } | null>(null);

  const nextIdx = useMemo(() => nextStepIndex(metrics), [metrics]);
  const allDone = nextIdx === -1;

  // Follow the demo: open whichever step is up next, until the presenter
  // takes over by opening one themselves.
  const [pinned, setPinned] = useState(false);
  useEffect(() => {
    if (pinned || !settled) return;
    setOpenStep(nextIdx === -1 ? null : PIPELINE[nextIdx].id);
  }, [nextIdx, settled, pinned]);

  const runAction = useCallback(
    async (step: PipelineStep, actionId: string) => {
      const action = step.actions.find((a) => a.id === actionId);
      if (!action || running) return;

      setRunning({ step: step.id, action: actionId });
      try {
        const data = await apiPost<Record<string, unknown>>(action.path);

        // Services report failure in the body as well as by status code.
        if (data.status === "error") {
          const msg = String(data.message ?? "Step failed");
          notify(msg, "negative");
          log(`${step.title} — ${action.label}`, msg, "negative");
          return;
        }

        const summary = action.summarise?.(data) ?? String(data.message ?? "Done");
        notify(`${action.label}: ${summary}`, "positive");
        log(action.describe, summary, "positive");
      } catch (e) {
        const msg = e instanceof Error ? e.message : "Step failed";
        notify(msg, "negative");
        log(`${step.title} — ${action.label}`, msg, "negative");
      } finally {
        setRunning(null);
        // Pull fresh counts straight away; the backend invalidated its cache
        // when the write landed.
        refresh();
      }
    },
    [running, notify, log, refresh],
  );

  const openPeek = useCallback(async (target: PeekTarget, label: string) => {
    setPeek({ label, loading: true, records: [] });
    try {
      let data: Record<string, unknown>;
      if (target.tier === "preview") {
        data = await apiGet(`/api/data/${target.table}/preview`);
      } else if (target.iceberg) {
        data = await apiGet(`/api/data/iceberg/${target.tier}/${target.table}/tail`);
      } else {
        data = await apiGet(
          `/api/data/peek/${target.tier}/${target.table}`,
          target.fraudOnly ? { fraud_only: "true" } : undefined,
        );
      }

      if (data.status === "error") {
        setPeek({ label, loading: false, records: [], error: String(data.message) });
        return;
      }
      setPeek({
        label,
        loading: false,
        records: (data.records as Record<string, unknown>[]) ?? [],
        total: typeof data.total === "number" ? data.total
             : typeof data.count === "number" ? data.count : undefined,
      });
    } catch (e) {
      setPeek({
        label, loading: false, records: [],
        error: e instanceof Error ? e.message : "Could not load records",
      });
    }
  }, []);

  if (!configured) {
    return (
      <AppShell>
        <div className="max-w-2xl mx-auto px-4 py-16">
          <EmptyState
            icon={<RiSettings3Line size={26} />}
            title="No cluster configured yet"
            hint="CatchX runs against a live HPE Data Fabric cluster. Add the host and credentials on the Setup page, then come back here to run the pipeline."
            action={
              <Link href="/setup">
                <Button variant="primary" icon={<RiSettings3Line size={13} />}>
                  Go to Setup
                </Button>
              </Link>
            }
          />
        </div>
      </AppShell>
    );
  }

  const activeStep: StepId | null = running?.step ?? null;

  return (
    <AppShell footer={<VolumeBar />}>
      <div className="max-w-[1500px] mx-auto px-4 py-4 flex flex-col gap-4">
        {/* Header row */}
        <div className="flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0">
            <h1 className="text-[17px] font-semibold tracking-tight text-text">
              Fraud &amp; Risk pipeline
            </h1>
            <p className="text-[12.5px] text-muted mt-0.5 max-w-2xl leading-snug">
              Six steps take raw transactions from a stream and a CSV through a bronze,
              silver and gold medallion architecture to a flagged-fraud data product —
              all inside the Data Fabric.
            </p>
          </div>

          <div className="flex items-center gap-2 shrink-0">
            <Button
              size="sm"
              variant="ghost"
              onClick={() => setExpertMode((v) => !v)}
              icon={expertMode ? <RiToggleFill size={14} /> : <RiToggleLine size={14} />}
              title={
                expertMode
                  ? "Expert mode on — step ordering is not enforced"
                  : "Guided mode — steps unlock in order"
              }
            >
              {expertMode ? "Expert" : "Guided"}
            </Button>
            <Button
              size="sm"
              variant="ghost"
              onClick={() => setLive(!live)}
              icon={live ? <RiPauseLine size={13} /> : <RiPlayLine size={13} />}
              title={live ? "Pause metric polling" : "Resume metric polling"}
            >
              {live ? "Live" : "Paused"}
            </Button>
            <Button
              size="sm"
              variant="secondary"
              onClick={refresh}
              icon={<RiRefreshLine size={13} />}
            >
              Refresh
            </Button>
          </div>
        </div>

        {lastError && (
          <div className="flex items-center gap-2 px-3 py-2 rounded-lg border border-warn/40
                          bg-warn-soft text-[12px] text-warn">
            <RiAlertLine size={14} className="shrink-0" />
            <span>Metrics unavailable — {lastError}</span>
          </div>
        )}

        {/* Diagram */}
        <Card className="p-3">
          {settled ? (
            <PipelineDiagram
              metrics={metrics}
              activeStep={activeStep}
              onPeek={(p, label) => openPeek(p, label)}
            />
          ) : (
            <div className="h-[280px] grid place-items-center text-muted text-[13px] gap-2">
              <Spinner /> Reading cluster state…
            </div>
          )}
          <p className="text-[11px] text-subtle mt-2 px-1">
            Counts are live from the cluster. Click any populated node to inspect its records.
          </p>
        </Card>

        {/* Steps + log */}
        <div className="grid gap-4 lg:grid-cols-[minmax(0,1fr)_320px]">
          <div className="flex flex-col gap-2">
            {allDone && hasData && (
              <div className="flex items-center gap-2 px-3 py-2 rounded-lg border border-good/40
                              bg-good-soft text-[12.5px] text-good">
                <RiFlashlightLine size={14} className="shrink-0" />
                <span>
                  Every step has run. Inspect any tier from the diagram, or reset the demo
                  from Setup to run it again.
                </span>
              </div>
            )}

            {PIPELINE.map((step) => (
              <StepCard
                key={step.id}
                step={step}
                metrics={metrics}
                expanded={openStep === step.id}
                onToggle={() => {
                  setPinned(true);
                  setOpenStep(openStep === step.id ? null : step.id);
                }}
                running={running?.step === step.id ? running.action : null}
                onRun={runAction}
                onViewCode={setCodeTarget}
                expertMode={expertMode}
              />
            ))}
          </div>

          <Card className="lg:sticky lg:top-4 h-fit max-h-[calc(100dvh-8rem)] flex flex-col overflow-hidden">
            <RunLog className="max-h-[60vh]" />
          </Card>
        </div>
      </div>

      {/* Data inspector */}
      <Dialog
        open={Boolean(peek)}
        onClose={() => setPeek(null)}
        size="xl"
        title={peek?.label ?? ""}
        description={
          peek?.error ? undefined : <>Live records read straight from the fabric</>
        }
      >
        {peek?.loading && (
          <div className="flex items-center justify-center gap-2 py-16 text-muted text-[13px]">
            <Spinner /> Loading records…
          </div>
        )}
        {peek && !peek.loading && peek.error && (
          <EmptyState title="Nothing to show" hint={peek.error} />
        )}
        {peek && !peek.loading && !peek.error && (
          <DataTable records={peek.records} total={peek.total} />
        )}
      </Dialog>

      <CodeViewer functionName={codeTarget} onClose={() => setCodeTarget(null)} />
    </AppShell>
  );
}

export default function PipelinePage() {
  const { configured } = useSettings();
  return (
    <MetricsProvider enabled={configured}>
      <PipelineInner />
    </MetricsProvider>
  );
}
