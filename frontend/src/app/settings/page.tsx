"use client";

import { useCallback, useEffect, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import Header from "@/components/Header";
import { useSettings } from "@/contexts/SettingsContext";
import type { ReadinessArtefacts, ArtefactStatus } from "@/contexts/SettingsContext";
import { useToast } from "@/contexts/ToastContext";
import { PrimaryButton, SectionHeader, StatusDot, TealButton } from "@/components/nexus";
import { Field } from "@/components/settings/Field";
import { ServiceMatrixPanel } from "@/components/settings/ServiceMatrixPanel";
import {
  RiKey2Line, RiCheckLine, RiRefreshLine, RiLoader4Line,
  RiCloseLine, RiPlayLine, RiDatabase2Line, RiFlowChart,
} from "@remixicon/react";
import type { Settings } from "@/lib/settings";
import { postSSE, type SSEEvent } from "@/lib/sse";
import { cx } from "@/lib/cx";

const EASE = [0.22, 1, 0.36, 1] as const;

type StepStatus = "idle" | "running" | "check" | "error";
type StepMap = Record<string, { status: StepStatus; message: string }>;

function initSteps(names: string[]): StepMap {
  return Object.fromEntries(names.map((n) => [n, { status: "idle" as StepStatus, message: "" }]));
}

export default function SettingsPage() {
  const {
    settings, resolvedEndpoints, services, artefacts, loadingArtefacts, isReady,
    loading, saving, testing, save, test, resetDefaults, fetchArtefacts,
  } = useSettings();
  const { notify } = useToast();
  const [draft, setDraft] = useState<Settings | null>(null);
  const [showPass, setShowPass] = useState(false);
  const [generatingS3, setGeneratingS3] = useState(false);
  const [s3KeyStatus, setS3KeyStatus] = useState<"none" | "ok" | "error">("none");

  useEffect(() => {
    if (settings) {
      setDraft(structuredClone(settings));
      setS3KeyStatus(settings.credentials.s3_access_key ? "ok" : "none");
    }
  }, [settings]);

  // Fetch artefact state once on mount
  useEffect(() => { fetchArtefacts(); }, [fetchArtefacts]);

  function patch<K extends keyof Settings>(key: K, value: Settings[K]) {
    setDraft((d) => (d ? { ...d, [key]: value } : d));
  }

  function patchSection<K extends keyof Settings, F extends keyof Settings[K]>(
    section: K, field: F, value: Settings[K][F],
  ) {
    setDraft((d) => {
      if (!d) return d;
      return { ...d, [section]: { ...(d[section] as object), [field]: value } as unknown as Settings[K] };
    });
  }

  async function onSave() {
    if (!draft) return;
    try {
      await save(draft);
      notify("Settings saved", "positive");
      await Promise.all([test(), fetchArtefacts()]);
    } catch (e) {
      notify(`Save failed: ${e instanceof Error ? e.message : "unknown"}`, "negative");
    }
  }

  async function onReset() {
    if (!confirm("Reset all settings to defaults? Persisted file will be overwritten.")) return;
    try {
      await resetDefaults();
      notify("Defaults restored", "positive");
    } catch (e) {
      notify(`Reset failed: ${e instanceof Error ? e.message : "unknown"}`, "negative");
    }
  }

  async function onGenerateS3Keys() {
    if (draft) { try { await save(draft); } catch { /* proceed */ } }
    setGeneratingS3(true);
    setS3KeyStatus("none");
    try {
      const r = await fetch("/api/settings/s3keys", { method: "POST" });
      if (!r.ok) {
        const err = await r.json().catch(() => ({ detail: "Unknown error" }));
        throw new Error(err.detail ?? `HTTP ${r.status}`);
      }
      const sr = await fetch("/api/settings");
      const data = await sr.json();
      setDraft((d) => d ? { ...d, credentials: data.settings.credentials } : d);
      setS3KeyStatus("ok");
      notify("S3 key generated and saved", "positive");
    } catch (e) {
      setS3KeyStatus("error");
      notify(`S3 key generation failed: ${e instanceof Error ? e.message : "unknown"}`, "negative");
    } finally {
      setGeneratingS3(false);
    }
  }

  if (loading || !draft) {
    return (
      <>
        <Header onConnectClick={() => undefined} onSettingsClick={() => undefined} />
        <main className="pt-[140px] max-w-7xl px-8 mx-auto">
          <div className="nexus-skeleton h-12 rounded-lg mb-6 max-w-md" />
          <div className="nexus-skeleton h-64 rounded-3xl" />
        </main>
      </>
    );
  }

  const clusterConfigured = !!draft.cluster_host && !!draft.credentials.cluster_user;
  const clientSetupDone = artefacts?.client_configured === true && artefacts?.nfs_mounted === true;

  return (
    <>
      <Header onConnectClick={() => undefined} onSettingsClick={() => undefined} />
      <motion.main
        className="pt-[120px] pb-[80px] max-w-7xl px-8 mx-auto"
        initial={{ opacity: 0, y: 8 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.4, ease: EASE }}
      >
        <SectionHeader title="Settings" />

        <div className="flex items-center gap-3 pl-12 mb-10">
          <StatusDot status={isReady ? "good" : "unknown"} pulse={isReady} size={10} />
          <p className="font-serif text-3xl text-white leading-tight">
            {isReady ? (
              <>Demo is <span className="text-status-good">ready.</span></>
            ) : (
              <>Configure cluster, verify services, and create demo artefacts.{" "}
                <span className="text-neutrals-medium">All checks must pass before the demo runs.</span>
              </>
            )}
          </p>
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-[1fr_440px] gap-8 pl-12">
          {/* ── Left column ────────────────────────────────────────────────────── */}
          <div className="flex flex-col gap-8 min-w-0">

            <ClusterSection
              draft={draft}
              patch={patch}
              patchSection={patchSection}
              showPass={showPass}
              setShowPass={setShowPass}
              resolvedEndpoints={resolvedEndpoints}
              onVerifySslChange={(v) => patchSection("flags", "verify_ssl", v)}
            />

            <ClientSetupSection
              clusterConfigured={clusterConfigured}
              artefacts={artefacts}
              loadingArtefacts={loadingArtefacts}
              draft={draft}
              onRefresh={fetchArtefacts}
              onPolarisCredFetched={(cred) =>
                setDraft((d) => d ? { ...d, credentials: { ...d.credentials, polaris_credential: cred } } : d)
              }
            />

            <S3KeySection
              hasKey={!!draft.credentials.s3_access_key}
              keyStatus={s3KeyStatus}
              generating={generatingS3}
              onGenerate={onGenerateS3Keys}
              clusterConfigured={clusterConfigured}
              clientSetupDone={clientSetupDone}
            />

            <ArtefactsSection
              clusterConfigured={clusterConfigured}
              clientSetupDone={clientSetupDone}
              artefacts={artefacts}
              loadingArtefacts={loadingArtefacts}
              draft={draft}
              onRefresh={fetchArtefacts}
            />

            <div className="flex items-center gap-3 pt-4">
              <PrimaryButton onClick={onSave} disabled={saving}>
                {saving ? "Saving…" : "Save & re-probe"}
              </PrimaryButton>
              <TealButton onClick={() => test()} disabled={testing} className="flex items-center gap-1.5">
                <RiRefreshLine size={13} className={testing ? "animate-spin" : ""} />
                {testing ? "Probing…" : "Test services"}
              </TealButton>
              <TealButton onClick={fetchArtefacts} disabled={loadingArtefacts} className="flex items-center gap-1.5">
                <RiRefreshLine size={13} className={loadingArtefacts ? "animate-spin" : ""} />
                {loadingArtefacts ? "Checking…" : "Check artefacts"}
              </TealButton>
              <button
                onClick={onReset}
                disabled={saving}
                className="ml-auto text-xs uppercase tracking-[0.15em] text-neutrals-medium hover:text-status-failed transition-colors duration-200"
              >
                Reset to defaults
              </button>
            </div>
          </div>

          {/* ── Right column: service reachability ──────────────────────────────── */}
          <div className="lg:sticky lg:top-32 self-start min-w-0">
            <ServiceMatrixPanel
              services={services}
              resolvedEndpoints={resolvedEndpoints}
              onTest={test}
              testing={testing}
            />
          </div>
        </div>
      </motion.main>
    </>
  );
}

// ── Section wrapper ────────────────────────────────────────────────────────────

function SectionPanel({ title, subtitle, children }: { title: string; subtitle?: string; children: React.ReactNode }) {
  return (
    <section className="bg-[#121212] border-2 border-white/10 rounded-3xl p-8">
      <div className="mb-6">
        <h3 className="font-medium text-base text-white uppercase tracking-[0.15em]">{title}</h3>
        {subtitle && <p className="text-[11px] text-neutrals-medium mt-1">{subtitle}</p>}
      </div>
      {children}
    </section>
  );
}

// ── SSE progress display ───────────────────────────────────────────────────────

const STEP_ICONS: Record<StepStatus, React.ReactNode> = {
  idle:    <span className="w-4 h-4 rounded-full border border-neutrals-dark shrink-0" />,
  running: <RiLoader4Line size={16} className="animate-spin text-brand-vivid shrink-0" />,
  check:   <RiCheckLine size={16} className="text-status-good shrink-0" />,
  error:   <RiCloseLine size={16} className="text-status-failed shrink-0" />,
};

function SSEStepList({ steps, labels }: { steps: StepMap; labels: Record<string, string> }) {
  const entries = Object.entries(steps).filter(([k]) => steps[k].status !== "idle");
  if (entries.length === 0) return null;
  return (
    <div className="flex flex-col gap-1.5 mt-4 pt-4 border-t border-neutrals-dark">
      {entries.map(([key, { status, message }]) => (
        <div key={key} className="flex items-start gap-3">
          {STEP_ICONS[status]}
          <div className="flex flex-col min-w-0">
            <span className="text-[11px] text-white">{labels[key] ?? key}</span>
            {message && (
              <span className={cx(
                "text-[10px] font-mono break-all",
                status === "error" ? "text-status-failed" : "text-neutrals-medium",
              )}>{message}</span>
            )}
          </div>
        </div>
      ))}
    </div>
  );
}

// ── Cluster section ────────────────────────────────────────────────────────────

function EndpointRow({ label, url }: { label: string; url: string }) {
  return (
    <div className="flex items-baseline gap-3 py-1.5 border-b border-neutrals-dark/50 last:border-0">
      <span className="text-[11px] uppercase tracking-[0.12em] text-neutrals-medium w-28 shrink-0">{label}</span>
      <span className="font-mono text-[11px] text-neutrals-light break-all">
        {url || <span className="text-neutrals-dark italic">enter cluster host above</span>}
      </span>
    </div>
  );
}

function ClusterSection({
  draft, patch, patchSection, showPass, setShowPass, resolvedEndpoints, onVerifySslChange,
}: {
  draft: Settings;
  patch: <K extends keyof Settings>(k: K, v: Settings[K]) => void;
  patchSection: <K extends keyof Settings, F extends keyof Settings[K]>(s: K, f: F, v: Settings[K][F]) => void;
  showPass: boolean;
  setShowPass: (v: boolean) => void;
  resolvedEndpoints: Record<string, string>;
  onVerifySslChange: (v: boolean) => void;
}) {
  return (
    <SectionPanel
      title="Cluster"
      subtitle="All service endpoints are derived from the cluster host on their standard Data Fabric ports."
    >
      <div className="grid grid-cols-1 md:grid-cols-2 gap-x-6 gap-y-5 mb-6">
        <Field
          label="Cluster host"
          placeholder="10.0.0.1 or df-host.local"
          value={draft.cluster_host}
          onChange={(v) => patch("cluster_host", v)}
          hint="IP or hostname — no https:// prefix"
          className="md:col-span-2"
        />
        <Field
          label="Username"
          placeholder="mapr"
          value={draft.credentials.cluster_user}
          onChange={(v) => patchSection("credentials", "cluster_user", v)}
        />
        <Field
          label="Password"
          type={showPass ? "text" : "password"}
          value={draft.credentials.cluster_pass}
          onChange={(v) => patchSection("credentials", "cluster_pass", v)}
        />
        <label className="flex items-center gap-2 text-[11px] text-neutrals-medium uppercase tracking-[0.15em] cursor-pointer">
          <input type="checkbox" checked={showPass} onChange={(e) => setShowPass(e.target.checked)} className="accent-brand-vivid" />
          Show password
        </label>
        <label className="flex items-center gap-2 text-[11px] text-neutrals-medium uppercase tracking-[0.15em] cursor-pointer">
          <input type="checkbox" checked={draft.flags.verify_ssl} onChange={(e) => onVerifySslChange(e.target.checked)} className="accent-brand-vivid" />
          Verify SSL certificates
          <span className="normal-case tracking-normal text-neutrals-dark ml-1">(enable for public CA-signed certs)</span>
        </label>
      </div>

      <div className="pt-4 border-t border-neutrals-dark">
        <p className="text-[11px] uppercase tracking-[0.15em] text-neutrals-medium mb-3">Derived service endpoints</p>
        <EndpointRow label="REST API"    url={draft.cluster_host ? `https://${draft.cluster_host}:8443` : ""} />
        <EndpointRow label="S3 / Object" url={resolvedEndpoints.s3_endpoint ?? ""} />
        <EndpointRow label="Polaris"     url={resolvedEndpoints.polaris_url ?? ""} />
        <EndpointRow label="Livy"        url={resolvedEndpoints.livy_url ?? ""} />
        <EndpointRow label="Grafana"     url={resolvedEndpoints.grafana_url ?? ""} />
        <EndpointRow label="OpenTSDB"    url={resolvedEndpoints.opentsdb_url ?? ""} />
        <EndpointRow label="Fluentd"     url={resolvedEndpoints.fluentd_host ?? ""} />
        <EndpointRow label="MCP"         url={resolvedEndpoints.mcp_server_url ?? ""} />
      </div>
    </SectionPanel>
  );
}

// ── Client setup section ───────────────────────────────────────────────────────

const CLIENT_STEP_LABELS: Record<string, string> = {
  connect:   "Connect to cluster",
  user:      "Ensure local user",
  ssh:       "Deploy SSH key",
  ssl:       "Fetch SSL truststore",
  configure: "Run configure.sh",
  keycreds:  "Copy key credentials",
  ticket:    "Create login ticket",
  nfs:       "Mount /mapr via NFS4",
};

function ClientBadge({ label, ok }: { label: string; ok: boolean }) {
  return (
    <div className="flex items-center gap-2">
      <StatusDot status={ok ? "good" : "failed"} size={8} />
      <span className="text-[11px] text-neutrals-light">{label}</span>
    </div>
  );
}

function ClientSetupSection({
  clusterConfigured, artefacts, loadingArtefacts, draft, onRefresh, onPolarisCredFetched,
}: {
  clusterConfigured: boolean;
  artefacts: ReadinessArtefacts | null;
  loadingArtefacts: boolean;
  draft: Settings;
  onRefresh: () => void;
  onPolarisCredFetched: (cred: string) => void;
}) {
  const [steps, setSteps] = useState<StepMap>(initSteps(Object.keys(CLIENT_STEP_LABELS)));
  const [running, setRunning] = useState(false);
  const [fetchingPolaris, setFetchingPolaris] = useState(false);
  const { notify } = useToast();

  const updateStep = useCallback((e: SSEEvent) => {
    setSteps((prev) => ({ ...prev, [e.name]: { status: e.status as StepStatus, message: e.message } }));
  }, []);

  async function onConfigure() {
    if (!clusterConfigured) return;
    setRunning(true);
    setSteps(initSteps(Object.keys(CLIENT_STEP_LABELS)));
    try {
      await postSSE(
        "/api/cluster/client/configure",
        { host: draft.cluster_host, user: draft.credentials.cluster_user, password: draft.credentials.cluster_pass },
        updateStep,
      );
    } catch (e) {
      setSteps((prev) => ({ ...prev, connect: { status: "error", message: String(e) } }));
    } finally {
      setRunning(false);
      onRefresh();
    }
  }

  async function onFetchPolaris() {
    setFetchingPolaris(true);
    try {
      const r = await fetch("/api/settings/polaris-creds");
      const data = await r.json();
      if (!r.ok) throw new Error(data.detail ?? `HTTP ${r.status}`);
      onPolarisCredFetched(data.polaris_credential);
      notify("Polaris credentials fetched and saved", "positive");
    } catch (e) {
      notify(`Failed to fetch Polaris credentials: ${e instanceof Error ? e.message : String(e)}`, "negative");
    } finally {
      setFetchingPolaris(false);
    }
  }

  const clientOk = artefacts?.client_configured ?? false;
  const nfsOk = artefacts?.nfs_mounted ?? false;

  return (
    <SectionPanel
      title="Client setup"
      subtitle="Configures the MapR client on this container — installs the SSL truststore, creates a login ticket, and mounts the global namespace at /mapr."
    >
      <div className="flex items-center justify-between gap-4 flex-wrap">
        <div className="flex items-center gap-6">
          {loadingArtefacts ? (
            <RiLoader4Line size={14} className="animate-spin text-neutrals-dark" />
          ) : (
            <>
              <ClientBadge label="MapR ticket" ok={clientOk} />
              <ClientBadge label="/mapr mounted" ok={nfsOk} />
            </>
          )}
        </div>
        <TealButton
          size="sm"
          onClick={onConfigure}
          disabled={running || !clusterConfigured}
          title={!clusterConfigured ? "Save cluster host and credentials first" : ""}
          className="flex items-center gap-1.5 shrink-0"
        >
          <RiPlayLine size={13} />
          {running ? "Configuring…" : (clientOk && nfsOk) ? "Re-configure" : "Configure client"}
        </TealButton>
      </div>

      <AnimatePresence>
        {Object.values(steps).some((s) => s.status !== "idle") && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.2 }}
            className="overflow-hidden"
          >
            <SSEStepList steps={steps} labels={CLIENT_STEP_LABELS} />
          </motion.div>
        )}
      </AnimatePresence>

      {/* Polaris credentials — fetched from cluster via SSH */}
      <div className="flex items-center justify-between gap-4 flex-wrap mt-5 pt-4 border-t border-neutrals-dark">
        <div className="flex flex-col gap-0.5 min-w-0">
          <span className="text-[11px] uppercase tracking-[0.15em] text-neutrals-medium">Polaris credentials</span>
          {draft.credentials.polaris_credential ? (
            <span className="font-mono text-[11px] text-status-good truncate max-w-xs">
              {draft.credentials.polaris_credential.split(":")[0]}:***
            </span>
          ) : (
            <span className="text-[11px] text-neutrals-dark italic">Not fetched yet</span>
          )}
        </div>
        <TealButton
          size="sm"
          onClick={onFetchPolaris}
          disabled={fetchingPolaris || !clusterConfigured}
          title={!clusterConfigured ? "Configure cluster credentials first" : "Read from /opt/mapr/polaris/.../credentials.txt via SSH"}
          className="flex items-center gap-1.5 shrink-0"
        >
          <RiKey2Line size={13} />
          {fetchingPolaris ? "Fetching…" : draft.credentials.polaris_credential ? "Refresh" : "Fetch credentials"}
        </TealButton>
      </div>
    </SectionPanel>
  );
}

// ── S3 key section ─────────────────────────────────────────────────────────────

function S3KeySection({
  hasKey, keyStatus, generating, onGenerate, clusterConfigured, clientSetupDone,
}: {
  hasKey: boolean;
  keyStatus: "none" | "ok" | "error";
  generating: boolean;
  onGenerate: () => void;
  clusterConfigured: boolean;
  clientSetupDone: boolean;
}) {
  return (
    <SectionPanel
      title="S3 credentials"
      subtitle="Keys are generated automatically via the Data Fabric REST API (/rest/s3keys/generate). You never need to enter them manually."
    >
      {!clientSetupDone ? (
        <p className="text-[11px] text-neutrals-dark py-1">
          Complete <span className="text-neutrals-medium font-medium">Client Setup</span> above before generating S3 credentials.
        </p>
      ) : (
        <>
          <div className="flex items-center gap-4">
            <div className="flex items-center gap-3 flex-1">
              {keyStatus === "ok" && <><RiCheckLine size={18} className="text-status-good shrink-0" /><span className="text-sm text-status-good">S3 key active</span></>}
              {keyStatus === "error" && <span className="text-sm text-status-failed">Key generation failed — check cluster connectivity and credentials above</span>}
              {keyStatus === "none" && !hasKey && <span className="text-sm text-neutrals-medium">No S3 key — click Generate to create one</span>}
              {keyStatus === "none" && hasKey && <><RiCheckLine size={18} className="text-status-good shrink-0" /><span className="text-sm text-status-good">S3 key stored</span></>}
            </div>
            <TealButton
              size="sm"
              onClick={onGenerate}
              disabled={generating || !clusterConfigured}
              title={!clusterConfigured ? "Configure cluster host and credentials first" : ""}
              className="flex items-center gap-1.5 shrink-0"
            >
              <RiKey2Line size={13} />
              {generating ? "Generating…" : hasKey ? "Refresh key" : "Generate key"}
            </TealButton>
          </div>
          {!clusterConfigured && (
            <p className="text-[11px] text-neutrals-dark mt-3">Save cluster host and credentials first, then generate an S3 key.</p>
          )}
        </>
      )}
    </SectionPanel>
  );
}

// ── Demo artefacts section ─────────────────────────────────────────────────────

const ARTEFACT_STEP_LABELS: Record<string, string> = {
  volumes: "Create data lake volumes",
  tables:  "Create binary tables",
  streams: "Create Kafka streams",
};

function ArtefactBadge({ label, status, icon }: { label: string; status: ArtefactStatus; icon: React.ReactNode }) {
  const colours: Record<ArtefactStatus, string> = {
    ok:      "text-status-good",
    missing: "text-status-degraded",
    error:   "text-status-failed",
    unknown: "text-neutrals-dark",
  };
  const dot: Record<ArtefactStatus, "good" | "degraded" | "failed" | "unknown"> = {
    ok: "good", missing: "degraded", error: "failed", unknown: "unknown",
  };
  return (
    <div className="flex items-center gap-2 py-1">
      <StatusDot status={dot[status]} size={8} />
      <span className="text-neutrals-dark shrink-0">{icon}</span>
      <span className="text-[11px] text-neutrals-light">{label}</span>
      <span className={cx("ml-auto text-[10px] uppercase tracking-[0.1em] font-medium", colours[status])}>
        {status}
      </span>
    </div>
  );
}

function ArtefactsSection({
  clusterConfigured, clientSetupDone, artefacts, loadingArtefacts, draft, onRefresh,
}: {
  clusterConfigured: boolean;
  clientSetupDone: boolean;
  artefacts: ReadinessArtefacts | null;
  loadingArtefacts: boolean;
  draft: Settings;
  onRefresh: () => void;
}) {
  const [steps, setSteps] = useState<StepMap>(initSteps(Object.keys(ARTEFACT_STEP_LABELS)));
  const [running, setRunning] = useState(false);

  const updateStep = useCallback((e: SSEEvent) => {
    setSteps((prev) => ({ ...prev, [e.name]: { status: e.status as StepStatus, message: e.message } }));
  }, []);

  async function onCreate() {
    if (!clusterConfigured) return;
    setRunning(true);
    setSteps(initSteps(Object.keys(ARTEFACT_STEP_LABELS)));
    try {
      await postSSE(
        "/api/cluster/artefacts",
        { host: draft.cluster_host, user: draft.credentials.cluster_user, password: draft.credentials.cluster_pass },
        updateStep,
      );
    } catch (e) {
      setSteps((prev) => ({ ...prev, volumes: { status: "error", message: String(e) } }));
    } finally {
      setRunning(false);
      onRefresh();
    }
  }

  const vols = artefacts?.volumes ?? {};
  const strs = artefacts?.streams ?? {};
  const allOk = [...Object.values(vols), ...Object.values(strs)].every((s) => s === "ok");
  const anyMissing = [...Object.values(vols), ...Object.values(strs)].some((s) => s === "missing" || s === "error");

  return (
    <SectionPanel
      title="Demo artefacts"
      subtitle="Volumes, binary tables, and Kafka streams required by the demo pipeline. Fixed at /nexmesh-demo — create if missing."
    >
      {!clientSetupDone ? (
        <p className="text-[11px] text-neutrals-dark py-1">
          Complete <span className="text-neutrals-medium font-medium">Client Setup</span> above before creating demo artefacts.
        </p>
      ) : (
        <>
          {loadingArtefacts ? (
            <div className="flex items-center gap-2 text-neutrals-dark py-2">
              <RiLoader4Line size={14} className="animate-spin" />
              <span className="text-[11px]">Checking artefacts…</span>
            </div>
          ) : (
            <div className="flex flex-col gap-0.5 mb-4">
              <ArtefactBadge label="Volume: /nexmesh-demo/bronze" status={vols["bronze"] ?? "unknown"} icon={<RiDatabase2Line size={12} />} />
              <ArtefactBadge label="Volume: /nexmesh-demo/silver" status={vols["silver"] ?? "unknown"} icon={<RiDatabase2Line size={12} />} />
              <ArtefactBadge label="Volume: /nexmesh-demo/gold"   status={vols["gold"]   ?? "unknown"} icon={<RiDatabase2Line size={12} />} />
              <ArtefactBadge label="Stream: /nexmesh-demo/incoming" status={strs["incoming"] ?? "unknown"} icon={<RiFlowChart size={12} />} />
            </div>
          )}

          <div className="flex items-center gap-3">
            <TealButton
              size="sm"
              onClick={onCreate}
              disabled={running || !clusterConfigured || allOk}
              title={!clusterConfigured ? "Save cluster host and credentials first" : allOk ? "All artefacts already exist" : ""}
              className="flex items-center gap-1.5"
            >
              <RiPlayLine size={13} />
              {running ? "Creating…" : anyMissing ? "Create missing" : "Create artefacts"}
            </TealButton>
            {allOk && !running && (
              <span className="text-[11px] text-status-good flex items-center gap-1.5">
                <RiCheckLine size={14} /> All artefacts present
              </span>
            )}
          </div>

          <AnimatePresence>
            {Object.values(steps).some((s) => s.status !== "idle") && (
              <motion.div
                initial={{ height: 0, opacity: 0 }}
                animate={{ height: "auto", opacity: 1 }}
                exit={{ height: 0, opacity: 0 }}
                transition={{ duration: 0.2 }}
                className="overflow-hidden"
              >
                <SSEStepList steps={steps} labels={ARTEFACT_STEP_LABELS} />
              </motion.div>
            )}
          </AnimatePresence>
        </>
      )}
    </SectionPanel>
  );
}

