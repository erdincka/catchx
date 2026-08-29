"use client";

import { useCallback, useEffect, useState } from "react";
import {
  RiRefreshLine, RiSaveLine, RiKey2Line, RiRobot2Line, RiDeleteBin6Line,
  RiRestartLine, RiCheckLine, RiExternalLinkLine, RiAlertLine,
} from "@remixicon/react";
import AppShell from "@/components/AppShell";
import SetupStage, { CONFIGURE_STEPS, PROVISION_STEPS } from "@/components/SetupStages";
import { Dialog } from "@/components/ui/Dialog";
import {
  Badge, Button, Card, CardHeader, Code, EmptyState, SectionTitle,
  Spinner, StatusDot, type Tone,
} from "@/components/ui";
import { TextField, PasswordField, Toggle } from "@/components/ui/fields";
import { useSettings } from "@/contexts/SettingsContext";
import { useToast } from "@/contexts/ToastContext";
import { apiDelete, apiGet, apiPost } from "@/lib/api";
import {
  SERVICE_META, SERVICE_ORDER, type ServiceStatus, type Settings,
} from "@/lib/settings";

const STATUS_TONE: Record<ServiceStatus, Tone> = {
  good: "good", degraded: "warn", failed: "bad", unknown: "neutral",
};

interface McpTool {
  name: string;
  description: string;
  reachable?: boolean;
  detail?: string;
  source: string;
}

export default function SetupPage() {
  const {
    settings, resolvedEndpoints, configured, services, readiness, clusterInfo,
    loading, saving, testing, ready,
    save, resetDefaults, testServices, refreshReadiness, refreshClusterInfo,
  } = useSettings();
  const { notify } = useToast();

  const [draft, setDraft] = useState<Settings | null>(null);
  const [dirty, setDirty] = useState(false);

  const [s3Busy, setS3Busy] = useState(false);
  const [mcp, setMcp] = useState<{ loading: boolean; tools?: McpTool[]; error?: string; endpoint?: string } | null>(null);
  const [confirmCleanup, setConfirmCleanup] = useState(false);
  const [cleaning, setCleaning] = useState(false);

  useEffect(() => {
    if (settings && !dirty) setDraft(settings);
  }, [settings, dirty]);

  const patch = useCallback((fn: (d: Settings) => Settings) => {
    setDraft((d) => (d ? fn(d) : d));
    setDirty(true);
  }, []);

  /** Persist the draft. Every action that makes the backend read settings
   *  calls this first — the backend is the only source of truth. */
  const persist = useCallback(async () => {
    if (!draft) return;
    await save(draft);
    setDirty(false);
  }, [draft, save]);

  const onSave = useCallback(async () => {
    try {
      await persist();
      notify("Settings saved", "positive");
      await Promise.all([testServices(), refreshReadiness(), refreshClusterInfo()]);
    } catch (e) {
      notify(e instanceof Error ? e.message : "Save failed", "negative");
    }
  }, [persist, notify, testServices, refreshReadiness, refreshClusterInfo]);

  const onGenerateS3 = useCallback(async () => {
    setS3Busy(true);
    try {
      await persist();
      const d = await apiPost<{ access_key: string }>("/api/settings/s3keys");
      notify(`S3 key generated (${d.access_key})`, "positive");
      await testServices();
    } catch (e) {
      notify(e instanceof Error ? e.message : "Could not generate S3 keys", "negative");
    } finally {
      setS3Busy(false);
    }
  }, [persist, notify, testServices]);

  const onDiscoverMcp = useCallback(async () => {
    setMcp({ loading: true });
    try {
      await persist();
      const d = await apiGet<{ tools: McpTool[]; endpoint: string }>("/api/mcp/tools");
      setMcp({ loading: false, tools: d.tools, endpoint: d.endpoint });
    } catch (e) {
      setMcp({ loading: false, error: e instanceof Error ? e.message : "MCP discovery failed" });
    }
  }, [persist]);

  const onCleanup = useCallback(async () => {
    setCleaning(true);
    try {
      const d = await apiDelete<{ messages?: string[] }>("/api/cluster/cleanup");
      notify(`Cleanup complete — ${d.messages?.length ?? 0} actions`, "positive");
      setConfirmCleanup(false);
      await refreshReadiness();
    } catch (e) {
      notify(e instanceof Error ? e.message : "Cleanup failed", "negative");
    } finally {
      setCleaning(false);
    }
  }, [notify, refreshReadiness]);

  if (loading || !draft) {
    return (
      <AppShell>
        <div className="max-w-3xl mx-auto px-4 py-16 flex items-center justify-center gap-2 text-muted">
          <Spinner /> Loading settings…
        </div>
      </AppShell>
    );
  }

  const hasCreds = Boolean(draft.cluster_host && draft.credentials.cluster_user);
  const clientReady = Boolean(readiness?.client_configured && readiness?.nfs_mounted);

  return (
    <AppShell>
      <div className="max-w-3xl mx-auto px-4 py-5 flex flex-col gap-4">
        <div>
          <h1 className="text-[17px] font-semibold tracking-tight text-text">Setup</h1>
          <p className="text-[12.5px] text-muted mt-0.5 leading-snug">
            CatchX stores this configuration on the backend, so it survives a browser
            refresh and every API route reads the same values. Work down the page.
          </p>
        </div>

        {/* ── 1. Connection ────────────────────────────────────────────────── */}
        <Card>
          <CardHeader
            title="1 · Cluster connection"
            description="The Data Fabric node running the REST API on port 8443."
            actions={
              <>
                {dirty && <Badge tone="warn">unsaved</Badge>}
                <Button
                  size="sm"
                  variant="primary"
                  onClick={onSave}
                  loading={saving}
                  icon={<RiSaveLine size={13} />}
                >
                  Save
                </Button>
              </>
            }
          />
          <div className="p-4 grid gap-3 sm:grid-cols-2">
            <TextField
              label="Host or IP"
              placeholder="df01.example.com"
              hint="A hostname is preferred — cluster certificates are usually wildcards that an IP cannot match."
              value={draft.cluster_host}
              onChange={(v) => patch((d) => ({ ...d, cluster_host: v }))}
              mono
            />
            <TextField
              label="Username"
              placeholder="mapr"
              value={draft.credentials.cluster_user}
              onChange={(v) =>
                patch((d) => ({ ...d, credentials: { ...d.credentials, cluster_user: v } }))
              }
            />
            <PasswordField
              label="Password"
              hint="Stored in the backend's settings file, not in your browser."
              value={draft.credentials.cluster_pass}
              onChange={(v) =>
                patch((d) => ({ ...d, credentials: { ...d.credentials, cluster_pass: v } }))
              }
            />
            <div className="flex flex-col justify-end gap-3 pb-1">
              <Toggle
                label="Verify TLS certificates"
                hint="Off suits internal clusters with self-signed certs."
                checked={draft.flags.verify_ssl}
                onChange={(v) => patch((d) => ({ ...d, flags: { ...d.flags, verify_ssl: v } }))}
              />
            </div>
          </div>
        </Card>

        {/* ── 2. Services ─────────────────────────────────────────────────── */}
        <Card>
          <CardHeader
            title="2 · Required services"
            description="CatchX needs the cluster API and the object store. MCP is optional."
            actions={
              <Button size="sm" onClick={testServices} loading={testing} icon={<RiRefreshLine size={13} />}>
                Probe
              </Button>
            }
          />
          <ul className="divide-y divide-border">
            {SERVICE_ORDER.filter((k) => k !== "mcp" || draft.flags.mcp_enabled).map((key) => {
              const meta = SERVICE_META[key];
              const probe = services[key];
              const tone = probe ? STATUS_TONE[probe.status] : "neutral";
              const url = resolvedEndpoints[`${key === "s3" ? "s3_endpoint" : "mcp_server_url"}`];
              return (
                <li key={key} className="px-4 py-2.5 flex items-start gap-3">
                  <StatusDot tone={tone} className="mt-1.5" />
                  <div className="min-w-0 flex-1">
                    <div className="flex items-center gap-2 flex-wrap">
                      <span className="text-[12.5px] font-medium text-text">{meta.label}</span>
                      {!meta.required && <Badge tone="neutral">optional</Badge>}
                      <span className="text-[11px] text-subtle">{meta.hint}</span>
                    </div>
                    <div className="text-[11px] text-muted mt-0.5 break-words">
                      {probe ? probe.detail : "Not probed yet"}
                      {probe && probe.latency_ms > 0 && (
                        <span className="text-subtle"> · {probe.latency_ms} ms</span>
                      )}
                    </div>
                  </div>
                  {key !== "cluster" && url && !url.includes("{host}") && (
                    <a
                      href={url}
                      target="_blank"
                      rel="noreferrer noopener"
                      className="mt-0.5 p-1 rounded text-subtle hover:text-text transition-colors"
                      title={url}
                    >
                      <RiExternalLinkLine size={13} />
                    </a>
                  )}
                </li>
              );
            })}
          </ul>
        </Card>

        {/* ── 3 & 4. Client + artefacts ───────────────────────────────────── */}
        <Card>
          <CardHeader
            title="3 · Configure the client"
            description="Point this container's MapR client at your cluster and mount the global namespace."
          />
          <div className="p-4">
            <SetupStage
              title="Client configuration"
              description="Deploys an SSH key, fetches the truststore, runs configure.sh and mounts /mapr over NFSv3."
              steps={CONFIGURE_STEPS}
              endpoint="/api/cluster/configure"
              runLabel="Configure"
              disabled={!hasCreds}
              disabledReason="Enter the cluster host and credentials first."
              beforeRun={persist}
              onFinished={(hadError) => {
                refreshReadiness();
                refreshClusterInfo();
                notify(
                  hadError ? "Client configuration finished with errors" : "Client configured",
                  hadError ? "warning" : "positive",
                );
              }}
            />
          </div>
        </Card>

        <Card>
          <CardHeader
            title="4 · Provision demo artefacts"
            description="Volumes, DocumentDB tables and streams the pipeline writes into."
          />
          <div className="p-4">
            <SetupStage
              title="Volumes, tables and streams"
              description={`Created under ${draft.targets.base_volume} on the cluster.`}
              steps={PROVISION_STEPS}
              endpoint="/api/cluster/provision"
              runLabel="Provision"
              disabled={!clientReady}
              disabledReason="Configure the client first — provisioning needs the mounted namespace."
              beforeRun={persist}
              onFinished={(hadError) => {
                refreshReadiness();
                notify(
                  hadError ? "Provisioning finished with errors" : "Artefacts provisioned",
                  hadError ? "warning" : "positive",
                );
              }}
            />

            {readiness && (
              <div className="mt-4 pt-3 border-t border-border flex flex-wrap gap-x-4 gap-y-1.5">
                {Object.entries({ ...readiness.volumes, ...readiness.streams }).map(([name, st]) => (
                  <span key={name} className="flex items-center gap-1.5 text-[11.5px]">
                    <StatusDot tone={st === "ok" ? "good" : st === "missing" ? "warn" : "bad"} size={6} />
                    <span className="text-muted">{name}</span>
                  </span>
                ))}
              </div>
            )}
          </div>
        </Card>

        {/* ── 5. S3 ───────────────────────────────────────────────────────── */}
        <Card>
          <CardHeader
            title="5 · Object store access"
            description="Generates an S3 key pair through the cluster REST API and stores it for the demo."
            actions={
              <Button
                size="sm"
                onClick={onGenerateS3}
                loading={s3Busy}
                disabled={!hasCreds}
                title={!hasCreds ? "Enter the cluster host and credentials first." : undefined}
                icon={<RiKey2Line size={13} />}
              >
                {draft.credentials.s3_access_key ? "Regenerate" : "Generate keys"}
              </Button>
            }
          />
          <div className="px-4 py-3 text-[12px] text-muted">
            {draft.credentials.s3_access_key ? (
              <span className="flex items-center gap-2">
                <RiCheckLine size={14} className="text-good" />
                Access key <Code>{draft.credentials.s3_access_key}</Code> · secret stored on the backend
              </span>
            ) : (
              "No S3 keys yet. They are generated by the cluster — never typed in."
            )}
          </div>
        </Card>

        {/* ── 6. MCP ──────────────────────────────────────────────────────── */}
        <Card>
          <CardHeader
            title="6 · Data Fabric MCP"
            description="Optional. Discovers the agent-callable tools the fabric's MCP server exposes."
            actions={
              <Button
                size="sm"
                onClick={onDiscoverMcp}
                loading={mcp?.loading}
                disabled={!draft.flags.mcp_enabled || !draft.cluster_host}
                icon={<RiRobot2Line size={13} />}
              >
                Discover
              </Button>
            }
          />
          <div className="px-4 py-3 flex flex-col gap-3">
            <Toggle
              label="Enable MCP"
              hint="When off, MCP is neither probed nor shown. The demo does not require it."
              checked={draft.flags.mcp_enabled}
              onChange={(v) => patch((d) => ({ ...d, flags: { ...d.flags, mcp_enabled: v } }))}
            />

            {mcp?.error && (
              <div className="flex items-start gap-2 text-[11.5px] text-warn bg-warn-soft
                              border border-warn/30 rounded-md px-2 py-1.5">
                <RiAlertLine size={13} className="mt-0.5 shrink-0" />
                <span>{mcp.error}</span>
              </div>
            )}

            {mcp?.tools && (
              <div className="flex flex-col gap-1.5">
                <SectionTitle>
                  {mcp.tools.length} tool{mcp.tools.length === 1 ? "" : "s"} at {mcp.endpoint}
                </SectionTitle>
                <ul className="flex flex-col gap-1">
                  {mcp.tools.map((t) => (
                    <li key={t.name} className="flex items-start gap-2 text-[11.5px]">
                      <StatusDot
                        tone={t.reachable === false ? "bad" : "good"}
                        size={6}
                        className="mt-1.5"
                      />
                      <span className="min-w-0">
                        <span className="font-mono text-text">{t.name}</span>
                        <span className="text-muted"> — {t.description}</span>
                        {t.detail && <span className="text-subtle"> ({t.detail})</span>}
                      </span>
                    </li>
                  ))}
                </ul>
              </div>
            )}
          </div>
        </Card>

        {/* ── Advanced ────────────────────────────────────────────────────── */}
        <Card>
          <CardHeader title="Demo targets" description="Where the demo creates its data on the cluster." />
          <div className="p-4 grid gap-3 sm:grid-cols-3">
            <TextField
              label="Base volume" mono
              value={draft.targets.base_volume}
              onChange={(v) => patch((d) => ({ ...d, targets: { ...d.targets, base_volume: v } }))}
            />
            <TextField
              label="Stream path" mono
              value={draft.targets.stream_path}
              onChange={(v) => patch((d) => ({ ...d, targets: { ...d.targets, stream_path: v } }))}
            />
            <TextField
              label="S3 bucket" mono
              value={draft.targets.s3_bucket}
              onChange={(v) => patch((d) => ({ ...d, targets: { ...d.targets, s3_bucket: v } }))}
            />
          </div>
        </Card>

        {/* ── Danger zone ─────────────────────────────────────────────────── */}
        <Card className="border-bad/30">
          <CardHeader
            title="Reset"
            description="Start the demo over, or clear the stored configuration."
          />
          <div className="p-4 flex flex-wrap gap-2">
            <Button
              variant="danger"
              size="sm"
              onClick={() => setConfirmCleanup(true)}
              disabled={!configured}
              icon={<RiDeleteBin6Line size={13} />}
            >
              Delete demo data
            </Button>
            <Button
              variant="secondary"
              size="sm"
              onClick={async () => {
                await resetDefaults();
                setDirty(false);
                notify("Settings reset to defaults", "info");
              }}
              icon={<RiRestartLine size={13} />}
            >
              Reset settings
            </Button>
            {ready && (
              <span className="flex items-center gap-1.5 text-[11.5px] text-good ml-auto self-center">
                <RiCheckLine size={13} /> Everything is ready
              </span>
            )}
          </div>
        </Card>

        {clusterInfo && (
          <p className="text-[11px] text-subtle text-center pb-2">
            Connected to <span className="font-mono text-muted">{clusterInfo.name}</span>
            {clusterInfo.ip && <> · {clusterInfo.ip}</>}
          </p>
        )}
      </div>

      <Dialog
        open={confirmCleanup}
        onClose={() => setConfirmCleanup(false)}
        size="md"
        title="Delete all demo data?"
        description="This removes the demo volumes, streams and tables from the cluster."
        footer={
          <>
            <Button size="sm" onClick={() => setConfirmCleanup(false)}>Cancel</Button>
            <Button size="sm" variant="danger" loading={cleaning} onClick={onCleanup}>
              Delete everything
            </Button>
          </>
        }
      >
        <div className="px-5 py-4 text-[12.5px] text-muted leading-relaxed">
          Every volume, stream and table created under{" "}
          <Code>{draft.targets.base_volume}</Code> will be deleted from the cluster,
          along with the generated source files. Your saved connection settings are kept
          so you can run the demo again from step 4.
        </div>
      </Dialog>
    </AppShell>
  );
}
