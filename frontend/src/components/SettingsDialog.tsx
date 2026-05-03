"use client";

import { useState } from "react";
import { useCluster } from "@/contexts/ClusterContext";
import { useToast } from "@/contexts/ToastContext";
import { NexusSectionDivider } from "@/components/nexus-core-components";
import { RiCloseLine, RiErrorWarningLine } from "@remixicon/react";

interface SettingsDialogProps {
  onClose: () => void;
}

export default function SettingsDialog({ onClose }: SettingsDialogProps) {
  const { settings, setSettings, host, user, pass, setClusterInfo } = useCluster();
  const { notify } = useToast();

  const [local, setLocal]           = useState({ ...settings });
  const [cmdOutput, setCmdOutput]   = useState<string | null>(null);

  function patch(key: keyof typeof local, val: string) {
    setLocal((prev) => ({ ...prev, [key]: val }));
  }

  function save() {
    setSettings(local);
    notify("Settings saved", "positive");
    onClose();
  }

  async function runMount(cmd: string) {
    const r = await fetch(
      `/api/data/fs/list?path=${encodeURIComponent(cmd)}`,
      { headers: { "X-Mapr-Host": host, "X-Mapr-User": user, "X-Mapr-Pass": pass } }
    );
    const d = await r.json();
    setCmdOutput(d.output ?? JSON.stringify(d));
  }

  async function doCleanup() {
    if (!confirm("This will DELETE ALL volumes and data permanently. Continue?")) return;
    const r = await fetch("/api/cluster/cleanup", {
      method: "DELETE",
      headers: { "X-Mapr-Host": host, "X-Mapr-User": user, "X-Mapr-Pass": pass },
    });
    if (r.ok) {
      const d = await r.json();
      (d.messages ?? []).forEach((m: string) => notify(m, "warning"));
      setClusterInfo(null);
    } else {
      notify("Cleanup failed", "negative");
    }
  }

  return (
    <div className="dialog-backdrop" onClick={onClose}>
      <div
        className="w-full max-w-md rounded-lg flex flex-col overflow-hidden max-h-[90vh]"
        style={{
          background: "rgba(18, 18, 18, 0.96)",
          backdropFilter: "blur(24px)",
          WebkitBackdropFilter: "blur(24px)",
          border: "2px solid white",
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-6 pt-6 pb-4 shrink-0">
          <h2 className="font-sans font-bold text-[21px] text-white">Settings</h2>
          <button
            onClick={onClose}
            className="text-neutrals-medium hover:text-white transition-colors duration-200 p-1"
          >
            <RiCloseLine size={20} />
          </button>
        </div>

        <NexusSectionDivider style={{ paddingLeft: 24, marginBottom: 0 }} />

        <div className="px-6 py-5 overflow-y-auto flex flex-col gap-5">
          <SettingsSection title="External Data Lakes">
            <DarkField label="S3 / Minio Host" placeholder="minio.local"    value={local.s3Server}  onChange={(v) => patch("s3Server", v)} />
            <DarkField label="NFS Server Path" placeholder="nfs-server:/export" value={local.nfsPath} onChange={(v) => patch("nfsPath", v)} />
            {local.nfsPath && (
              <OutlineBtn onClick={() => runMount(local.nfsPath)}>List NFS Path</OutlineBtn>
            )}
          </SettingsSection>

          <NexusSectionDivider style={{ paddingLeft: 0, marginBottom: 0 }} />

          <SettingsSection title="S3 Credentials">
            <p className="font-sans font-light text-xs text-neutrals-medium">For Iceberg and Spark access</p>
            <DarkField label="Access Key" value={local.s3AccessKey} onChange={(v) => patch("s3AccessKey", v)} />
            <DarkField label="Secret Key" value={local.s3SecretKey} onChange={(v) => patch("s3SecretKey", v)} type="password" />
          </SettingsSection>

          <NexusSectionDivider style={{ paddingLeft: 0, marginBottom: 0 }} />

          <SettingsSection title="External Links">
            <DarkField label="Dashboard URL" value={local.dashboardUrl} onChange={(v) => patch("dashboardUrl", v)} />
            <DarkField label="Catalogue URL" value={local.catalogueUrl} onChange={(v) => patch("catalogueUrl", v)} />
          </SettingsSection>

          <NexusSectionDivider style={{ paddingLeft: 0, marginBottom: 0 }} />

          <SettingsSection title="Cluster Mount">
            <OutlineBtn onClick={() => runMount("/mapr")}>List /mapr</OutlineBtn>
          </SettingsSection>

          <NexusSectionDivider style={{ paddingLeft: 0, marginBottom: 0 }} />

          <SettingsSection title="Danger Zone" titleClass="text-status-failed">
            <p className="font-sans font-light text-xs text-neutrals-medium">
              Removes ALL volumes and data permanently
            </p>
            <button
              onClick={doCleanup}
              className="flex items-center gap-2 px-3 py-1.5 font-sans text-xs text-white rounded transition-colors duration-200"
              style={{ background: "#BF0300" }}
            >
              <RiErrorWarningLine size={14} />
              DELETE ALL
            </button>
          </SettingsSection>

          {cmdOutput !== null && (
            <div
              className="rounded p-3"
              style={{ background: "#000000", border: "1px solid #474747" }}
            >
              <div className="flex justify-between items-center mb-2">
                <span className="font-sans text-brand-contrast text-xs font-mono">Output</span>
                <button
                  onClick={() => setCmdOutput(null)}
                  className="text-neutrals-medium hover:text-white transition-colors duration-200 text-xs"
                >
                  ✕
                </button>
              </div>
              <pre className="text-status-good text-xs font-mono overflow-auto max-h-48 whitespace-pre-wrap">
                {cmdOutput}
              </pre>
            </div>
          )}
        </div>

        {/* Footer */}
        <div className="flex justify-end px-6 py-4 shrink-0" style={{ borderTop: "1px solid #474747" }}>
          <button
            onClick={save}
            className="px-6 py-2.5 rounded font-sans font-medium text-sm text-white transition-colors duration-200"
            style={{ background: "#F2561D" }}
            onMouseEnter={(e) => (e.currentTarget.style.background = "#D9704A")}
            onMouseLeave={(e) => (e.currentTarget.style.background = "#F2561D")}
          >
            Save
          </button>
        </div>
      </div>
    </div>
  );
}

function SettingsSection({
  title,
  titleClass = "text-neutrals-medium",
  children,
}: {
  title: string;
  titleClass?: string;
  children: React.ReactNode;
}) {
  return (
    <div className="flex flex-col gap-2">
      <p className={`font-sans font-medium text-xs uppercase tracking-widest ${titleClass}`}>{title}</p>
      {children}
    </div>
  );
}

function DarkField({
  label,
  value,
  onChange,
  placeholder,
  type = "text",
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  type?: string;
}) {
  return (
    <div className="flex flex-col gap-1">
      <label className="font-sans font-light text-xs text-neutrals-medium uppercase tracking-wider">
        {label}
      </label>
      <input
        type={type}
        value={value}
        placeholder={placeholder}
        onChange={(e) => onChange(e.target.value)}
        className="rounded px-3 py-1.5 text-sm font-sans text-white placeholder-neutrals-dark focus:outline-none transition-colors duration-200"
        style={{ background: "#000000", border: "1px solid #474747" }}
        onFocus={(e) => (e.currentTarget.style.borderColor = "#F2561D")}
        onBlur={(e)  => (e.currentTarget.style.borderColor = "#474747")}
      />
    </div>
  );
}

function OutlineBtn({ onClick, children }: { onClick: () => void; children: React.ReactNode }) {
  return (
    <button
      onClick={onClick}
      className="font-sans text-xs px-3 py-1.5 rounded text-neutrals-light hover:text-white transition-colors duration-200"
      style={{ border: "1px solid #474747" }}
      onMouseEnter={(e) => (e.currentTarget.style.borderColor = "#F2561D")}
      onMouseLeave={(e) => (e.currentTarget.style.borderColor = "#474747")}
    >
      {children}
    </button>
  );
}
