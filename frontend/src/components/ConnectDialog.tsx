"use client";

import { useState } from "react";
import { useCluster } from "@/contexts/ClusterContext";
import { useToast } from "@/contexts/ToastContext";
import { CLUSTER_SETUP_STEPS } from "@/lib/constants";
import { NexusSectionDivider } from "@/components/nexus-core-components";
import { RiCheckLine, RiCloseLine, RiLoader4Line, RiErrorWarningLine } from "@remixicon/react";

type StepStatus = "pending" | "running" | "ok" | "error";

interface ConnectDialogProps {
  onClose: () => void;
}

export default function ConnectDialog({ onClose }: ConnectDialogProps) {
  const { host, user, pass, setHost, setUser, setPass, setClusterInfo, setDemoMode } =
    useCluster();
  const { notify } = useToast();

  const [localHost, setLocalHost] = useState(host);
  const [localUser, setLocalUser] = useState(user);
  const [localPass, setLocalPass] = useState(pass);
  const [running, setRunning]     = useState(false);
  const [stepStatuses, setStepStatuses] = useState<Record<string, StepStatus>>(
    Object.fromEntries(CLUSTER_SETUP_STEPS.map((s) => [s.name, "pending"]))
  );

  function patchStep(name: string, status: StepStatus) {
    setStepStatuses((prev) => ({ ...prev, [name]: status }));
  }

  async function runSetup() {
    if (!localHost) { notify("Enter a hostname", "warning"); return; }
    if (!localUser) { notify("Enter a username", "warning"); return; }

    setHost(localHost);
    setUser(localUser);
    setPass(localPass);
    setRunning(true);
    setStepStatuses(Object.fromEntries(CLUSTER_SETUP_STEPS.map((s) => [s.name, "pending"])));

    try {
      const response = await fetch("/api/cluster/setup", {
        method: "POST",
        headers: {
          "Content-Type": "application/json",
          "X-Mapr-Host": localHost,
          "X-Mapr-User": localUser,
          "X-Mapr-Pass": localPass,
        },
        body: JSON.stringify({ host: localHost, user: localUser, password: localPass }),
      });

      if (!response.ok || !response.body) {
        notify(`Backend returned HTTP ${response.status}`, "negative");
        return;
      }

      const reader  = response.body.getReader();
      const decoder = new TextDecoder();
      let buf = "";
      let hadError = false;
      let clusterName = "";

      while (true) {
        const { done, value } = await reader.read();
        if (done) break;
        buf += decoder.decode(value, { stream: true });
        const lines = buf.split("\n");
        buf = lines.pop() ?? "";
        for (const line of lines) {
          if (!line.startsWith("data:")) continue;
          try {
            const data = JSON.parse(line.slice(5).trim()) as {
              name?: string; status?: string; message?: string; cluster_name?: string;
            };
            const { name, status, message, cluster_name } = data;
            if (cluster_name) clusterName = cluster_name;
            if (name) {
              const s: StepStatus =
                status === "check" ? "ok" : status === "error" ? "error" : "running";
              patchStep(name, s);
              if (status === "error") hadError = true;
            }
            if (message && status !== "running") {
              notify(message, status === "check" ? "positive" : "negative");
            }
          } catch { /* malformed line */ }
        }
      }

      if (hadError) {
        notify("Setup completed with errors — check steps above", "warning");
      } else {
        notify("Setup complete!", "positive");
        setClusterInfo({ name: clusterName || localHost, ip: localHost });
        setDemoMode(false);
      }
    } catch (e) {
      notify(`Setup failed: ${e}`, "negative");
    } finally {
      setRunning(false);
    }
  }

  const statusIcon: Record<StepStatus, React.ReactNode> = {
    pending: <span className="w-4 h-4 rounded-full border border-neutrals-dark inline-block" />,
    running: <RiLoader4Line size={16} className="text-brand-contrast animate-spin" />,
    ok:      <RiCheckLine  size={16} className="text-status-good" />,
    error:   <RiErrorWarningLine size={16} className="text-status-failed" />,
  };

  return (
    <div className="dialog-backdrop" onClick={onClose}>
      <div
        className="w-full max-w-lg rounded-lg flex flex-col overflow-hidden"
        style={{
          background: "rgba(18, 18, 18, 0.96)",
          backdropFilter: "blur(24px)",
          WebkitBackdropFilter: "blur(24px)",
          border: "2px solid white",
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-6 pt-6 pb-4">
          <h2 className="font-sans font-bold text-[21px] text-white leading-tight">
            Connect to Data Fabric
          </h2>
          <button
            onClick={onClose}
            className="text-neutrals-medium hover:text-white transition-colors duration-200 p-1"
          >
            <RiCloseLine size={20} />
          </button>
        </div>

        <NexusSectionDivider style={{ paddingLeft: 24, marginBottom: 0 }} />

        <div className="px-6 py-5 flex flex-col gap-8">
          {/* Credentials */}
          <div className="flex flex-col gap-3">
            <DarkInput label="Hostname / IP Address" value={localHost} onChange={setLocalHost} />
            <DarkInput label="Username"              value={localUser} onChange={setLocalUser} />
            <DarkInput label="Password"              value={localPass} onChange={setLocalPass} type="password" />
          </div>

          {/* Setup steps */}
          <div className="flex flex-col gap-3">
            {CLUSTER_SETUP_STEPS.map((step) => {
              const status = stepStatuses[step.name] ?? "pending";
              return (
                <div key={step.name} className="flex items-center gap-3">
                  <span className="shrink-0">{statusIcon[status]}</span>
                  <span className="font-sans font-light text-sm text-neutrals-light">{step.info}</span>
                </div>
              );
            })}
          </div>

          {/* Action */}
          <div className="flex justify-end pb-1">
            <button
              onClick={runSetup}
              disabled={running}
              className="flex items-center gap-2 px-6 py-2.5 rounded font-sans font-medium text-sm text-white disabled:opacity-50 disabled:cursor-not-allowed transition-colors duration-200"
              style={{ background: running ? "#474747" : "#F2561D" }}
            >
              {running && <RiLoader4Line size={16} className="animate-spin" />}
              Connect &amp; Setup
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

function DarkInput({
  label,
  value,
  onChange,
  type = "text",
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
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
        onChange={(e) => onChange(e.target.value)}
        className="rounded px-3 py-2 text-sm font-sans text-white focus:outline-none transition-colors duration-200"
        style={{
          background: "#000000",
          border: "1px solid #474747",
        }}
        onFocus={(e) => (e.currentTarget.style.borderColor = "#F2561D")}
        onBlur={(e)  => (e.currentTarget.style.borderColor = "#474747")}
      />
    </div>
  );
}
