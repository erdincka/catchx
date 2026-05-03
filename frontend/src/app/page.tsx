"use client";

import { useState, useEffect } from "react";
import { useRouter } from "next/navigation";
import { motion } from "framer-motion";
import Header from "@/components/Header";
import Footer from "@/components/Footer";
import MeshDiagram from "@/components/MeshDiagram";
import ConnectDialog from "@/components/ConnectDialog";
import SettingsDialog from "@/components/SettingsDialog";
import DataExplorer, { ExplorerFilesystem } from "@/components/DataExplorer";
import { NexusSectionDivider } from "@/components/nexus-core-components";
import { useCluster } from "@/contexts/ClusterContext";
import { useToast } from "@/contexts/ToastContext";
import { RiUploadLine } from "@remixicon/react";

export default function MeshPage() {
  const router = useRouter();
  const { host, user, pass, clusterInfo, settings } = useCluster();
  const { notify } = useToast();

  const [showConnect,  setShowConnect]  = useState(false);
  const [showSettings, setShowSettings] = useState(false);
  const [explorer,     setExplorer]     = useState<{ title: string; path: string; output: string } | null>(null);

  // Auto-open connect dialog on first visit if no cluster stored
  useEffect(() => {
    if (!host) {
      const timer = setTimeout(() => setShowConnect(true), 600);
      return () => clearTimeout(timer);
    }
  }, []);

  function handleRegion(id: string) {
    switch (id) {
      case "Fraud":
        router.push("/fraud");
        break;
      case "NFS":
        notify("NFS: click the upload button to copy customers.csv to the mount path.", "info");
        break;
      case "S3": {
        const s3 = settings.s3Server || "localhost:9000";
        window.open(`http://${s3}`, "_blank");
        break;
      }
      case "IAM":
        if (host) window.open(`https://${host}:8443/app/dfui/#/login`, "_blank");
        else notify("Connect to a cluster first.", "warning");
        break;
      case "Policies":
      case "Edge":
        notify(`${id}: informational — no action configured.`, "info");
        break;
      case "Catalogue": {
        const url = settings.catalogueUrl;
        if (!url) notify("Set Catalogue URL in Settings.", "warning");
        else window.open(url, "_blank");
        break;
      }
      default:
        if (id) notify(`${id}: not configured yet.`, "info");
    }
  }

  async function handleNfsUpload() {
    const name = clusterInfo?.name ?? "";
    const r = await fetch(`/api/data/fs/nfs-upload?cluster=${name}`, {
      method: "POST",
      headers: { "X-Mapr-Host": host, "X-Mapr-User": user, "X-Mapr-Pass": pass },
    });
    const d = await r.json();
    if (r.ok) notify(d.message ?? "Uploaded.", "positive");
    else notify(d.detail ?? "Upload failed.", "negative");
  }

  async function handleS3Upload() {
    const { s3Server, s3AccessKey, s3SecretKey } = settings;
    if (!s3Server || !s3SecretKey) { notify("Configure S3 settings first.", "warning"); return; }
    const r = await fetch(
      `/api/data/s3/upload?s3_server=${s3Server}&access_key=${s3AccessKey}&secret_key=${s3SecretKey}`,
      { method: "POST", headers: { "X-Mapr-Host": host, "X-Mapr-User": user, "X-Mapr-Pass": pass } }
    );
    const d = await r.json();
    if (r.ok) notify(d.message ?? "Uploaded.", "positive");
    else notify(d.detail ?? "Upload failed.", "negative");
  }

  async function handleVolumeExplore(label: string, path: string) {
    setExplorer({ title: `Exploring: ${label}`, path, output: "Loading…" });
    try {
      const r = await fetch(`/api/data/fs/list?path=${encodeURIComponent(path)}`, {
        headers: { "X-Mapr-Host": host, "X-Mapr-User": user, "X-Mapr-Pass": pass },
      });
      const data = await r.json();
      const output = data.output ?? JSON.stringify(data, null, 2);
      setExplorer({ title: `Exploring: ${label}`, path, output });
    } catch (e) {
      setExplorer({ title: `Exploring: ${label}`, path, output: String(e) });
    }
  }

  return (
    <div className="flex flex-col h-screen bg-neutrals-deep">
      <Header
        onConnectClick={() => setShowConnect(true)}
        onSettingsClick={() => setShowSettings(true)}
      />

      {/* Page hero label */}
      <motion.div
        className="shrink-0 px-8 pt-[88px] pb-2"
        initial={{ opacity: 0, y: -8 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5, delay: 0.1, ease: [0.22, 1, 0.36, 1] }}
      >
        <NexusSectionDivider
          // @ts-ignore
          title="Nexus Hybrid Data Mesh"
          style={{ paddingLeft: 0 }}
        />
        <p className="font-sans font-light text-sm text-neutrals-medium mt-2 tracking-wide max-w-2xl">
          Seven autonomous data domains connected via Global Namespace, unified by platform-level governance,
          security, and observability. Hover any domain to learn its purpose. Click <span className="text-brand-vivid font-medium">Fraud</span> to explore the live pipeline.
        </p>
      </motion.div>

      <main className="flex-1 overflow-hidden relative">
        <MeshDiagram onRegionClick={handleRegion}>
          {/* External integration upload buttons */}
          {settings.nfsPath && (
            <button
              onClick={handleNfsUpload}
              title="Upload Customers via NFS"
              className="absolute top-[84px] left-2 flex items-center gap-1.5 font-sans text-[10px] text-neutrals-light hover:text-brand-vivid rounded px-2.5 py-1.5 transition-colors duration-200 z-20 uppercase tracking-wider"
              style={{ background: "#121212", border: "1px solid #474747" }}
              onMouseEnter={(e) => (e.currentTarget.style.borderColor = "#F2561D")}
              onMouseLeave={(e) => (e.currentTarget.style.borderColor = "#474747")}
            >
              <RiUploadLine size={11} /> NFS Upload
            </button>
          )}
          {settings.s3Server && (
            <button
              onClick={handleS3Upload}
              title="Upload Transactions to S3"
              className="absolute top-[84px] right-2 flex items-center gap-1.5 font-sans text-[10px] text-neutrals-light hover:text-brand-vivid rounded px-2.5 py-1.5 transition-colors duration-200 z-20 uppercase tracking-wider"
              style={{ background: "#121212", border: "1px solid #474747" }}
              onMouseEnter={(e) => (e.currentTarget.style.borderColor = "#F2561D")}
              onMouseLeave={(e) => (e.currentTarget.style.borderColor = "#474747")}
            >
              <RiUploadLine size={11} /> S3 Upload
            </button>
          )}
        </MeshDiagram>
      </main>

      <Footer onVolumeExplore={handleVolumeExplore} />

      {/* Modals & panels */}
      {showConnect  && <ConnectDialog onClose={() => setShowConnect(false)} />}
      {showSettings && <SettingsDialog onClose={() => setShowSettings(false)} />}

      <DataExplorer
        title={explorer?.title ?? ""}
        isOpen={!!explorer}
        onClose={() => setExplorer(null)}
      >
        {explorer && (
          <ExplorerFilesystem path={explorer.path} output={explorer.output} />
        )}
      </DataExplorer>
    </div>
  );
}
