"use client";

import { useState } from "react";
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

export default function MeshPage() {
  const router = useRouter();
  const { host, user, pass, clusterInfo, settings } = useCluster();
  const { notify } = useToast();

  const [showConnect, setShowConnect] = useState(false);
  const [showSettings, setShowSettings] = useState(false);
  const [explorer, setExplorer] = useState<{ title: string; path: string; output: string } | null>(null);

  // No auto-popup — the "Not connected" indicator in the header is sufficient

  function handleRegion(id: string) {
    switch (id) {
      case "Fraud":
        router.push("/fraud");
        break;
      case "NFS":
        notify("NFS mount — use the NFS Upload button to copy customers.csv into the cluster.", "info");
        break;
      case "S3": {
        const ep = settings.s3Server || host;
        if (ep) window.open(`http://${ep}:9000`, "_blank");
        else notify("Configure cluster host in Settings to open the Object Store console.", "warning");
        break;
      }
      case "IAM":
        if (host) window.open(`https://${host}:8443/app/mcs/`, "_blank");
        else notify("Configure cluster host in Settings first.", "warning");
        break;
      case "Policies":
        notify("Data Policies: governance rules are enforced at platform level across all domains.", "info");
        break;
      case "Catalogue":
        notify("Data Catalogue: schema registry and lineage available via the Polaris REST catalog.", "info");
        break;
      default:
        if (id) notify(`${id}: hover to learn about this domain.`, "info");
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
          Eight autonomous data domains unified by the Data Fabric platform — Global Namespace, governed access,
          and integrated NFS and S3 sources. Hover any domain to learn its data products.{" "}
          Click <span className="text-brand-vivid font-medium">Fraud & Risk</span> to explore the live pipeline.
        </p>
      </motion.div>

      <main className="flex-1 h-0 overflow-hidden relative">
        <MeshDiagram onRegionClick={handleRegion} />
      </main>

      <Footer onVolumeExplore={handleVolumeExplore} />

      {/* Modals & panels */}
      {showConnect && <ConnectDialog onClose={() => setShowConnect(false)} />}
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
