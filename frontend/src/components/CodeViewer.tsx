"use client";

import { useEffect, useRef, useState } from "react";
import { useCluster } from "@/contexts/ClusterContext";
import { useToast } from "@/contexts/ToastContext";
import { RiCloseLine, RiLoader4Line } from "@remixicon/react";
import { NexusSectionDivider } from "@/components/nexus-core-components";

interface CodeViewerProps {
  functionName: string;
  onClose: () => void;
}

export default function CodeViewer({ functionName, onClose }: CodeViewerProps) {
  const { host, user, pass, clusterInfo } = useCluster();
  const { notify } = useToast();
  const [source, setSource]   = useState("");
  const [module, setModule]   = useState("");
  const [loading, setLoading] = useState(true);
  const codeRef = useRef<HTMLElement>(null);

  useEffect(() => {
    async function load() {
      setLoading(true);
      try {
        const params = new URLSearchParams({
          cluster:   clusterInfo?.name ?? "",
          mapr_user: user,
          mapr_pass: pass,
        });
        const r = await fetch(`/api/code/${functionName}?${params}`, {
          headers: { "X-Mapr-Host": host, "X-Mapr-User": user, "X-Mapr-Pass": pass },
        });
        if (!r.ok) { notify(`Could not load code for '${functionName}'`, "negative"); onClose(); return; }
        const data = await r.json() as { source: string; module?: string };
        setSource(data.source ?? "");
        setModule(data.module ?? "");
      } catch (e) {
        notify(String(e), "negative");
        onClose();
      } finally {
        setLoading(false);
      }
    }
    load();
  }, [functionName]); // eslint-disable-line react-hooks/exhaustive-deps

  useEffect(() => {
    if (!source || !codeRef.current) return;
    import("highlight.js/lib/core").then(async ({ default: hljs }) => {
      const python = (await import("highlight.js/lib/languages/python")).default;
      const xml    = (await import("highlight.js/lib/languages/xml")).default;
      hljs.registerLanguage("python", python);
      hljs.registerLanguage("xml", xml);
      if (codeRef.current) hljs.highlightElement(codeRef.current);
    });
  }, [source]);

  return (
    <div className="dialog-backdrop" onClick={onClose}>
      <div
        className="w-full max-w-4xl rounded-lg flex flex-col max-h-[90vh]"
        style={{
          background: "rgba(18, 18, 18, 0.96)",
          backdropFilter: "blur(24px)",
          WebkitBackdropFilter: "blur(24px)",
          border: "2px solid white",
        }}
        onClick={(e) => e.stopPropagation()}
      >
        {/* Header */}
        <div className="flex items-center justify-between px-5 py-4 shrink-0">
          <div className="flex items-center gap-3">
            <span className="font-sans font-bold text-sm text-brand-vivid font-mono">{functionName}</span>
            {module && (
              <span className="font-sans font-light text-xs text-neutrals-medium">{module}</span>
            )}
          </div>
          <button
            onClick={onClose}
            className="text-neutrals-medium hover:text-white transition-colors duration-200 p-1"
          >
            <RiCloseLine size={20} />
          </button>
        </div>

        <NexusSectionDivider style={{ paddingLeft: 20, marginBottom: 0 }} />

        <div className="overflow-auto flex-1 p-4">
          {loading ? (
            <div className="flex items-center justify-center h-32 gap-2 text-neutrals-medium font-sans">
              <RiLoader4Line size={18} className="animate-spin text-brand-vivid" />
              Loading…
            </div>
          ) : (
            <pre className="rounded overflow-auto">
              <code ref={codeRef} className="language-python text-xs">
                {source}
              </code>
            </pre>
          )}
        </div>
      </div>
    </div>
  );
}
