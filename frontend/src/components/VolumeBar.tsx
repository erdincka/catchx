"use client";

import { useCallback, useState } from "react";
import { RiFolderOpenLine, RiTerminalBoxLine } from "@remixicon/react";
import { Dialog } from "@/components/ui/Dialog";
import { Button, EmptyState, Spinner, Code } from "@/components/ui";
import { useSettings } from "@/contexts/SettingsContext";
import { apiGet } from "@/lib/api";

/**
 * Browse the Data Fabric global namespace.
 *
 * Every tier is an ordinary directory under /mapr — being able to `ls` them
 * from the app is the point, not a debugging aid.
 */
export default function VolumeBar() {
  const { clusterInfo, settings } = useSettings();
  const name = clusterInfo?.name ?? "";
  const base = settings?.targets.base_volume ?? "/catchx-demo";

  const [view, setView] = useState<{
    label: string;
    path: string;
    output?: string;
    error?: string;
    loading: boolean;
  } | null>(null);

  const explore = useCallback(async (label: string, path: string) => {
    setView({ label, path, loading: true });
    try {
      const d = await apiGet<{ output: string; path: string }>("/api/data/fs/list", { path });
      setView({ label, path: d.path ?? path, output: d.output, loading: false });
    } catch (e) {
      setView({
        label, path, loading: false,
        error: e instanceof Error ? e.message : "Could not list this path",
      });
    }
  }, []);

  const targets = [
    { label: "Global namespace", path: "/mapr", always: true },
    { label: "Demo", path: `/mapr/${name}${base}`, always: false },
    { label: "Bronze", path: `/mapr/${name}${base}/bronze`, always: false },
    { label: "Silver", path: `/mapr/${name}${base}/silver`, always: false },
    { label: "Gold", path: `/mapr/${name}${base}/gold`, always: false },
  ].filter((t) => t.always || name);

  return (
    <>
      <div className="h-9 px-3 flex items-center gap-1.5 overflow-x-auto">
        <RiFolderOpenLine size={12} className="text-subtle shrink-0" />
        <span className="text-[10px] uppercase tracking-[0.14em] text-subtle shrink-0 mr-1">
          Browse
        </span>
        {targets.map((t) => (
          <button
            key={t.label}
            onClick={() => explore(t.label, t.path)}
            title={t.path}
            className="px-2 h-6 rounded-md text-[11px] text-muted whitespace-nowrap
                       hover:text-text hover:bg-surface-hover transition-colors"
          >
            {t.label}
          </button>
        ))}
        <div className="flex-1" />
        {name && (
          <span className="text-[10px] text-subtle font-mono shrink-0 pr-1">{name}</span>
        )}
      </div>

      <Dialog
        open={Boolean(view)}
        onClose={() => setView(null)}
        size="xl"
        title={view?.label ?? ""}
        description={view && <Code>{view.path}</Code>}
      >
        {view?.loading && (
          <div className="flex items-center justify-center gap-2 py-16 text-muted text-[13px]">
            <Spinner /> Listing…
          </div>
        )}
        {view && !view.loading && view.error && (
          <EmptyState
            icon={<RiTerminalBoxLine size={24} />}
            title="Could not list this path"
            hint={view.error}
          />
        )}
        {view && !view.loading && view.output !== undefined && (
          <pre className="m-0 p-4 overflow-auto font-mono text-[11.5px] leading-relaxed
                          bg-surface-sunk text-text whitespace-pre">
            {view.output.trim() || "(empty directory)"}
          </pre>
        )}
      </Dialog>
    </>
  );
}
