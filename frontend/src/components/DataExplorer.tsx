"use client";

import { motion, AnimatePresence } from "framer-motion";
import { NexusSectionDivider } from "@/components/nexus-core-components";
import { RiCloseLine } from "@remixicon/react";

interface DataExplorerProps {
  title: string;
  isOpen: boolean;
  onClose: () => void;
  children: React.ReactNode;
}

export default function DataExplorer({ title, isOpen, onClose, children }: DataExplorerProps) {
  return (
    <AnimatePresence>
      {isOpen && (
        <>
          {/* Dim backdrop — click to close */}
          <motion.div
            className="fixed inset-0 z-40"
            style={{ background: "rgba(0,0,0,0.45)", backdropFilter: "blur(2px)", WebkitBackdropFilter: "blur(2px)" }}
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.2 }}
            onClick={onClose}
          />

          {/* Slide-in panel */}
          <motion.div
            className="fixed top-[80px] right-0 bottom-0 z-50 flex flex-col"
            style={{
              width: 420,
              background: "rgba(12,12,12,0.98)",
              backdropFilter: "blur(24px)",
              WebkitBackdropFilter: "blur(24px)",
              borderLeft: "1px solid rgba(255,255,255,0.10)",
            }}
            initial={{ x: 440 }}
            animate={{ x: 0 }}
            exit={{ x: 440 }}
            transition={{ duration: 0.38, ease: [0.22, 1, 0.36, 1] }}
          >
            {/* Panel header */}
            <div
              className="flex items-center justify-between px-5 py-4 shrink-0"
              style={{ borderBottom: "1px solid rgba(255,255,255,0.08)" }}
            >
              <NexusSectionDivider
                // @ts-ignore
                title={title}
                style={{ paddingLeft: 0, marginBottom: 0, flex: 1 }}
              />
              <button
                onClick={onClose}
                className="ml-4 p-1 text-neutrals-medium hover:text-white transition-colors duration-200 shrink-0"
              >
                <RiCloseLine size={18} />
              </button>
            </div>

            {/* Scrollable content */}
            <div className="flex-1 overflow-auto p-4">
              {children}
            </div>
          </motion.div>
        </>
      )}
    </AnimatePresence>
  );
}

// ── DataTable content for use inside DataExplorer ─────────────────────────────

export function ExplorerTable({ records }: { records: Record<string, unknown>[] }) {
  if (!records.length) {
    return (
      <div className="flex flex-col items-center justify-center py-16 gap-3">
        <span className="font-sans font-light text-neutrals-medium text-sm">No records found</span>
      </div>
    );
  }

  const columns = Object.keys(records[0]);

  return (
    <div className="overflow-auto rounded-lg" style={{ border: "1px solid #474747" }}>
      <table className="w-full text-left font-sans text-[11px]" style={{ minWidth: "max-content" }}>
        <thead>
          <tr style={{ background: "#1a1a1a", borderBottom: "1px solid #474747" }}>
            {columns.map((col) => (
              <th
                key={col}
                className="px-3 py-2 font-semibold text-neutrals-light uppercase tracking-wider whitespace-nowrap"
                style={{ fontSize: "10px" }}
              >
                {col}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {records.map((row, i) => (
            <tr
              key={i}
              style={{ background: i % 2 === 0 ? "#000000" : "#0d0d0d", borderBottom: "1px solid #1a1a1a" }}
            >
              {columns.map((col) => {
                const val = String(row[col] ?? "");
                return (
                  <td
                    key={col}
                    className="px-3 py-1.5 text-neutrals-light font-light whitespace-nowrap max-w-[180px] overflow-hidden text-ellipsis"
                    title={val}
                  >
                    {val}
                  </td>
                );
              })}
            </tr>
          ))}
        </tbody>
      </table>
      <div
        className="px-3 py-2 font-sans font-light text-[10px] text-neutrals-dark uppercase tracking-wider"
        style={{ borderTop: "1px solid #1a1a1a" }}
      >
        {records.length} record{records.length !== 1 ? "s" : ""}
      </div>
    </div>
  );
}

// ── Filesystem output for use inside DataExplorer ─────────────────────────────

export function ExplorerFilesystem({ path, output }: { path: string; output: string }) {
  return (
    <div className="flex flex-col gap-3">
      <div
        className="flex items-center gap-2 px-3 py-2 rounded font-mono text-xs"
        style={{ background: "#0a0a0a", border: "1px solid #2a2a2a" }}
      >
        <span className="text-brand-contrast">$</span>
        <span className="text-neutrals-light">ls {path}</span>
      </div>
      <pre
        className="text-status-good text-xs font-mono overflow-auto whitespace-pre-wrap rounded-lg p-3"
        style={{ background: "#0a0a0a", border: "1px solid #2a2a2a", maxHeight: "calc(100vh - 260px)" }}
      >
        {output}
      </pre>
    </div>
  );
}
