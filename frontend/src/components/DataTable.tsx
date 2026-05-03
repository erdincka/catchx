"use client";

import { RiCloseLine } from "@remixicon/react";
import { NexusSectionDivider } from "@/components/nexus-core-components";

interface DataTableProps {
  title: string;
  records: Record<string, unknown>[];
  onClose: () => void;
}

export default function DataTable({ title, records, onClose }: DataTableProps) {
  if (!records.length) return null;

  const columns = Object.keys(records[0]);

  return (
    <div className="dialog-backdrop" onClick={onClose}>
      <div
        className="w-full max-w-5xl rounded-lg flex flex-col max-h-[90vh]"
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
            <span className="font-sans font-bold text-sm text-white">{title}</span>
            <span className="font-sans font-light text-xs text-neutrals-medium">{records.length} records</span>
          </div>
          <button
            onClick={onClose}
            className="text-neutrals-medium hover:text-white transition-colors duration-200 p-1"
          >
            <RiCloseLine size={20} />
          </button>
        </div>

        <NexusSectionDivider style={{ paddingLeft: 20, marginBottom: 0 }} />

        <div className="overflow-auto flex-1">
          <table className="w-full text-xs border-collapse">
            <thead>
              <tr className="sticky top-0" style={{ background: "#121212" }}>
                {columns.map((col) => (
                  <th
                    key={col}
                    className="text-left px-3 py-2.5 font-sans font-medium text-neutrals-light whitespace-nowrap uppercase tracking-wider text-[10px]"
                    style={{ borderBottom: "1px solid #474747" }}
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
                  style={{ background: i % 2 === 0 ? "#000000" : "#121212" }}
                >
                  {columns.map((col) => (
                    <td
                      key={col}
                      className="px-3 py-1.5 font-sans font-light text-neutrals-light max-w-xs truncate"
                      style={{ borderBottom: "1px solid #1a1a1a" }}
                      title={String(row[col] ?? "")}
                    >
                      {String(row[col] ?? "")}
                    </td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}
