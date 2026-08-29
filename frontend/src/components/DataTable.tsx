"use client";

import React, { useMemo } from "react";
import { EmptyState } from "@/components/ui";

/** Columns that read best first when present. */
const PREFERRED = ["_id", "name", "amount", "fraud", "score", "category", "country"];

function renderCell(v: unknown): { text: string; muted: boolean } {
  if (v === null || v === undefined || v === "") return { text: "—", muted: true };
  if (typeof v === "boolean") return { text: v ? "true" : "false", muted: false };
  if (typeof v === "number") return { text: v.toLocaleString(), muted: false };
  if (typeof v === "object") return { text: JSON.stringify(v), muted: true };
  const s = String(v);
  // Masked PII arrives as a run of asterisks — call it out rather than
  // showing a meaningless string of stars.
  if (/^\*+$/.test(s)) return { text: "masked", muted: true };
  return { text: s, muted: false };
}

export default function DataTable({
  records,
  total,
  emptyHint,
}: {
  records: Record<string, unknown>[];
  total?: number;
  emptyHint?: string;
}) {
  const columns = useMemo(() => {
    const seen = new Set<string>();
    for (const row of records) for (const k of Object.keys(row)) seen.add(k);
    const all = [...seen];
    const first = PREFERRED.filter((c) => seen.has(c));
    return [...first, ...all.filter((c) => !first.includes(c))];
  }, [records]);

  if (records.length === 0) {
    return (
      <EmptyState
        title="No records"
        hint={emptyHint ?? "This table exists but has no rows yet."}
      />
    );
  }

  return (
    <div className="flex flex-col min-h-0">
      <div className="overflow-auto flex-1 min-h-0">
        <table className="w-full border-collapse text-[12px]">
          <thead className="sticky top-0 z-10">
            <tr>
              {columns.map((c) => (
                <th
                  key={c}
                  scope="col"
                  className="text-left font-semibold text-subtle whitespace-nowrap
                             px-3 py-2 bg-surface-sunk border-b border-border
                             text-[10.5px] uppercase tracking-[0.08em]"
                >
                  {c}
                </th>
              ))}
            </tr>
          </thead>
          <tbody>
            {records.map((row, i) => (
              <tr key={i} className="hover:bg-surface-hover transition-colors">
                {columns.map((c) => {
                  const { text, muted } = renderCell(row[c]);
                  return (
                    <td
                      key={c}
                      title={text}
                      className={
                        "px-3 py-1.5 border-b border-border whitespace-nowrap max-w-[22rem] truncate " +
                        (muted ? "text-subtle italic" : "text-text")
                      }
                    >
                      {text}
                    </td>
                  );
                })}
              </tr>
            ))}
          </tbody>
        </table>
      </div>

      <div className="shrink-0 px-3 py-2 border-t border-border text-[11px] text-muted">
        Showing {records.length.toLocaleString()}
        {typeof total === "number" && total > records.length
          ? ` of ${total.toLocaleString()} rows`
          : records.length === 1 ? " row" : " rows"}
      </div>
    </div>
  );
}
