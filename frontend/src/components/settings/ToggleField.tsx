"use client";

import { cx } from "@/lib/cx";

interface ToggleFieldProps {
  label: string;
  hint?: string;
  value: boolean;
  onChange: (v: boolean) => void;
  className?: string;
}

export function ToggleField({ label, hint, value, onChange, className }: ToggleFieldProps) {
  return (
    <label className={cx("flex items-center justify-between gap-4 cursor-pointer select-none", className)}>
      <div className="flex flex-col gap-0.5 min-w-0">
        <span className="text-sm text-white">{label}</span>
        {hint && <span className="text-[11px] text-neutrals-medium">{hint}</span>}
      </div>
      <div
        role="switch"
        aria-checked={value}
        onClick={() => onChange(!value)}
        className="relative w-10 h-5 rounded-full transition-colors duration-200 shrink-0"
        style={{ background: value ? "#F2561D" : "#474747" }}
      >
        <div
          className="absolute top-0.5 w-4 h-4 bg-white rounded-full shadow transition-transform duration-200"
          style={{ transform: value ? "translateX(22px)" : "translateX(2px)" }}
        />
      </div>
    </label>
  );
}
