"use client";

import { cx } from "@/lib/cx";

interface SelectFieldProps<T extends string> {
  label: string;
  hint?: string;
  value: T;
  options: ReadonlyArray<{ value: T; label: string }>;
  onChange: (v: T) => void;
  className?: string;
}

export function SelectField<T extends string>({ label, hint, value, options, onChange, className }: SelectFieldProps<T>) {
  return (
    <label className={cx("flex flex-col gap-1.5 min-w-0", className)}>
      <span className="text-xs uppercase tracking-[0.15em] text-neutrals-medium">{label}</span>
      <select
        value={value}
        onChange={(e) => onChange(e.target.value as T)}
        className={cx(
          "bg-[#0a0a0a] border border-neutrals-dark rounded-lg px-3 py-2",
          "text-sm text-white",
          "focus:outline-none focus:border-brand-vivid",
        )}
      >
        {options.map((o) => (
          <option key={o.value} value={o.value}>{o.label}</option>
        ))}
      </select>
      {hint && <span className="text-[11px] text-neutrals-medium">{hint}</span>}
    </label>
  );
}
