"use client";

import { InputHTMLAttributes } from "react";
import { cx } from "@/lib/cx";

interface FieldProps extends Omit<InputHTMLAttributes<HTMLInputElement>, "onChange"> {
  label: string;
  hint?: string;
  resolvedHint?: string;
  onChange: (v: string) => void;
}

export function Field({ label, hint, resolvedHint, value, onChange, type = "text", className, ...rest }: FieldProps) {
  return (
    <label className={cx("flex flex-col gap-1.5 min-w-0", className)}>
      <span className="text-xs uppercase tracking-[0.15em] text-neutrals-medium">{label}</span>
      <input
        {...rest}
        type={type}
        value={value ?? ""}
        onChange={(e) => onChange(e.target.value)}
        className={cx(
          "bg-[#0a0a0a] border border-neutrals-dark rounded-lg px-3 py-2",
          "text-sm text-white font-mono",
          "focus:outline-none focus:border-brand-vivid",
          "placeholder:text-neutrals-dark",
        )}
      />
      {resolvedHint && resolvedHint !== value && (
        <span className="text-[11px] text-neutrals-medium font-mono truncate">→ {resolvedHint}</span>
      )}
      {hint && <span className="text-[11px] text-neutrals-medium">{hint}</span>}
    </label>
  );
}
