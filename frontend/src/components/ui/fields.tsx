"use client";

import React, { useId, useState } from "react";
import { RiEyeLine, RiEyeOffLine } from "@remixicon/react";
import { cx } from "@/lib/cx";

const INPUT_BASE =
  "w-full h-9 px-2.5 rounded-lg bg-surface border border-border text-[13px] text-text " +
  "placeholder:text-subtle transition-colors duration-150 " +
  "hover:border-border-strong focus:border-accent focus:outline-none " +
  "focus:ring-2 focus:ring-[color-mix(in_srgb,var(--accent)_25%,transparent)] " +
  "disabled:opacity-50 disabled:cursor-not-allowed";

function Label({ htmlFor, children, hint }: { htmlFor: string; children: React.ReactNode; hint?: string }) {
  return (
    <label htmlFor={htmlFor} className="block">
      <span className="text-[12px] font-medium text-text">{children}</span>
      {hint && <span className="block text-[11px] text-muted mt-0.5 leading-snug">{hint}</span>}
    </label>
  );
}

export function TextField({
  label,
  hint,
  value,
  onChange,
  placeholder,
  mono,
  disabled,
  type = "text",
  autoComplete = "off",
}: {
  label: string;
  hint?: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  mono?: boolean;
  disabled?: boolean;
  type?: string;
  autoComplete?: string;
}) {
  const id = useId();
  return (
    <div className="flex flex-col gap-1.5">
      <Label htmlFor={id} hint={hint}>{label}</Label>
      <input
        id={id}
        type={type}
        value={value}
        disabled={disabled}
        autoComplete={autoComplete}
        placeholder={placeholder}
        onChange={(e) => onChange(e.target.value)}
        className={cx(INPUT_BASE, mono && "font-mono text-[12px]")}
      />
    </div>
  );
}

export function PasswordField({
  label,
  hint,
  value,
  onChange,
  placeholder,
  disabled,
}: {
  label: string;
  hint?: string;
  value: string;
  onChange: (v: string) => void;
  placeholder?: string;
  disabled?: boolean;
}) {
  const id = useId();
  const [reveal, setReveal] = useState(false);
  return (
    <div className="flex flex-col gap-1.5">
      <Label htmlFor={id} hint={hint}>{label}</Label>
      <div className="relative">
        <input
          id={id}
          type={reveal ? "text" : "password"}
          value={value}
          disabled={disabled}
          placeholder={placeholder}
          autoComplete="current-password"
          onChange={(e) => onChange(e.target.value)}
          className={cx(INPUT_BASE, "pr-9")}
        />
        <button
          type="button"
          tabIndex={-1}
          onClick={() => setReveal((r) => !r)}
          aria-label={reveal ? "Hide password" : "Show password"}
          className="absolute right-1 top-1/2 -translate-y-1/2 p-1.5 rounded-md text-subtle
                     hover:text-text hover:bg-surface-hover transition-colors"
        >
          {reveal ? <RiEyeOffLine size={14} /> : <RiEyeLine size={14} />}
        </button>
      </div>
    </div>
  );
}

export function NumberField({
  label,
  hint,
  value,
  onChange,
  min = 1,
  max = 10000,
  disabled,
}: {
  label: string;
  hint?: string;
  value: number;
  onChange: (v: number) => void;
  min?: number;
  max?: number;
  disabled?: boolean;
}) {
  const id = useId();
  return (
    <div className="flex flex-col gap-1.5">
      <Label htmlFor={id} hint={hint}>{label}</Label>
      <input
        id={id}
        type="number"
        min={min}
        max={max}
        value={value}
        disabled={disabled}
        onChange={(e) => {
          const n = Number(e.target.value);
          if (!Number.isNaN(n)) onChange(Math.min(max, Math.max(min, n)));
        }}
        className={cx(INPUT_BASE, "font-mono")}
      />
    </div>
  );
}

export function Toggle({
  label,
  hint,
  checked,
  onChange,
  disabled,
}: {
  label: string;
  hint?: string;
  checked: boolean;
  onChange: (v: boolean) => void;
  disabled?: boolean;
}) {
  return (
    <button
      type="button"
      role="switch"
      aria-checked={checked}
      disabled={disabled}
      onClick={() => onChange(!checked)}
      className="flex items-start gap-3 w-full text-left group disabled:opacity-50 disabled:cursor-not-allowed"
    >
      <span
        className={cx(
          "mt-0.5 relative w-8 h-[18px] rounded-full shrink-0 transition-colors duration-200",
          checked ? "bg-accent" : "bg-border-strong",
        )}
      >
        <span
          className={cx(
            "absolute top-[2px] w-[14px] h-[14px] rounded-full bg-white shadow-sm transition-transform duration-200",
            checked ? "translate-x-[16px]" : "translate-x-[2px]",
          )}
        />
      </span>
      <span className="min-w-0">
        <span className="block text-[12px] font-medium text-text">{label}</span>
        {hint && <span className="block text-[11px] text-muted mt-0.5 leading-snug">{hint}</span>}
      </span>
    </button>
  );
}
