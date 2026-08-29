"use client";

import React from "react";
import { cx } from "@/lib/cx";

/* ── Button ─────────────────────────────────────────────────────────────────*/

type ButtonVariant = "primary" | "secondary" | "ghost" | "danger";
type ButtonSize = "sm" | "md";

const BUTTON_VARIANTS: Record<ButtonVariant, string> = {
  primary:
    "bg-accent text-on-accent hover:bg-accent-hover border-transparent shadow-[var(--shadow-sm)]",
  secondary:
    "bg-surface text-text border-border hover:bg-surface-hover hover:border-border-strong",
  ghost:
    "bg-transparent text-muted border-transparent hover:bg-surface-hover hover:text-text",
  danger:
    "bg-transparent text-bad border-border hover:bg-bad-soft hover:border-bad",
};

const BUTTON_SIZES: Record<ButtonSize, string> = {
  sm: "h-7 px-2.5 text-[12px] gap-1.5 rounded-md",
  md: "h-9 px-3.5 text-[13px] gap-2 rounded-lg",
};

export function Button({
  variant = "secondary",
  size = "md",
  loading = false,
  icon,
  className,
  children,
  disabled,
  ...rest
}: React.ButtonHTMLAttributes<HTMLButtonElement> & {
  variant?: ButtonVariant;
  size?: ButtonSize;
  loading?: boolean;
  icon?: React.ReactNode;
}) {
  return (
    <button
      {...rest}
      disabled={disabled || loading}
      aria-busy={loading || undefined}
      className={cx(
        "inline-flex items-center justify-center border font-medium whitespace-nowrap",
        "transition-colors duration-150 select-none",
        "disabled:opacity-45 disabled:cursor-not-allowed disabled:hover:bg-inherit",
        BUTTON_VARIANTS[variant],
        BUTTON_SIZES[size],
        className,
      )}
    >
      {loading ? <Spinner size={size === "sm" ? 12 : 14} /> : icon}
      {children}
    </button>
  );
}

/* ── Spinner ────────────────────────────────────────────────────────────────*/

export function Spinner({ size = 14, className }: { size?: number; className?: string }) {
  return (
    <svg
      width={size}
      height={size}
      viewBox="0 0 16 16"
      className={cx("animate-spin shrink-0", className)}
      aria-hidden
    >
      <circle cx="8" cy="8" r="6.5" fill="none" stroke="currentColor" strokeWidth="2" opacity="0.22" />
      <path
        d="M8 1.5A6.5 6.5 0 0 1 14.5 8"
        fill="none"
        stroke="currentColor"
        strokeWidth="2"
        strokeLinecap="round"
      />
    </svg>
  );
}

/* ── Card ───────────────────────────────────────────────────────────────────*/

export function Card({
  className,
  children,
  ...rest
}: React.HTMLAttributes<HTMLDivElement>) {
  return (
    <div
      {...rest}
      className={cx(
        "bg-surface border border-border rounded-[var(--radius)]",
        className,
      )}
    >
      {children}
    </div>
  );
}

export function CardHeader({
  title,
  description,
  actions,
  className,
}: {
  title: React.ReactNode;
  description?: React.ReactNode;
  actions?: React.ReactNode;
  className?: string;
}) {
  return (
    <div className={cx("flex items-start justify-between gap-4 px-4 py-3 border-b border-border", className)}>
      <div className="min-w-0">
        <h2 className="text-[13px] font-semibold tracking-tight text-text">{title}</h2>
        {description && (
          <p className="text-[12px] text-muted mt-0.5 leading-snug">{description}</p>
        )}
      </div>
      {actions && <div className="flex items-center gap-2 shrink-0">{actions}</div>}
    </div>
  );
}

/* ── Section heading ────────────────────────────────────────────────────────*/

export function SectionTitle({
  children,
  className,
}: {
  children: React.ReactNode;
  className?: string;
}) {
  return (
    <h3
      className={cx(
        "text-[11px] font-semibold uppercase tracking-[0.14em] text-subtle",
        className,
      )}
    >
      {children}
    </h3>
  );
}

/* ── Badge ──────────────────────────────────────────────────────────────────*/

export type Tone = "neutral" | "accent" | "good" | "warn" | "bad" | "info";

const BADGE_TONES: Record<Tone, string> = {
  neutral: "bg-surface-sunk text-muted border-border",
  accent:  "bg-accent-soft text-accent-text border-transparent",
  good:    "bg-good-soft text-good border-transparent",
  warn:    "bg-warn-soft text-warn border-transparent",
  bad:     "bg-bad-soft text-bad border-transparent",
  info:    "bg-info-soft text-info border-transparent",
};

export function Badge({
  tone = "neutral",
  className,
  children,
}: {
  tone?: Tone;
  className?: string;
  children: React.ReactNode;
}) {
  return (
    <span
      className={cx(
        "inline-flex items-center gap-1 px-1.5 py-0.5 rounded-md border",
        "text-[11px] font-medium leading-none whitespace-nowrap",
        BADGE_TONES[tone],
        className,
      )}
    >
      {children}
    </span>
  );
}

/* ── Status dot ─────────────────────────────────────────────────────────────*/

const DOT_TONES: Record<Tone, string> = {
  neutral: "bg-[var(--text-subtle)]",
  accent:  "bg-accent",
  good:    "bg-good",
  warn:    "bg-warn",
  bad:     "bg-bad",
  info:    "bg-info",
};

export function StatusDot({
  tone = "neutral",
  pulse = false,
  size = 7,
  className,
}: {
  tone?: Tone;
  pulse?: boolean;
  size?: number;
  className?: string;
}) {
  return (
    <span
      style={{ width: size, height: size }}
      className={cx("rounded-full shrink-0", DOT_TONES[tone], pulse && "pulse-ring", className)}
      aria-hidden
    />
  );
}

/* ── Skeleton ───────────────────────────────────────────────────────────────*/

export function Skeleton({ className }: { className?: string }) {
  return <div className={cx("skeleton", className)} aria-hidden />;
}

/* ── Empty state ────────────────────────────────────────────────────────────*/

export function EmptyState({
  icon,
  title,
  hint,
  action,
  className,
}: {
  icon?: React.ReactNode;
  title: string;
  hint?: string;
  action?: React.ReactNode;
  className?: string;
}) {
  return (
    <div className={cx("flex flex-col items-center justify-center text-center px-6 py-10 gap-2", className)}>
      {icon && <div className="text-subtle mb-1">{icon}</div>}
      <p className="text-[13px] font-medium text-text">{title}</p>
      {hint && <p className="text-[12px] text-muted max-w-sm leading-relaxed">{hint}</p>}
      {action && <div className="mt-2">{action}</div>}
    </div>
  );
}

/* ── Stat ───────────────────────────────────────────────────────────────────*/

export function Stat({
  label,
  value,
  hint,
  tone = "neutral",
  className,
}: {
  label: string;
  value: React.ReactNode;
  hint?: string;
  tone?: Tone;
  className?: string;
}) {
  const valueTone =
    tone === "neutral" ? "text-text"
    : tone === "accent" ? "text-accent-text"
    : tone === "good" ? "text-good"
    : tone === "warn" ? "text-warn"
    : tone === "bad" ? "text-bad"
    : "text-info";

  return (
    <div className={cx("min-w-0", className)}>
      <div className="text-[10px] font-semibold uppercase tracking-[0.12em] text-subtle truncate">
        {label}
      </div>
      <div className={cx("font-mono text-[18px] leading-tight mt-0.5", valueTone)}>{value}</div>
      {hint && <div className="text-[11px] text-muted mt-0.5 truncate">{hint}</div>}
    </div>
  );
}

/* ── Inline code ────────────────────────────────────────────────────────────*/

export function Code({ children, className }: { children: React.ReactNode; className?: string }) {
  return (
    <code
      className={cx(
        "font-mono text-[12px] px-1 py-0.5 rounded bg-surface-sunk border border-border text-muted",
        className,
      )}
    >
      {children}
    </code>
  );
}
