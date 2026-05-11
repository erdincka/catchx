"use client";

import { cx } from "@/lib/cx";

export type ServiceStatus = "good" | "degraded" | "failed" | "unknown";

interface StatusDotProps {
  status: ServiceStatus;
  pulse?: boolean;
  size?: number;
  className?: string;
  title?: string;
}

const COLOURS: Record<ServiceStatus, string> = {
  good:     "#008C48",
  degraded: "#F27D00",
  failed:   "#BF0300",
  unknown:  "#64748B",
};

export function StatusDot({ status, pulse = false, size = 10, className, title }: StatusDotProps) {
  return (
    <span
      className={cx("inline-block rounded-full shrink-0", pulse && status === "good" && "status-pulse-dot", className)}
      style={{ width: size, height: size, background: COLOURS[status] }}
      title={title}
      aria-label={title ?? `status: ${status}`}
    />
  );
}
