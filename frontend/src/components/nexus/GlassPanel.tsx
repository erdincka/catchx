"use client";

import { HTMLAttributes, forwardRef } from "react";
import { cx } from "@/lib/cx";

type Variant = "dark" | "nav";

interface GlassPanelProps extends HTMLAttributes<HTMLDivElement> {
  variant?: Variant;
}

export const GlassPanel = forwardRef<HTMLDivElement, GlassPanelProps>(
  function GlassPanel({ variant = "dark", className, children, ...rest }, ref) {
    return (
      <div
        ref={ref}
        {...rest}
        className={cx(
          variant === "nav" ? "nexus-glass-nav" : "nexus-glass",
          "rounded-3xl",
          className,
        )}
      >
        {children}
      </div>
    );
  },
);
