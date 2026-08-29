"use client";

import React, { useEffect, useRef } from "react";
import { RiCloseLine } from "@remixicon/react";
import { cx } from "@/lib/cx";

type Size = "md" | "lg" | "xl";

const SIZES: Record<Size, string> = {
  md: "max-w-lg",
  lg: "max-w-2xl",
  xl: "max-w-5xl",
};

/**
 * Modal dialog. Closes on Escape and backdrop click, restores focus on exit,
 * and locks body scroll while open.
 */
export function Dialog({
  open,
  onClose,
  title,
  description,
  size = "lg",
  footer,
  children,
}: {
  open: boolean;
  onClose: () => void;
  title: React.ReactNode;
  description?: React.ReactNode;
  size?: Size;
  footer?: React.ReactNode;
  children: React.ReactNode;
}) {
  const panelRef = useRef<HTMLDivElement>(null);
  const restoreTo = useRef<HTMLElement | null>(null);

  useEffect(() => {
    if (!open) return;

    restoreTo.current = document.activeElement as HTMLElement | null;
    const prevOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    panelRef.current?.focus();

    const onKey = (e: KeyboardEvent) => {
      if (e.key === "Escape") {
        e.stopPropagation();
        onClose();
      }
    };
    document.addEventListener("keydown", onKey);

    return () => {
      document.removeEventListener("keydown", onKey);
      document.body.style.overflow = prevOverflow;
      restoreTo.current?.focus?.();
    };
  }, [open, onClose]);

  if (!open) return null;

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center p-4
                 bg-[rgb(0_0_0/0.45)] backdrop-blur-[3px]"
      onMouseDown={(e) => {
        if (e.target === e.currentTarget) onClose();
      }}
    >
      <div
        ref={panelRef}
        role="dialog"
        aria-modal="true"
        tabIndex={-1}
        className={cx(
          "w-full flex flex-col max-h-[88vh] rise outline-none",
          "bg-surface border border-border rounded-xl shadow-[var(--shadow-lg)]",
          SIZES[size],
        )}
      >
        <div className="flex items-start justify-between gap-4 px-5 py-4 border-b border-border shrink-0">
          <div className="min-w-0">
            <h2 className="text-[15px] font-semibold tracking-tight text-text">{title}</h2>
            {description && (
              <p className="text-[12px] text-muted mt-0.5 leading-snug">{description}</p>
            )}
          </div>
          <button
            onClick={onClose}
            aria-label="Close"
            className="p-1.5 -m-1 rounded-md text-subtle hover:text-text hover:bg-surface-hover
                       transition-colors shrink-0"
          >
            <RiCloseLine size={18} />
          </button>
        </div>

        {/* Marked so content can find its scroll container without depending
            on how deeply it happens to be nested. */}
        <div data-dialog-scroll className="overflow-auto flex-1 min-h-0">{children}</div>

        {footer && (
          <div className="flex items-center justify-end gap-2 px-5 py-3 border-t border-border shrink-0">
            {footer}
          </div>
        )}
      </div>
    </div>
  );
}
