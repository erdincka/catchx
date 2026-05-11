"use client";

import { ButtonHTMLAttributes, forwardRef } from "react";
import { cx } from "@/lib/cx";

type Size = "sm" | "md" | "lg";

interface PrimaryButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  size?: Size;
}

const SIZE_CLASSES: Record<Size, string> = {
  sm: "text-xs px-4 py-2",
  md: "text-sm px-6 py-3",
  lg: "text-base px-8 py-3.5",
};

export const PrimaryButton = forwardRef<HTMLButtonElement, PrimaryButtonProps>(
  function PrimaryButton({ size = "md", className, children, ...rest }, ref) {
    return (
      <button
        ref={ref}
        {...rest}
        className={cx(
          "bg-brand-vivid text-white font-medium rounded-full",
          "hover:brightness-110 hover:shadow-lg hover:shadow-brand-vivid/20",
          "transition-all duration-200 active:scale-[0.97]",
          "disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:brightness-100",
          SIZE_CLASSES[size],
          className,
        )}
      >
        {children}
      </button>
    );
  },
);
