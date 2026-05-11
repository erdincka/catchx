"use client";

import { ButtonHTMLAttributes, forwardRef } from "react";
import { cx } from "@/lib/cx";

type Size = "sm" | "md" | "lg";

interface TealButtonProps extends ButtonHTMLAttributes<HTMLButtonElement> {
  size?: Size;
}

const SIZE_CLASSES: Record<Size, string> = {
  sm: "text-xs px-4 py-2",
  md: "text-sm px-6 py-3",
  lg: "text-base px-8 py-3.5",
};

export const TealButton = forwardRef<HTMLButtonElement, TealButtonProps>(
  function TealButton({ size = "md", className, children, ...rest }, ref) {
    return (
      <button
        ref={ref}
        {...rest}
        className={cx(
          "bg-brand-contrast text-white font-medium rounded-full",
          "hover:brightness-110 hover:shadow-lg hover:shadow-brand-contrast/20 hover:scale-[1.02]",
          "transition-all duration-300 active:scale-[0.97]",
          "disabled:opacity-50 disabled:cursor-not-allowed disabled:hover:brightness-100 disabled:hover:scale-100",
          SIZE_CLASSES[size],
          className,
        )}
      >
        {children}
      </button>
    );
  },
);
