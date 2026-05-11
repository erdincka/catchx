"use client";

import { cx } from "@/lib/cx";

interface SectionHeaderProps {
  title: string;
  className?: string;
}

export function SectionHeader({ title, className }: SectionHeaderProps) {
  return (
    <div className={cx("flex flex-col gap-3 pl-12 mb-12", className)}>
      <h2 className="text-white uppercase tracking-widest font-medium text-2xl m-0">
        {title}
      </h2>
      <div className="nexus-gradient-h h-[3px] -ml-12 max-w-3xl" />
    </div>
  );
}
