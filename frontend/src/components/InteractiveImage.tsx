"use client";

import { useRef } from "react";

interface SvgRegion {
  id: string;
  shape: "rect";
  x: number;
  y: number;
  width: number;
  height: number;
  rx?: number;
  ry?: number;
  fill: string;
  transform?: string;
}

interface InteractiveImageProps {
  src: string;
  /** SVG viewBox string, e.g. "0 0 7680 4320" */
  viewBox: string;
  regions: SvgRegion[];
  onRegionClick: (id: string) => void;
  className?: string;
  children?: React.ReactNode;
}

const OPACITY = 0.35;

export default function InteractiveImage({
  src,
  viewBox,
  regions,
  onRegionClick,
  className = "",
  children,
}: InteractiveImageProps) {
  const containerRef = useRef<HTMLDivElement>(null);

  return (
    <div ref={containerRef} className={`relative w-full h-full ${className}`}>
      {/* Base image */}
      {/* eslint-disable-next-line @next/next/no-img-element */}
      <img src={src} alt="diagram" className="w-full h-full object-contain" draggable={false} />

      {/* SVG overlay — same dimensions as image, pointer events only on regions */}
      <svg
        viewBox={viewBox}
        className="absolute inset-0 w-full h-full"
        style={{ pointerEvents: "none" }}
        preserveAspectRatio="xMidYMid meet"
      >
        {regions.map((r, i) => (
          <rect
            key={`${r.id}-${i}`}
            x={r.x}
            y={r.y}
            width={r.width}
            height={r.height}
            rx={r.rx ?? 0}
            ry={r.ry ?? 0}
            fill={r.fill}
            fillOpacity={OPACITY}
            stroke="none"
            transform={r.transform}
            style={{ pointerEvents: "all", cursor: "pointer" }}
            onPointerUp={() => onRegionClick(r.id)}
          />
        ))}
      </svg>

      {/* Absolute-positioned children (buttons, overlays) */}
      {children}
    </div>
  );
}

export type { SvgRegion };
