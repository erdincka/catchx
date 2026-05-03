"use client";

import { useState } from "react";

/**
 * ──────────────────────────────────────────────────────────────────────────────
 *  NexusCard — Generic branded card container
 * ──────────────────────────────────────────────────────────────────────────────
 *
 *  Replicates the consistent card styling used across the Nexus intranet portal
 *  (AIToolCard, NewsCard, SiteCard, StatusCard). Provides the rounded corners,
 *  subtle white border, dark background surface, gradient overlay, and optional
 *  active/glow state — all controllable via props.
 *
 *  Props:
 *  ──────────────────────────────────────────────────────────────────────────
 *  @prop {React.ReactNode}  children
 *    Card content rendered inside the overlay layer.
 *
 *  @prop {"editorial" | "feature" | "status" | "display"}  [variant="editorial"]
 *    Visual preset controlling border-radius, border-width, and overlay style:
 *      - "editorial" — 10px radius, 2px white border (News/Site card style)
 *      - "feature"   — 22px radius, 2px translucent border (AI Tool card style)
 *      - "status"    — 24px radius, 1px slate border, no overlay (Status card style)
 *      - "display"   — 10px radius, 4px white border via pseudo-element (Site bento style)
 *
 *  @prop {boolean}  [active=false]
 *    When true, applies the signature orange glowing box-shadow effect.
 *
 *  @prop {"vivid" | "contrast" | "neutral"}  [glowColour="vivid"]
 *    Controls the glow colour when `active` is true:
 *      - "vivid"    — Brand Vivid orange glow (#F2561D)
 *      - "contrast" — Brand Contrast teal glow (#008A8C)
 *      - "neutral"  — Neutral grey glow (#8C8C8C)
 *
 *  @prop {string}  [backgroundImage]
 *    Optional URL for a background image (displayed with object-fit: cover).
 *
 *  @prop {boolean}  [overlay=true]
 *    Whether to render the gradient overlay for text legibility.
 *    Defaults to true for "editorial" and "feature" variants.
 *
 *  @prop {"bottom-up" | "hero-left" | "site" | "none"}  [overlayStyle="bottom-up"]
 *    Gradient direction preset for the overlay:
 *      - "bottom-up" — dark at bottom, transparent at top (News card pattern)
 *      - "hero-left" — dark at left, transparent at right (Hero pattern)
 *      - "site"      — transparent top half, dark bottom (Site card pattern)
 *      - "none"      — no gradient overlay
 *
 *  @prop {number | string}  [width]
 *    Card width. Numbers are treated as pixels.
 *
 *  @prop {number | string}  [height]
 *    Card height. Numbers are treated as pixels.
 *
 *  @prop {string}  [aspectRatio]
 *    CSS aspect-ratio value (e.g. "592 / 324" for landscape site cards).
 *
 *  @prop {boolean}  [hoverScale=false]
 *    When true, the card scales to 1.02 on hover.
 *
 *  @prop {(e: React.MouseEvent) => void}  [onClick]
 *    Optional click handler.
 *
 *  @prop {string}  [className]
 *    Additional class names for the root element.
 *
 *  @prop {object}  [style]
 *    Additional inline styles for the root element.
 *
 *  @prop {string}  [id]
 *    Optional HTML id attribute.
 * ──────────────────────────────────────────────────────────────────────────────
 */

/* ── Brand tokens ──────────────────────────────────────────────────────────── */
const BRAND_VIVID    = "#F2561D";
const BRAND_CONTRAST = "#008A8C";
const NEUTRALS_MEDIUM = "#8C8C8C";
const NEUTRALS_SLATE = "#64748B";
const SURFACE_CARD   = "#121212";

/* ── Glow shadow definitions ───────────────────────────────────────────────── */
const GLOW_SHADOWS = {
  vivid:    `0 7px 25px 6px rgba(242, 86, 29, 0.20)`,
  contrast: `0 7px 25px 6px rgba(0, 138, 140, 0.20)`,
  neutral:  `0 7px 25px 6px rgba(140, 140, 140, 0.20)`,
};

/* ── Glow border colours (subtle matching border) ──────────────────────────── */
const GLOW_BORDERS = {
  vivid:    `2px solid rgba(242, 86, 29, 0.25)`,
  contrast: `2px solid rgba(0, 138, 140, 0.25)`,
  neutral:  `2px solid rgba(140, 140, 140, 0.25)`,
};

/* ── Overlay gradient presets ──────────────────────────────────────────────── */
const OVERLAY_GRADIENTS = {
  "bottom-up":
    "linear-gradient(0deg, rgba(0, 0, 0, 0.85) 0%, rgba(0, 0, 0, 0.45) 45%, rgba(0, 0, 0, 0) 100%)",
  "hero-left":
    "linear-gradient(90deg, rgba(0,0,0,0.95) 0%, rgba(0,0,0,0.7) 35%, rgba(0,0,0,0.1) 65%, transparent 100%)",
  site:
    "linear-gradient(180deg, rgba(0, 0, 0, 0) 50%, rgba(0, 0, 0, 0.92) 88%)",
  none: "none",
};

/* ── Variant presets ───────────────────────────────────────────────────────── */
const VARIANT_STYLES = {
  editorial: {
    borderRadius: 10,
    border: "2px solid white",
    defaultOverlay: "bottom-up",
  },
  feature: {
    borderRadius: 22,
    border: "2px solid rgba(255,255,255,0.65)",
    defaultOverlay: "bottom-up",
  },
  status: {
    borderRadius: 24,
    border: `1px solid ${NEUTRALS_SLATE}`,
    defaultOverlay: "none",
    background: SURFACE_CARD,
  },
  display: {
    borderRadius: 10,
    border: "none", // border is handled by ::after pseudo in CSS
    defaultOverlay: "site",
    usePseudoBorder: true,
  },
};


export default function NexusCard({
  children,
  variant = "editorial",
  active = false,
  glowColour = "vivid",
  backgroundImage,
  overlay,
  overlayStyle,
  width,
  height,
  aspectRatio,
  hoverScale = false,
  onClick,
  className = "",
  style = {},
  id,
}) {
  const [isHovered, setIsHovered] = useState(false);

  const variantConfig = VARIANT_STYLES[variant] || VARIANT_STYLES.editorial;
  const showOverlay = overlay !== undefined
    ? overlay
    : variantConfig.defaultOverlay !== "none";
  const resolvedOverlayStyle = overlayStyle || variantConfig.defaultOverlay || "none";

  /* ── Compute root styles ─────────────────────────────────────────────────── */
  const rootStyle = {
    position: "relative",
    overflow: "hidden",
    borderRadius: variantConfig.borderRadius,
    border: active ? GLOW_BORDERS[glowColour] : variantConfig.border,
    background: variantConfig.background || "black",
    boxShadow: active ? GLOW_SHADOWS[glowColour] : "none",
    width: typeof width === "number" ? `${width}px` : width,
    height: typeof height === "number" ? `${height}px` : height,
    aspectRatio,
    cursor: onClick ? "pointer" : undefined,
    transition: "box-shadow 0.3s ease, border 0.3s ease, transform 0.3s ease",
    transform: hoverScale && isHovered ? "scale(1.02)" : "scale(1)",
    flexShrink: 0,
    ...style,
  };

  return (
    <div
      id={id}
      className={className}
      style={rootStyle}
      onClick={onClick}
      onMouseEnter={() => setIsHovered(true)}
      onMouseLeave={() => setIsHovered(false)}
    >
      {/* ── Background image layer ───────────────────────────────────────── */}
      {backgroundImage && (
        <div
          style={{
            position: "absolute",
            inset: 0,
            borderRadius: variantConfig.borderRadius - 2,
            overflow: "hidden",
          }}
        >
          <img
            src={backgroundImage}
            alt=""
            aria-hidden="true"
            style={{
              width: "100%",
              height: "100%",
              objectFit: "cover",
              objectPosition: "center",
              display: "block",
            }}
          />
        </div>
      )}

      {/* ── Gradient overlay ─────────────────────────────────────────────── */}
      {showOverlay && resolvedOverlayStyle !== "none" && (
        <div
          style={{
            position: "absolute",
            inset: 0,
            borderRadius: variantConfig.borderRadius - 2,
            background: OVERLAY_GRADIENTS[resolvedOverlayStyle] || OVERLAY_GRADIENTS["bottom-up"],
            pointerEvents: "none",
            zIndex: 1,
          }}
        />
      )}

      {/* ── Display variant: white pseudo-border (4px solid white via inset) ── */}
      {variantConfig.usePseudoBorder && (
        <div
          style={{
            position: "absolute",
            inset: 0,
            border: "4px solid white",
            borderRadius: variantConfig.borderRadius,
            zIndex: 10,
            pointerEvents: "none",
          }}
        />
      )}

      {/* ── Content layer ────────────────────────────────────────────────── */}
      <div
        style={{
          position: "relative",
          zIndex: 2,
          height: "100%",
        }}
      >
        {children}
      </div>
    </div>
  );
}
