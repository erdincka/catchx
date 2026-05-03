/**
 * ──────────────────────────────────────────────────────────────────────────────
 *  NexusSectionDivider — Signature horizontal gradient accent line
 * ──────────────────────────────────────────────────────────────────────────────
 *
 *  Renders the glowing orange-to-transparent gradient divider used beneath
 *  section titles throughout the Nexus intranet portal (e.g. "NEXUS AI TOOLS",
 *  "NEXUS SERVICE STATUS", "NEXUS NETWORK NEWS").
 *
 *  The gradient fades:
 *    transparent → Brand Soft (#D9704A) → Brand Vivid (#F2561D) → transparent
 *
 *  This "industrial glow" line is a core Nexus brand element and must never
 *  be a hard-edged solid line — the transparent fade on both ends is essential.
 *
 *  Props:
 *  ──────────────────────────────────────────────────────────────────────────
 *  @prop {"horizontal" | "vertical"}  [direction="horizontal"]
 *    Gradient orientation. Horizontal is the default section divider;
 *    vertical is used for the Hero accent bar beside text.
 *
 *  @prop {number | string}  [thickness=3]
 *    Line thickness in pixels (height for horizontal, width for vertical).
 *
 *  @prop {number | string}  [length]
 *    Line length. Defaults to "100%" for horizontal, "100%" for vertical.
 *    Numbers are treated as pixels.
 *
 *  @prop {string}  [maxLength]
 *    Maximum length (CSS max-width or max-height). Defaults to "48rem" (768px)
 *    for horizontal, matching the original max-w-3xl.
 *
 *  @prop {string}  [title]
 *    Optional section title rendered above the divider line, matching the
 *    Nexus SectionHeader pattern (Poppins 500, 24px, white, uppercase,
 *    tracking-widest).
 *
 *  @prop {string}  [className]
 *    Additional class names for the root wrapper.
 *
 *  @prop {object}  [style]
 *    Additional inline styles for the root wrapper.
 *
 *  @prop {object}  [lineStyle]
 *    Additional inline styles merged directly onto the gradient line element.
 *
 *  @prop {string}  [startColour="#D9704A"]
 *    Override the gradient start colour (Brand Soft by default).
 *
 *  @prop {string}  [endColour="#F2561D"]
 *    Override the gradient end colour (Brand Vivid by default).
 * ──────────────────────────────────────────────────────────────────────────────
 */

/* ── Brand tokens ──────────────────────────────────────────────────────────── */
const BRAND_SOFT  = "#D9704A";
const BRAND_VIVID = "#F2561D";

export default function NexusSectionDivider({
  direction = "horizontal",
  thickness = 3,
  length,
  maxLength,
  title,
  className = "",
  style = {},
  lineStyle = {},
  startColour = BRAND_SOFT,
  endColour = BRAND_VIVID,
}) {
  const isHorizontal = direction === "horizontal";

  /* ── Build the gradient string ───────────────────────────────────────────── */
  const angle = isHorizontal ? "90deg" : "180deg";
  const gradient = `linear-gradient(${angle}, transparent 0%, ${startColour} 20%, ${endColour} 80%, transparent 100%)`;

  /* ── Resolve dimensions ──────────────────────────────────────────────────── */
  const thicknessPx = typeof thickness === "number" ? `${thickness}px` : thickness;
  const resolvedLength = length
    ? (typeof length === "number" ? `${length}px` : length)
    : "100%";
  const resolvedMaxLength = maxLength || (isHorizontal ? "48rem" : undefined);

  /* ── Line element styles ─────────────────────────────────────────────────── */
  const lineElementStyle = isHorizontal
    ? {
        width: resolvedLength,
        maxWidth: resolvedMaxLength,
        height: thicknessPx,
        background: gradient,
        ...lineStyle,
      }
    : {
        width: thicknessPx,
        height: resolvedLength,
        maxHeight: resolvedMaxLength,
        background: gradient,
        flexShrink: 0,
        ...lineStyle,
      };

  /* ── Title-only rendering (no title = just the line) ─────────────────────── */
  if (!title) {
    return (
      <div className={className} style={style}>
        <div style={lineElementStyle} />
      </div>
    );
  }

  /* ── With title: mirrors the SectionHeader layout ────────────────────────── */
  return (
    <div
      className={className}
      style={{
        display: "flex",
        flexDirection: "column",
        gap: 12,
        paddingLeft: 48,
        marginBottom: 48,
        ...style,
      }}
    >
      {/* Section title */}
      <h2
        style={{
          color: "white",
          textTransform: "uppercase",
          letterSpacing: "0.1em",
          fontFamily: "var(--font-poppins, var(--font-sans, sans-serif))",
          fontSize: 24,
          fontWeight: 500,
          margin: 0,
        }}
      >
        {title}
      </h2>

      {/* Gradient divider line — pulled left to bleed to the screen edge */}
      <div
        style={{
          ...lineElementStyle,
          marginLeft: -48,
        }}
      />
    </div>
  );
}
