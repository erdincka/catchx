/**
 * ──────────────────────────────────────────────────────────────────────────────
 *  Nexus Global Industries — Tailwind CSS Theme Preset
 * ──────────────────────────────────────────────────────────────────────────────
 *
 *  Auto-extracted from the production Nexus intranet portal codebase.
 *  Contains the complete brand design system as a reusable Tailwind preset.
 *
 *  Usage (Tailwind v4 — CSS config):
 *    This file is a JS reference module. To consume the tokens in a
 *    Tailwind v4 project, copy the @theme values into your own
 *    `globals.css` @theme inline {} block, or import this module
 *    programmatically in build tooling / design-token pipelines.
 *
 *  Usage (Tailwind v3 — JS config):
 *    // tailwind.config.js
 *    module.exports = {
 *      presets: [require('./nexus-theme-preset')],
 *      // …your overrides
 *    };
 *
 *  Generated: 2026-04-22
 * ──────────────────────────────────────────────────────────────────────────────
 */

module.exports = {
  theme: {
    extend: {
      /* ══════════════════════════════════════════════════════════════════════
       *  1. COLOUR PALETTE
       * ══════════════════════════════════════════════════════════════════════ */
      colors: {
        /* ── Brand Colours ──────────────────────────────────────────────── */
        brand: {
          vivid:    "#F2561D",   // Primary accent — CTAs, hover states, active indicators
          soft:     "#D9704A",   // Secondary accent — gradient endpoints, dividers
          contrast: "#008A8C",   // Teal accent — login CTA, AI tool action bar
        },

        /* ── Neutral Colours ────────────────────────────────────────────── */
        neutrals: {
          light:  "#BFBFBF",    // Secondary text, sub-menu items, muted labels
          medium: "#8C8C8C",    // Tertiary text, timestamps, icon tints
          dark:   "#474747",    // Borders, dividers, dimmed nav items
          deep:   "#000000",    // Page background (pure black)
          slate:  "#64748B",    // Status card borders
        },

        /* ── Status Colours ─────────────────────────────────────────────── */
        status: {
          good:     "#008C48",  // Healthy / operational, stock price UP
          failed:   "#BF0300",  // Error / down, stock price DOWN
          degraded: "#F27D00",  // Warning / degraded performance
        },

        /* ── Surface & Background Colours (derived) ─────────────────────── */
        surface: {
          page:       "#000000",                    // Body / page background
          card:       "#121212",                     // Card panels, data panels, status cards
          rowEven:    "#000000",                     // Alternating list rows (even)
          rowOdd:     "#121212",                     // Alternating list rows (odd)
          glassDark:  "rgba(18, 18, 18, 0.92)",      // Profile cards, tooltips
          glassNav:   "rgba(0, 0, 0, 0.35)",         // Main navigation bar
          glassSubNav:"rgba(0, 0, 0, 0.30)",         // Sub-menu dropdown
          loginCard:  "rgba(18, 18, 18, 0.85)",      // Login prompt modal
          navButton:  "rgba(18, 18, 18, 0.75)",      // Carousel prev/next buttons
          categoryPill: "rgba(140, 140, 140, 0.75)", // News card category pill
        },
      },

      /* ══════════════════════════════════════════════════════════════════════
       *  2. TYPOGRAPHY
       * ══════════════════════════════════════════════════════════════════════ */
      fontFamily: {
        /**
         * Primary (sans-serif): Poppins
         * Loaded via next/font/google with weights: 300, 400, 500, 600, 700
         * CSS variable: --font-poppins → mapped to --font-sans
         */
        sans: ["var(--font-poppins)", "Arial", "Helvetica", "sans-serif"],

        /**
         * Display (serif): Instrument Serif
         * Loaded via next/font/google with weight: 400
         * CSS variable: --font-instrument-serif → mapped to --font-serif
         */
        serif: ["var(--font-instrument-serif)", "serif"],
      },

      /**
       * Typography scale — named sizes matching the Nexus design system.
       * Each entry: [fontSize, { lineHeight, letterSpacing?, fontWeight? }]
       */
      fontSize: {
        /* Hero greeting — Instrument Serif 400 */
        "hero":         ["clamp(3rem, 6vw, 5rem)", { lineHeight: "1", letterSpacing: "-0.025em" }],

        /* Section headers — Poppins 500, uppercase, tracking-widest */
        "section":      ["24px",  { lineHeight: "1.4",   letterSpacing: "0.1em",  fontWeight: "500" }],

        /* Nav menu items — Poppins 400, uppercase */
        "nav":          ["16px",  { lineHeight: "1.5",   letterSpacing: "8px" }],

        /* Nav sub-menu items — Poppins 400, uppercase */
        "nav-sub":      ["14px",  { lineHeight: "1.5",   letterSpacing: "7px" }],

        /* Card titles — Poppins 700 */
        "card-title":   ["21px",  { lineHeight: "30px",  fontWeight: "700" }],

        /* Card body / tagline — Poppins 300 */
        "card-body":    ["13px",  { lineHeight: "20px",  fontWeight: "300" }],

        /* Footer headings — Poppins 700, uppercase */
        "footer-heading": ["16px", { lineHeight: "1.5",  letterSpacing: "8px",    fontWeight: "700" }],

        /* Footer links — Poppins 300 */
        "footer-link":  ["12px",  { lineHeight: "1.5",   fontWeight: "300" }],

        /* Status labels — Poppins 400 */
        "status":       ["15px",  { lineHeight: "1.5" }],

        /* Category pills — Poppins 700, uppercase */
        "pill":         ["11px",  { lineHeight: "1.4",   letterSpacing: "0.5px",  fontWeight: "700" }],

        /* Clock city labels — Poppins 300, uppercase */
        "clock-city":   ["16px",  { lineHeight: "1.5",   letterSpacing: "3.2px",  fontWeight: "300" }],

        /* Clock timezone labels — Poppins 400, uppercase */
        "clock-tz":     ["16px",  { lineHeight: "1.5",   letterSpacing: "8px" }],

        /* Site card name — Instrument Serif 400 */
        "site-name":    ["40px",  { lineHeight: "1.1" }],

        /* Site card role — Poppins 700, uppercase */
        "site-role":    ["14px",  { lineHeight: "1.6",   letterSpacing: "5px",    fontWeight: "700" }],

        /* Legal / timestamps — Poppins 300, uppercase */
        "legal":        ["10px",  { lineHeight: "1.5",   fontWeight: "300" }],

        /* Read more micro-link — Poppins 400, uppercase */
        "read-more":    ["8px",   { lineHeight: "1.5",   letterSpacing: "3px" }],

        /* Facility tags — Poppins 300, uppercase */
        "facility":     ["11px",  { lineHeight: "1.5",   fontWeight: "300" }],

        /* Facilities section header — Poppins 300, uppercase */
        "facilities-header": ["12px", { lineHeight: "1.5", letterSpacing: "2.5px", fontWeight: "300" }],

        /* Info labels (address, phone, etc.) — Poppins 300 */
        "info":         ["14px",  { lineHeight: "1.3",   fontWeight: "300" }],
      },

      /* ══════════════════════════════════════════════════════════════════════
       *  3. BOX SHADOWS & GLOW EFFECTS
       * ══════════════════════════════════════════════════════════════════════ */
      boxShadow: {
        /* ── Clock neumorphic glows ─────────────────────────────────────── */
        "glow-vivid":    "0 7px 25px 6px rgba(242, 86, 29, 0.20)",   // Brand Vivid glow (GMT clock)
        "glow-contrast": "0 7px 25px 6px rgba(0, 138, 140, 0.20)",   // Brand Contrast glow (CET clock)
        "glow-neutral":  "0 7px 25px 6px rgba(140, 140, 140, 0.20)", // Neutral glow (AST clock)

        /* ── Carousel / slider glows ────────────────────────────────────── */
        "glow-slider":   "0 0 8px rgba(242, 86, 29, 0.4)",           // Active slider thumb glow

        /* ── Card shadows ───────────────────────────────────────────────── */
        "card-action":   "0 3px 6px rgba(0, 0, 0, 0.35)",            // AI tool action bar
        "card-button":   "0 4px 8px rgba(0, 0, 0, 0.45)",            // AI tool action button (pressed)
        "card-panel":    "0px -2px 12px 2px rgba(0, 0, 0, 0.35)",    // Site card float-up data panel
      },

      /* ══════════════════════════════════════════════════════════════════════
       *  4. BORDER RADIUS SCALE
       * ══════════════════════════════════════════════════════════════════════ */
      borderRadius: {
        "login":      "80px",   // Login card / inputs — full pill shape
        "status":     "24px",   // Status cards — large rounded
        "ai-card":    "22px",   // AI Tool card wrapper
        "ai-inner":   "20px",   // AI Tool card inner image
        "profile":    "16px",   // Profile hover card popover
        "news":       "10px",   // News cards — tight, editorial feel
        "site":       "10px",   // Site cards — clean, map-like feel
        "status-row": "8px",    // Status row inline items
        "carousel":   "7px",    // Carousel pill indicators
        "tooltip":    "6px",    // Tooltip popovers
      },

      /* ══════════════════════════════════════════════════════════════════════
       *  5. BACKDROP BLUR VALUES
       * ══════════════════════════════════════════════════════════════════════ */
      backdropBlur: {
        "nav":      "16px",     // Main NavBar + Sub-menu
        "profile":  "24px",     // Profile card popover
        "tooltip":  "12px",     // Tooltip overlays
      },

      /* ══════════════════════════════════════════════════════════════════════
       *  6. CUSTOM GRADIENT DEFINITIONS
       *     (Stored as backgroundImage values for use with Tailwind classes)
       * ══════════════════════════════════════════════════════════════════════ */
      backgroundImage: {
        /* ── Brand Accent Gradients (the signature Nexus "industrial glow") ── */

        /**
         * Vertical orange accent line — used beside Hero welcome text.
         * Fades: transparent → Brand Soft → Brand Vivid → transparent
         * Apply to a 3px-wide element.
         */
        "accent-vertical":
          "linear-gradient(180deg, transparent 0%, #D9704A 20%, #F2561D 80%, transparent 100%)",

        /**
         * Horizontal orange divider line — used in SectionHeader components.
         * Same colour stops as vertical, rotated 90°.
         * Apply to a 3px-tall element.
         */
        "accent-horizontal":
          "linear-gradient(90deg, transparent 0%, #D9704A 20%, #F2561D 80%, transparent 100%)",

        /* ── Overlay / Legibility Gradients ─────────────────────────────── */

        /**
         * Hero left-side gradient overlay — ensures text legibility over
         * the scroll-driven background animation canvas.
         */
        "hero-overlay":
          "linear-gradient(90deg, rgba(0,0,0,0.95) 0%, rgba(0,0,0,0.7) 35%, rgba(0,0,0,0.1) 65%, transparent 100%)",

        /**
         * Bottom-up gradient — used on News cards for text legibility
         * over background images.
         */
        "card-overlay-bottom":
          "linear-gradient(0deg, rgba(0, 0, 0, 0.85) 0%, rgba(0, 0, 0, 0.45) 45%, rgba(0, 0, 0, 0) 100%)",

        /**
         * Site card gradient overlay — softer bottom fade for location
         * name legibility.
         */
        "site-overlay":
          "linear-gradient(180deg, rgba(0, 0, 0, 0) 50%, rgba(0, 0, 0, 0.92) 88%)",

        /* ── Navigation Scroll Fade Gradients ──────────────────────────── */

        /**
         * Left fade — sub-menu overflow indicator (scroll left available).
         */
        "nav-fade-left":
          "linear-gradient(to right, rgba(0,0,0,0.85) 0%, transparent 100%)",

        /**
         * Right fade — sub-menu overflow indicator (scroll right available).
         */
        "nav-fade-right":
          "linear-gradient(to left, rgba(0,0,0,0.85) 0%, transparent 100%)",
      },

      /* ══════════════════════════════════════════════════════════════════════
       *  7. TRANSITION TIMING FUNCTIONS
       * ══════════════════════════════════════════════════════════════════════ */
      transitionTimingFunction: {
        "nexus-smooth": "cubic-bezier(0.22, 1, 0.36, 1)",   // Float-up panels, staggered entrances
        "nexus-slide":  "cubic-bezier(0.25, 0.46, 0.45, 0.94)", // 3D carousel sliding
      },

      /* ══════════════════════════════════════════════════════════════════════
       *  8. TRANSITION DURATION
       * ══════════════════════════════════════════════════════════════════════ */
      transitionDuration: {
        "hover":    "200ms",    // Standard hover state transitions
        "nav":      "250ms",    // Nav item colour transitions
        "login":    "300ms",    // Login button hover
        "carousel": "700ms",   // 3D carousel transform
        "panel":    "1200ms",   // Site card float-up panel
      },

      /* ══════════════════════════════════════════════════════════════════════
       *  9. SPACING & LAYOUT TOKENS
       * ══════════════════════════════════════════════════════════════════════ */
      spacing: {
        "section-top":     "120px",   // Section vertical top padding
        "section-bottom":  "48px",    // Section bottom breathing room
        "nav-height":      "80px",    // Fixed navbar height (h-20)
        "nav-px":          "28px",    // NavBar horizontal padding (px-7)
        "content-px":      "32px",    // Standard content horizontal padding (px-8)
        "header-indent":   "48px",    // SectionHeader left padding
        "bento-gap":       "30px",    // Site bento grid gap
        "card-gap":        "24px",    // Between status cards
      },

      /* ══════════════════════════════════════════════════════════════════════
       *  10. MAX-WIDTH TOKENS
       * ══════════════════════════════════════════════════════════════════════ */
      maxWidth: {
        "content":  "1280px",   // max-w-7xl — footer, clocks, content
        "wide":     "1600px",   // Status cards, wider sections
        "hero":     "1750px",   // Site bento grid, hero-width content
      },

      /* ══════════════════════════════════════════════════════════════════════
       *  11. BORDER WIDTHS & STYLES (custom ring / outline tokens)
       * ══════════════════════════════════════════════════════════════════════ */
      borderWidth: {
        "card":     "2px",      // News card, AI Tool card borders
        "site":     "4px",      // Site card white border (::after pseudo)
        "clock":    "2px",      // Clock neumorphic border
        "divider":  "3px",      // Accent gradient divider thickness
      },

      /* ══════════════════════════════════════════════════════════════════════
       *  12. ASPECT RATIOS
       * ══════════════════════════════════════════════════════════════════════ */
      aspectRatio: {
        "site-landscape": "592 / 324",  // London landscape card
        "site-portrait":  "288 / 324",  // Standard portrait site card
      },
    },
  },
};
