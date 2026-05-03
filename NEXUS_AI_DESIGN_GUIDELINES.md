# NEXUS AI DESIGN GUIDELINES

> **Purpose:** This document is a strict instruction set for AI coding agents building applications under the Nexus Global Industries brand. You **must** follow every rule in this document without exception. Deviations from these guidelines will produce output that is off-brand and unacceptable.
>
> **Audience:** AI language models and coding assistants.
>
> **Last updated:** 2026-04-22

---

## 1. Brand Aesthetic — The Nexus Visual Identity

When building any Nexus-branded interface, you must understand and embody the following aesthetic principles:

### 1.1 Core Identity

Nexus Global Industries is a **premium, dark-mode-first, high-tech-but-elegant** brand. Think of the visual language of a luxury car dashboard merged with a Bloomberg terminal — information-dense, but never cluttered. Every surface is dark. Every accent is deliberate. Every animation is smooth and purposeful.

### 1.2 Mandatory Aesthetic Rules

You must always adhere to these non-negotiable visual rules:

- **Pure black backgrounds.** The page background is always `#000000`. Sections do not have coloured backgrounds. There are no light-mode surfaces. There are no grey page backgrounds. The darkness is the canvas.
- **Glassmorphism over opacity.** Navigation bars, tooltips, modals, and floating panels use semi-transparent black backgrounds with `backdrop-filter: blur()`. This creates the frosted-glass depth effect that defines the Nexus UI.
- **Burnt orange is the signature.** The primary accent colour is `#F2561D` (Brand Vivid) — a deep, industrial burnt orange. It is used sparingly but decisively: active states, hover highlights, accent lines, and CTAs. It must never be overused. It must never be replaced with red, coral, or amber.
- **The "industrial glow" gradient.** A signature design element is the gradient line that fades `transparent → #D9704A → #F2561D → transparent`. This line is used as section dividers and accent bars. It must always fade to transparent on both ends. A hard-edged solid orange line is **forbidden**.
- **Card surfaces are `#121212`.** Cards, panels, and data containers sit on a near-black surface (`#121212`) that is barely distinguishable from the page background. This creates subtle visual layering without breaking the dark aesthetic.
- **White borders define card edges.** Cards use thin white borders (`2px solid white` or `2px solid rgba(255,255,255,0.65)`) to create clean separation against the dark background. Never use coloured borders on cards unless applying the active glow effect.
- **Animations are smooth and restrained.** Use `cubic-bezier(0.22, 1, 0.36, 1)` for entrance animations. Use `200ms` for hover transitions. Avoid bouncy or playful easing. The tone is professional and deliberate.

### 1.3 What This Brand Is NOT

- It is **not** Material Design. Do not use elevation shadows, FABs, or snackbars.
- It is **not** light-mode. Do not create white backgrounds, light grey surfaces, or pastel accents.
- It is **not** playful. Do not use rounded-pill buttons with gradient fills, emoji decorations, or bouncy spring animations.
- It is **not** minimalist to the point of emptiness. Nexus UIs are information-rich. Embrace density with clear hierarchy.

---

## 2. Typography Rules

The Nexus brand uses exactly **two** typefaces. You must never introduce a third typeface. You must never fall back to browser defaults without the correct CSS variable chain.

### 2.1 Font Stack

| Role | Typeface | CSS Variable | Tailwind Class | Weights |
|------|----------|-------------|----------------|---------|
| **Primary (Sans-Serif)** | **Poppins** | `--font-poppins` → `--font-sans` | `font-sans` | 300, 400, 500, 600, 700 |
| **Display (Serif)** | **Instrument Serif** | `--font-instrument-serif` → `--font-serif` | `font-serif` | 400 |

### 2.2 When to Use Serif (Instrument Serif)

You must use `font-serif` (Instrument Serif) **only** in these specific contexts:

- **Hero greetings and headlines** — Large, statement text at the top of a page (e.g. "Hello Simon"). Use at `clamp(3rem, 6vw, 5rem)` with `font-weight: 400` and tight tracking.
- **Location/place names on cards** — The large display name on site cards (e.g. "London", "Berlin"). Use at `40px` with `font-weight: 400`.

**You must never use Instrument Serif for:**
- Navigation labels
- Button text
- Body copy
- Form labels or inputs
- Status indicators
- Any text below 24px in size

### 2.3 When to Use Sans-Serif (Poppins)

You must use `font-sans` (Poppins) for **everything else**. This is the workhorse typeface. Here are the strict weight and spacing rules:

| Context | Weight | Size | Letter Spacing | Transform |
|---------|--------|------|----------------|-----------|
| **Section headers** | 500 | 24px | `0.1em` (tracking-widest) | `uppercase` |
| **Navigation items** | 400 | 16px | `8px` | `uppercase` |
| **Sub-navigation items** | 400 | 14px | `7px` | `uppercase` |
| **Card titles** | 700 | 21px | normal | none |
| **Card body text** | 300 | 13–15px | normal | none |
| **Footer column headings** | 700 | 16px | `8px` | `uppercase` |
| **Footer links** | 300 | 11–12px | normal | none |
| **Status labels** | 400 | 15px | normal | none |
| **Category pills** | 700 | 11px | `0.5px` | `uppercase` |
| **Legal / timestamps** | 300 | 10–11px | normal | `uppercase` |
| **Clock city labels** | 300 | 16px | `3.2px` | `uppercase` |
| **Clock timezone labels** | 400 | 16px | `8px` | `uppercase` |

### 2.4 Critical Typography Rules

- **Wide letter-spacing is reserved for uppercase navigation and labels.** When text is `uppercase`, you must apply generous `letter-spacing` (typically 5–8px). When text is mixed-case or sentence-case, you must use normal letter-spacing.
- **Weight 300 (Light) is for secondary and body text.** Addresses, descriptions, footer links, and supporting information use weight 300. It creates an elegant, thin appearance against the dark background.
- **Weight 700 (Bold) is for titles and headings within cards.** Card titles, column headings, and category pills use bold weight for hierarchy.
- **Weight 500 (Medium) is for section headers only.** The main section titles ("NEXUS AI TOOLS", "NEXUS SERVICE STATUS") use weight 500 — not 400, not 700.
- **You must never use weight 800 or 900.** These weights are not loaded and will cause font loading issues.

---

## 3. Colour Rules

You must **only** use the colours defined in the Nexus theme preset (`nexus-theme-preset.js`). Using any colour outside this palette is a violation of the brand guidelines.

### 3.1 The Complete Palette

#### Brand Colours
| Token | Hex | Usage |
|-------|-----|-------|
| `brand.vivid` | `#F2561D` | Primary accent. CTAs, hover states, active indicators, accent lines. |
| `brand.soft` | `#D9704A` | Secondary accent. Gradient endpoints, subtle dividers, column underlines. |
| `brand.contrast` | `#008A8C` | Teal accent. Login CTAs, action bar backgrounds. Used sparingly. |

#### Neutral Colours
| Token | Hex | Usage |
|-------|-----|-------|
| `neutrals.light` | `#BFBFBF` | Secondary text, sub-menu items, muted labels. |
| `neutrals.medium` | `#8C8C8C` | Tertiary text, timestamps, icon tints. |
| `neutrals.dark` | `#474747` | Borders, dividers, dimmed/inactive nav items. |
| `neutrals.deep` | `#000000` | Page background. Pure black. |
| `neutrals.slate` | `#64748B` | Status card borders only. |

#### Status Colours
| Token | Hex | Usage |
|-------|-----|-------|
| `status.good` | `#008C48` | Operational, healthy, stock price up. |
| `status.failed` | `#BF0300` | Error, down, stock price down. |
| `status.degraded` | `#F27D00` | Warning, degraded performance. |

#### Surface Colours
| Token | Value | Usage |
|-------|-------|-------|
| `surface.card` | `#121212` | Card backgrounds, data panels. |
| `surface.glassDark` | `rgba(18, 18, 18, 0.92)` | Profile cards, tooltips. |
| `surface.glassNav` | `rgba(0, 0, 0, 0.35)` | Main navigation bar. |
| `surface.glassSubNav` | `rgba(0, 0, 0, 0.30)` | Sub-menu dropdown. |
| `surface.loginCard` | `rgba(18, 18, 18, 0.85)` | Login/modal backgrounds. |

### 3.2 Forbidden Colours

You must **never** use any of the following in a Nexus-branded application:

- ❌ **Default Tailwind blue** (`blue-500`, `blue-600`, `indigo-*`, `sky-*`, etc.) — The Nexus brand has no blue. The only cool colour is `brand.contrast` (`#008A8C` teal), and it is used exclusively for login/action CTAs.
- ❌ **Bright white backgrounds** (`bg-white`, `#FFFFFF` as a surface) — White is only used for text and card borders. Never as a background.
- ❌ **Tailwind default greys** (`gray-100` through `gray-900`) — Use only the Nexus neutral scale (`neutrals.light` through `neutrals.deep`).
- ❌ **Pure red** (`#FF0000`, `red-500`) — Use `status.failed` (`#BF0300`) for error states.
- ❌ **Pure green** (`#00FF00`, `green-500`) — Use `status.good` (`#008C48`) for success states.
- ❌ **Pastel colours of any kind** — The Nexus palette is high-contrast against black. Pastels are invisible and off-brand.
- ❌ **Any gradient using Tailwind's default `from-*` / `to-*` presets** — All Nexus gradients are hand-crafted. Use the definitions in the theme preset.

### 3.3 The Universal Hover Rule

This is the single most important interaction rule in the Nexus design system:

> **All interactive text elements transition from white to Brand Vivid (`#F2561D`) on hover, using `transition-colors duration-200`.**

This applies to:
- Navigation items
- Footer links
- Social media icons
- Sub-menu items
- Any clickable text element

When one navigation item is hovered, all *other* items must dim to `neutrals.dark` (`#474747`). Only the hovered item turns Brand Vivid.

---

## 4. Component Usage — Nexus Core Components

The following reusable components are available in the `nexus-core-components` library. When building a new Nexus-branded page, you must use these components rather than creating ad-hoc alternatives.

### 4.1 Importing Components

```jsx
import {
  NexusGlobalNav,
  NexusCard,
  NexusSectionDivider,
} from "@/brand_output/nexus-core-components";
```

### 4.2 NexusGlobalNav

The fixed-position glassmorphic navigation bar. You must place this component at the root layout level so it persists across all pages.

**Basic usage:**

```jsx
const navItems = [
  {
    id: "sales",
    label: "Sales",
    subItems: ["Enterprise Sales", "Marketing", "Business Development"],
  },
  {
    id: "corporate",
    label: "Corporate",
    subItems: ["Human Resources", "Finance", "Legal & Compliance"],
  },
  {
    id: "engineering",
    label: "Engineering",
    subItems: ["Software Development", "R&D and Innovation Labs"],
  },
];

<NexusGlobalNav
  navItems={navItems}
  leftSlot={
    <div className="w-12 h-12 rounded-full ring-2 ring-brand-vivid overflow-hidden">
      <img src="/avatar.webp" alt="User" className="w-full h-full object-cover" />
    </div>
  }
  rightSlot={<WeatherWidget location="London" />}
  onItemClick={(id) => router.push(`/${id}`)}
  onSubItemClick={(item, parentId) => router.push(`/${parentId}/${item}`)}
/>
```

**Rules:**
- You must always provide a `leftSlot` — this is typically a user avatar with a `ring-2 ring-brand-vivid` border, or the Nexus logo mark.
- Navigation labels are automatically rendered as uppercase with wide tracking. You must not apply additional text transforms.
- The `subItems` array controls the animated sub-menu dropdown. If a nav item has no sub-items, omit the property or pass an empty array.

---

### 4.3 NexusCard

A generic card container that replicates the four card styles used across the portal.

**Editorial card (for news, articles, media content):**

```jsx
<NexusCard
  variant="editorial"
  backgroundImage="/images/news/article-hero.jpg"
  overlayStyle="bottom-up"
  width={425}
  height={568}
>
  <div style={{ position: "absolute", bottom: 0, left: 0, right: 0, padding: "0 20px 22px", zIndex: 2 }}>
    <h3 className="font-sans text-[21px] font-bold text-white">Article Headline</h3>
    <p className="font-sans text-[13px] font-light text-white mt-2">Summary text...</p>
  </div>
</NexusCard>
```

**Feature card with active glow (for tools, products, featured items):**

```jsx
<NexusCard
  variant="feature"
  backgroundImage="/images/tools/background.jpg"
  active={isSelected}
  glowColour="vivid"
  width={350}
>
  <div className="flex flex-col items-center p-8 min-h-[425px]">
    <div className="w-[110px] h-[110px] rounded-full bg-white/40 border-[3px] border-white flex items-center justify-center">
      <ToolIcon size={60} color="white" />
    </div>
    <p className="font-sans text-[21px] font-bold text-white mt-4">Tool Name</p>
    <p className="font-sans text-[15px] font-light text-white mt-3">Tagline text</p>
  </div>
</NexusCard>
```

**Status card (for data tables, status dashboards):**

```jsx
<NexusCard variant="status">
  <div className="p-6">
    <h3 className="font-sans text-[20px] font-bold text-white mb-5">Service Status</h3>
    {services.map((svc, i) => (
      <div
        key={svc.id}
        style={{
          background: i % 2 === 0 ? "#000000" : "#121212",
          borderRadius: 8,
          padding: "8px 20px",
        }}
      >
        <span className="font-sans text-[15px] text-white">{svc.name}</span>
      </div>
    ))}
  </div>
</NexusCard>
```

**Display card with pseudo-border (for location/site cards):**

```jsx
<NexusCard
  variant="display"
  backgroundImage="/images/sites/london.jpg"
  aspectRatio="592 / 324"
>
  <h3 className="font-serif text-[40px] text-white absolute top-[18px] left-[26px]">London</h3>
</NexusCard>
```

**Rules:**
- You must always choose the correct `variant` for the content type. Do not use `"editorial"` for status dashboards or `"status"` for image-backed content.
- When using `active={true}`, you must also specify `glowColour`. The default is `"vivid"` (orange), but `"contrast"` (teal) and `"neutral"` (grey) are available for specific use cases like clock widgets.
- The `overlayStyle` prop controls how the gradient overlay renders. Use `"bottom-up"` for image-backed content cards, `"hero-left"` for hero sections, and `"site"` for location cards.

---

### 4.4 NexusSectionDivider

The signature gradient accent line. You must use this component at the start of every content section.

**Full section header (title + divider):**

```jsx
<NexusSectionDivider title="Nexus AI Tools" />
```

This renders the section title in Poppins 500/24px/uppercase/tracking-widest with the gradient divider line beneath it. You must not build section headers manually — always use this component.

**Standalone divider (no title):**

```jsx
<NexusSectionDivider />
```

**Vertical accent bar (for use beside Hero text blocks):**

```jsx
<div className="flex items-stretch gap-6">
  <NexusSectionDivider direction="vertical" length={120} />
  <div>
    <p className="text-white text-lg font-light">Welcome to the Nexus…</p>
  </div>
</div>
```

**Rules:**
- You must never create a solid orange line as a section divider. The gradient fade on both ends is a core brand element.
- The `title` prop automatically applies the correct typography. You must not override the font size, weight, or tracking of section titles.
- When using the vertical variant, the thickness defaults to `3px`. You must not make it thicker than `4px`.

---

## 5. Layout & Spacing Rules

When structuring page layouts, you must follow these spacing conventions:

| Rule | Value | Context |
|------|-------|---------|
| **Section top padding** | `120px` | Vertical space above each content section. |
| **Section bottom padding** | `48px` | Breathing room below each section. |
| **Content max-width** | `1280px` (max-w-7xl) | Standard content containers, footer, clocks. |
| **Wide max-width** | `1600px` | Status card rows, wider data layouts. |
| **NavBar height** | `80px` (h-20) | Fixed. Do not change. |
| **Content horizontal padding** | `32px` (px-8) | Standard left/right padding within content areas. |
| **Section header indent** | `48px` | Left padding on SectionHeader / NexusSectionDivider with title. |
| **Card gap** | `24px` | Space between cards in a row/grid. |
| **Bento grid gap** | `30px` | Space between site/bento-style cards. |

---

## 6. Animation & Motion Rules

### 6.1 Required Animation Library

You must use **Framer Motion** for all animations. Do not use CSS `@keyframes` for component entrance/exit animations. CSS transitions are acceptable for simple hover states.

### 6.2 Standard Animation Patterns

**Fade-in up (component entrance):**

```jsx
<motion.div
  initial={{ opacity: 0, y: 20 }}
  animate={{ opacity: 1, y: 0 }}
  transition={{ duration: 0.5, ease: [0.22, 1, 0.36, 1] }}
>
```

**Staggered children (list/grid items):**

```jsx
{items.map((item, i) => (
  <motion.div
    key={item.id}
    initial={{ opacity: 0, y: 20 }}
    animate={{ opacity: 1, y: 0 }}
    transition={{ duration: 0.5, delay: i * 0.1, ease: [0.22, 1, 0.36, 1] }}
  >
```

**AnimatePresence for enter/exit:**

```jsx
<AnimatePresence>
  {isVisible && (
    <motion.div
      initial={{ opacity: 0, scale: 0.95 }}
      animate={{ opacity: 1, scale: 1 }}
      exit={{ opacity: 0, scale: 1.05 }}
      transition={{ duration: 0.3, ease: "easeOut" }}
    >
```

### 6.3 Motion Rules

- You must wrap all conditional renders in `<AnimatePresence>` to ensure smooth exit animations.
- You must use `cubic-bezier(0.22, 1, 0.36, 1)` (aliased as `nexus-smooth` in the theme preset) for panel reveals and entrance animations. This is a decelerating ease that feels premium.
- You must use `cubic-bezier(0.25, 0.46, 0.45, 0.94)` (aliased as `nexus-slide` in the theme preset) for carousel and sliding transitions.
- You must never use `ease-in` alone. It creates an accelerating motion that feels unfinished.
- You must never use spring animations with high `bounce` values. Nexus motion is smooth and controlled, not bouncy.

---

## 7. Glassmorphism Rules

When creating floating panels, navigation overlays, tooltips, or modal backdrops, you must apply the glassmorphic treatment:

```css
background: rgba(18, 18, 18, 0.92);
backdrop-filter: blur(24px);
-webkit-backdrop-filter: blur(24px);
border: 1px solid rgba(255, 255, 255, 0.12);
border-radius: 16px;
```

### Blur Scale

| Context | Blur | Background Opacity |
|---------|------|--------------------|
| **Navigation bar** | `blur(16px)` | `rgba(0, 0, 0, 0.35)` |
| **Sub-menu dropdown** | `blur(16px)` | `rgba(0, 0, 0, 0.30)` |
| **Profile/hover cards** | `blur(24px)` | `rgba(18, 18, 18, 0.92)` |
| **Tooltips** | `blur(12px)` | `rgba(24, 24, 24, 0.92)` |
| **Modals/login cards** | `blur(24px)` | `rgba(18, 18, 18, 0.85)` |

**Rules:**
- You must always include `-webkit-backdrop-filter` alongside `backdrop-filter` for Safari compatibility.
- Glass surfaces must never be fully opaque. The background content must be subtly visible through the blur.
- Glass surfaces must have a very subtle border (`rgba(255, 255, 255, 0.12)` or `border-neutrals-dark/50`) to define their edges.

---

## 8. Iconography Rules

- You must use **`@remixicon/react`** as the primary icon library. Import icons using the `Ri` prefix pattern (e.g. `RiMapPinFill`, `RiArrowRightLine`).
- Icons must be sized explicitly using the `size` prop (not CSS).
- Icon colours must use brand tokens. Common patterns: `color="white"` for default state, `color="#F2561D"` for active/accent state, `color="#8C8C8C"` for muted/secondary state.
- You must never use emoji as UI icons.
- You must never use FontAwesome, Heroicons, or other icon libraries unless explicitly requested.

---

## 9. Localisation Rules

- You must use **`en_GB`** for all user-visible text. This means: "colour" not "color", "centre" not "center", "organisation" not "organization", "minimise" not "minimize".
- Date formats must use `DD/MM/YYYY` or `YYYY-MM-DD`, never `MM/DD/YYYY`.
- Currency must default to GBP (£) unless context specifies otherwise.
- The `<html>` element must always include `lang="en-GB"`.

---

## 10. Quick Reference — Do vs. Don't

| ✅ DO | ❌ DON'T |
|-------|----------|
| Use `#000000` as the page background | Use white, light grey, or coloured section backgrounds |
| Use `#121212` for card/panel surfaces | Use `bg-white` or `bg-gray-*` for cards |
| Use Brand Vivid `#F2561D` for hover states | Use Tailwind blue, indigo, or any non-brand colour for hover |
| Use the gradient divider component | Create solid orange lines as dividers |
| Use Poppins for all UI text | Introduce new typefaces or use system fonts |
| Use Instrument Serif for hero/display text only | Use serif fonts for navigation, buttons, or body text |
| Use `backdrop-filter: blur()` for glass effects | Use solid opaque backgrounds for floating elements |
| Use Framer Motion for entrance animations | Use CSS `@keyframes` for component transitions |
| Use `AnimatePresence` for conditional elements | Hard-cut content in and out of the DOM |
| Apply wide `letter-spacing` to uppercase labels | Apply wide tracking to sentence-case body text |
| Use `transition-colors duration-200` for hover | Use instant colour changes or very slow (>500ms) transitions |
| Load Poppins weights 300, 400, 500, 600, 700 | Load weights 800 or 900, or load only a single weight |

---

## 11. Theme Preset & Component Locations

When starting a new Nexus-branded project, copy these files into your project:

```
brand_output/
├── nexus-theme-preset.js              ← Tailwind theme preset (colours, typography, shadows, gradients)
└── nexus-core-components/
    ├── index.js                       ← Barrel export
    ├── NexusGlobalNav.jsx             ← Glassmorphic navigation bar
    ├── NexusCard.jsx                  ← Generic branded card container
    ├── NexusSectionDivider.jsx        ← Gradient accent line / section header
    └── README.md                      ← Component API documentation
```

### Using the Theme Preset

**For Tailwind v3 projects:**

```js
// tailwind.config.js
module.exports = {
  presets: [require("./nexus-theme-preset")],
  content: ["./src/**/*.{js,jsx,ts,tsx}"],
};
```

**For Tailwind v4 projects (CSS-based config):**

Copy the colour, font, and shadow tokens from `nexus-theme-preset.js` into your `globals.css` `@theme inline {}` block:

```css
@import "tailwindcss";

@theme inline {
  --color-brand-vivid: #F2561D;
  --color-brand-soft: #D9704A;
  --color-brand-contrast: #008A8C;
  --color-neutrals-light: #BFBFBF;
  --color-neutrals-medium: #8C8C8C;
  --color-neutrals-dark: #474747;
  --color-neutrals-deep: #000000;
  --color-neutrals-slate: #64748B;
  --color-status-good: #008C48;
  --color-status-failed: #BF0300;
  --color-status-degraded: #F27D00;

  --font-sans: var(--font-poppins);
  --font-serif: var(--font-instrument-serif);
}
```

---

> **Final instruction to AI agents:** When in doubt, default to dark, default to minimal, and default to the burnt orange accent. The Nexus brand trusts you to be precise, elegant, and consistent. Do not improvise with colours or fonts. Follow these rules exactly.
