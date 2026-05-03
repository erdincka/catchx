# Nexus Core Components

Reusable React component library for maintaining the **Nexus Global Industries** brand structure across projects.

> **Peer Dependencies:** `react`, `framer-motion`, `@remixicon/react`

---

## Components

### `NexusGlobalNav`

The glassmorphic fixed top navigation bar with animated menu items and sub-menu dropdowns.

```jsx
import { NexusGlobalNav } from "./nexus-core-components";

const navItems = [
  { id: "sales",       label: "Sales",        subItems: ["Enterprise Sales", "Marketing"] },
  { id: "engineering", label: "Engineering",   subItems: ["Software Dev", "R&D"] },
  { id: "security",    label: "Security",      subItems: ["IT Security", "Site Security"] },
];

<NexusGlobalNav
  navItems={navItems}
  leftSlot={<img src="/logo.png" alt="Logo" className="w-12 h-12 rounded-full" />}
  rightSlot={<span className="text-white text-sm">☀ 18°C</span>}
  onItemClick={(id) => console.log("Clicked:", id)}
  onSubItemClick={(item, parentId) => console.log("Sub-item:", item, "from:", parentId)}
/>
```

**Key Props:** `navItems`, `leftSlot`, `rightSlot`, `onItemClick`, `onSubItemClick`

---

### `NexusCard`

A generic card container replicating all card styling variants from the portal.

```jsx
import { NexusCard } from "./nexus-core-components";

{/* Editorial card (News / Site style) */}
<NexusCard
  variant="editorial"
  backgroundImage="/images/news/story.jpg"
  width={425}
  height={568}
>
  <div className="p-6 text-white">Card content here</div>
</NexusCard>

{/* Feature card with active glow (AI Tool style) */}
<NexusCard variant="feature" active glowColour="vivid" width={350}>
  <div className="p-8 text-white text-center">Active tool</div>
</NexusCard>

{/* Status card (no background image, dark surface) */}
<NexusCard variant="status">
  <div className="p-8 text-white">Status rows here</div>
</NexusCard>

{/* Display card with bento border (Site card style) */}
<NexusCard variant="display" backgroundImage="/images/sites/london.jpg" aspectRatio="592 / 324">
  <div className="p-6 text-white">London HQ</div>
</NexusCard>
```

**Variants:** `editorial` (10px, 2px white), `feature` (22px, translucent), `status` (24px, slate), `display` (10px, 4px pseudo-border)

**Key Props:** `variant`, `active`, `glowColour`, `backgroundImage`, `overlay`, `overlayStyle`, `hoverScale`

---

### `NexusSectionDivider`

The signature orange-to-transparent gradient line used under section titles.

```jsx
import { NexusSectionDivider } from "./nexus-core-components";

{/* Divider with title (full SectionHeader pattern) */}
<NexusSectionDivider title="Nexus AI Tools" />

{/* Standalone divider line */}
<NexusSectionDivider />

{/* Vertical accent bar (Hero-style, beside text) */}
<NexusSectionDivider direction="vertical" length={120} />

{/* Custom colours */}
<NexusSectionDivider startColour="#008A8C" endColour="#00B5B8" />
```

**Key Props:** `direction`, `thickness`, `length`, `maxLength`, `title`, `startColour`, `endColour`

---

## Theme Preset

Pair these components with the **Tailwind CSS theme preset** at:

```
brand_output/nexus-theme-preset.js
```

This preset contains all colour tokens, typography, shadows, and gradient definitions the components reference.
