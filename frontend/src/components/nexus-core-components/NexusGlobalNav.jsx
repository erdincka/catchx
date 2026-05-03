"use client";

import { useState, useRef, useEffect, useCallback } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { RiArrowLeftSLine, RiArrowRightSLine } from "@remixicon/react";

/**
 * ──────────────────────────────────────────────────────────────────────────────
 *  NexusGlobalNav — Reusable top navigation bar
 * ──────────────────────────────────────────────────────────────────────────────
 *
 *  Replicates the Nexus Global Industries glassmorphic navigation bar with:
 *    • Left slot (profile avatar / logo)
 *    • Centre menu items with animated hover state & vertical dividers
 *    • Expandable sub-menu row with overflow-aware scroll, gradient fades,
 *      drag-to-scroll, and arrow controls
 *    • Right slot (weather widget / utility)
 *
 *  Props:
 *  ──────────────────────────────────────────────────────────────────────────
 *  @prop {React.ReactNode}  leftSlot
 *    Content rendered in the left-hand slot (e.g. avatar, logo).
 *
 *  @prop {React.ReactNode}  rightSlot
 *    Content rendered in the right-hand slot (e.g. weather widget, search).
 *
 *  @prop {Array<{ id: string, label: string, subItems?: string[] }>}  navItems
 *    Navigation link definitions. Each item has:
 *      - id:       unique key
 *      - label:    display text (rendered uppercase)
 *      - subItems: optional array of sub-menu strings shown on hover
 *
 *  @prop {(id: string) => void}  [onItemClick]
 *    Optional callback fired when a nav item is clicked. Receives the item id.
 *
 *  @prop {(subItem: string, parentId: string) => void}  [onSubItemClick]
 *    Optional callback fired when a sub-menu item is clicked.
 *
 *  @prop {string}  [className]
 *    Additional class names merged onto the root <nav> element.
 *
 *  @prop {object}  [style]
 *    Additional inline styles merged onto the root <nav> element.
 * ──────────────────────────────────────────────────────────────────────────────
 */

/* ── Brand tokens ──────────────────────────────────────────────────────────── */
const BRAND_VIVID  = "#F2561D";
const BRAND_SOFT   = "#D9704A";
const NEUTRALS_LIGHT = "#BFBFBF";
const NEUTRALS_DARK  = "#474747";
const WHITE = "#FFFFFF";

const GLASS_NAV      = "rgba(0, 0, 0, 0.35)";
const GLASS_SUB_NAV  = "rgba(0, 0, 0, 0.30)";
const BLUR_NAV       = "blur(16px)";

const NAV_FADE_LEFT  = "linear-gradient(to right, rgba(0,0,0,0.85) 0%, transparent 100%)";
const NAV_FADE_RIGHT = "linear-gradient(to left, rgba(0,0,0,0.85) 0%, transparent 100%)";

/* ── Vertical divider between menu items ───────────────────────────────────── */
function Divider() {
  return (
    <div
      style={{
        height: 32,
        width: 0.7,
        backgroundColor: BRAND_SOFT,
        opacity: 0.7,
        flexShrink: 0,
      }}
    />
  );
}

/* ── Dot separator between sub-menu items ──────────────────────────────────── */
function DotSeparator() {
  return (
    <div
      style={{
        width: 6,
        height: 6,
        borderRadius: "50%",
        backgroundColor: NEUTRALS_LIGHT,
        opacity: 0.5,
        flexShrink: 0,
      }}
    />
  );
}

/* ── Single menu item ──────────────────────────────────────────────────────── */
function NavItem({ item, activeId, onEnter, onLeave, onClick }) {
  return (
    <button
      className="cursor-pointer"
      style={{
        background: "transparent",
        border: "none",
        outline: "none",
        padding: 0,
        cursor: "pointer",
      }}
      onMouseEnter={() => onEnter(item.id)}
      onMouseLeave={onLeave}
      onClick={() => onClick?.(item.id)}
    >
      <motion.span
        style={{
          fontSize: 16,
          fontFamily: "var(--font-poppins, var(--font-sans, sans-serif))",
          fontWeight: 400,
          textTransform: "uppercase",
          letterSpacing: 8,
          whiteSpace: "nowrap",
          userSelect: "none",
        }}
        animate={{
          color: activeId === item.id
            ? BRAND_VIVID          // hovered item → brand vivid
            : activeId
              ? NEUTRALS_DARK      // another item is hovered → dim
              : WHITE,             // default → white
        }}
        transition={{ duration: 0.25 }}
      >
        {item.label}
      </motion.span>
    </button>
  );
}

/* ── Sub-menu row (overflow-aware with gradient fade + arrow controls) ────── */
function SubMenu({ subItems, parentId, onSubItemClick }) {
  const scrollRef = useRef(null);
  const [canScrollLeft, setCanScrollLeft] = useState(false);
  const [canScrollRight, setCanScrollRight] = useState(false);
  const [isOverflowing, setIsOverflowing] = useState(false);

  // Drag-to-scroll state
  const isDragging = useRef(false);
  const dragStartX = useRef(0);
  const scrollStartX = useRef(0);

  const checkOverflow = useCallback(() => {
    const el = scrollRef.current;
    if (!el) return;
    const overflows = el.scrollWidth > el.clientWidth + 2;
    setIsOverflowing(overflows);
    setCanScrollLeft(el.scrollLeft > 2);
    setCanScrollRight(el.scrollLeft < el.scrollWidth - el.clientWidth - 2);
  }, []);

  useEffect(() => {
    const timer = setTimeout(checkOverflow, 50);
    window.addEventListener("resize", checkOverflow);
    return () => {
      clearTimeout(timer);
      window.removeEventListener("resize", checkOverflow);
    };
  }, [checkOverflow, subItems]);

  // Horizontal scroll interception from vertical wheel events
  useEffect(() => {
    const el = scrollRef.current;
    if (!el) return;
    const onWheel = (e) => {
      if (Math.abs(e.deltaY) > Math.abs(e.deltaX)) {
        e.preventDefault();
        el.scrollLeft += e.deltaY;
        checkOverflow();
      }
    };
    el.addEventListener("wheel", onWheel, { passive: false });
    return () => el.removeEventListener("wheel", onWheel);
  }, [checkOverflow, subItems]);

  // Click-to-scroll
  const scrollBy = (direction) => {
    const el = scrollRef.current;
    if (!el) return;
    const amount = el.clientWidth * 0.6;
    el.scrollBy({ left: direction * amount, behavior: "smooth" });
    setTimeout(checkOverflow, 350);
  };

  // Drag-to-scroll handlers
  const handleMouseDown = (e) => {
    isDragging.current = true;
    dragStartX.current = e.clientX;
    scrollStartX.current = scrollRef.current?.scrollLeft || 0;
    document.body.style.cursor = "grabbing";
    document.body.style.userSelect = "none";
  };
  const handleMouseMove = useCallback((e) => {
    if (!isDragging.current) return;
    const dx = e.clientX - dragStartX.current;
    if (scrollRef.current) {
      scrollRef.current.scrollLeft = scrollStartX.current - dx;
      checkOverflow();
    }
  }, [checkOverflow]);
  const handleMouseUp = useCallback(() => {
    isDragging.current = false;
    document.body.style.cursor = "";
    document.body.style.userSelect = "";
  }, []);

  useEffect(() => {
    document.addEventListener("mousemove", handleMouseMove);
    document.addEventListener("mouseup", handleMouseUp);
    return () => {
      document.removeEventListener("mousemove", handleMouseMove);
      document.removeEventListener("mouseup", handleMouseUp);
    };
  }, [handleMouseMove, handleMouseUp]);

  /* ── Arrow button shared styles ─────────────────────────────────────────── */
  const arrowBtnStyle = {
    width: 28,
    height: 28,
    borderRadius: "50%",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    cursor: "pointer",
    border: "none",
    backgroundColor: "rgba(255,255,255,0.10)",
    color: "rgba(255,255,255,0.70)",
    transition: "all 0.2s",
  };

  return (
    <motion.div
      style={{ position: "relative", width: "100%" }}
      initial={{ opacity: 0, y: -6 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -6 }}
      transition={{ duration: 0.2, ease: "easeOut" }}
    >
      {/* Left arrow + gradient fade */}
      {canScrollLeft && (
        <>
          <div
            style={{
              position: "absolute",
              left: 0,
              top: 0,
              bottom: 0,
              width: 80,
              zIndex: 10,
              pointerEvents: "none",
              background: NAV_FADE_LEFT,
            }}
          />
          <button
            aria-label="Scroll left"
            onClick={() => scrollBy(-1)}
            style={{
              ...arrowBtnStyle,
              position: "absolute",
              left: 8,
              top: "50%",
              transform: "translateY(-50%)",
              zIndex: 20,
            }}
          >
            <RiArrowLeftSLine size={16} />
          </button>
        </>
      )}

      {/* Scrollable content */}
      <div
        ref={scrollRef}
        style={{
          display: "flex",
          alignItems: "center",
          gap: 24,
          padding: isOverflowing ? "12px 48px" : "12px 32px",
          justifyContent: isOverflowing ? undefined : "center",
          overflowX: "auto",
          overflowY: "hidden",
          cursor: isOverflowing ? "grab" : undefined,
          scrollbarWidth: "none",
          msOverflowStyle: "none",
          maxWidth: "100%",
        }}
        onScroll={checkOverflow}
        onMouseDown={isOverflowing ? handleMouseDown : undefined}
      >
        {subItems.map((item, i) => (
          <div
            key={item}
            style={{ display: "flex", alignItems: "center", gap: 24, flexShrink: 0 }}
          >
            {i > 0 && <DotSeparator />}
            <span
              onClick={() => onSubItemClick?.(item, parentId)}
              style={{
                fontSize: 14,
                fontFamily: "var(--font-poppins, var(--font-sans, sans-serif))",
                fontWeight: 400,
                textTransform: "uppercase",
                letterSpacing: 7,
                color: NEUTRALS_LIGHT,
                whiteSpace: "nowrap",
                userSelect: "none",
                cursor: "default",
                transition: "color 0.2s",
              }}
              onMouseEnter={(e) => (e.currentTarget.style.color = WHITE)}
              onMouseLeave={(e) => (e.currentTarget.style.color = NEUTRALS_LIGHT)}
            >
              {item}
            </span>
          </div>
        ))}
      </div>

      {/* Right arrow + gradient fade */}
      {canScrollRight && (
        <>
          <div
            style={{
              position: "absolute",
              right: 0,
              top: 0,
              bottom: 0,
              width: 80,
              zIndex: 10,
              pointerEvents: "none",
              background: NAV_FADE_RIGHT,
            }}
          />
          <button
            aria-label="Scroll right"
            onClick={() => scrollBy(1)}
            style={{
              ...arrowBtnStyle,
              position: "absolute",
              right: 8,
              top: "50%",
              transform: "translateY(-50%)",
              zIndex: 20,
            }}
          >
            <RiArrowRightSLine size={16} />
          </button>
        </>
      )}
    </motion.div>
  );
}


/* ═══════════════════════════════════════════════════════════════════════════════
 *  MAIN COMPONENT
 * ═══════════════════════════════════════════════════════════════════════════════ */

export default function NexusGlobalNav({
  navItems = [],
  leftSlot = null,
  rightSlot = null,
  onItemClick,
  onSubItemClick,
  className = "",
  style = {},
}) {
  const [activeMenu, setActiveMenu] = useState(null);
  const menuTimeoutRef = useRef(null);

  const handleMenuEnter = (id) => {
    clearTimeout(menuTimeoutRef.current);
    setActiveMenu(id);
  };

  const handleMenuLeave = () => {
    menuTimeoutRef.current = setTimeout(() => setActiveMenu(null), 150);
  };

  const handleSubMenuEnter = () => {
    clearTimeout(menuTimeoutRef.current);
  };

  const activeItem = navItems.find((n) => n.id === activeMenu);

  return (
    <nav
      className={className}
      style={{
        position: "fixed",
        top: 0,
        left: 0,
        right: 0,
        zIndex: 40,
        ...style,
      }}
      onMouseLeave={handleMenuLeave}
    >
      {/* ── Main bar ──────────────────────────────────────────────────────── */}
      <div
        style={{
          height: 80,
          display: "flex",
          alignItems: "center",
          padding: "0 28px",
          gap: 32,
          background: GLASS_NAV,
          backdropFilter: BLUR_NAV,
          WebkitBackdropFilter: BLUR_NAV,
        }}
      >
        {/* Left slot */}
        {leftSlot && (
          <div style={{ flexShrink: 0 }}>{leftSlot}</div>
        )}

        {/* Centre menu items */}
        <div
          style={{
            flex: 1,
            display: "flex",
            alignItems: "center",
            justifyContent: "center",
            gap: 32,
          }}
        >
          {navItems.map((item, i) => (
            <div
              key={item.id}
              style={{ display: "flex", alignItems: "center", gap: 32 }}
            >
              {i > 0 && <Divider />}
              <NavItem
                item={item}
                activeId={activeMenu}
                onEnter={handleMenuEnter}
                onLeave={handleMenuLeave}
                onClick={onItemClick}
              />
            </div>
          ))}
        </div>

        {/* Right slot */}
        {rightSlot && (
          <div style={{ flexShrink: 0 }}>{rightSlot}</div>
        )}
      </div>

      {/* ── Sub-menu dropdown ─────────────────────────────────────────────── */}
      <AnimatePresence>
        {activeItem?.subItems?.length > 0 && (
          <motion.div
            style={{
              borderTop: `1px solid rgba(140, 140, 140, 0.30)`,
              overflow: "hidden",
              background: GLASS_SUB_NAV,
              backdropFilter: BLUR_NAV,
              WebkitBackdropFilter: BLUR_NAV,
            }}
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.25, ease: "easeInOut" }}
            onMouseEnter={handleSubMenuEnter}
            onMouseLeave={handleMenuLeave}
          >
            <SubMenu
              subItems={activeItem.subItems}
              parentId={activeItem.id}
              onSubItemClick={onSubItemClick}
            />
          </motion.div>
        )}
      </AnimatePresence>
    </nav>
  );
}
