"use client";

import React, { createContext, useCallback, useContext, useEffect, useState } from "react";
import { THEME_KEY, type ThemeChoice } from "@/lib/theme";

export type { ThemeChoice };

interface ThemeState {
  /** What the user picked. */
  choice: ThemeChoice;
  /** What is actually on screen once "system" is resolved. */
  resolved: "light" | "dark";
  setChoice: (c: ThemeChoice) => void;
  /** Cycles light → dark → system. */
  cycle: () => void;
}

const ThemeContext = createContext<ThemeState | null>(null);

function systemPrefersDark(): boolean {
  return typeof window !== "undefined"
    && window.matchMedia("(prefers-color-scheme: dark)").matches;
}

function apply(choice: ThemeChoice): "light" | "dark" {
  const resolved = choice === "system" ? (systemPrefersDark() ? "dark" : "light") : choice;
  const root = document.documentElement;
  // Kill transitions for one frame so the swap doesn't animate every element.
  root.classList.add("theme-switching");
  root.classList.toggle("dark", resolved === "dark");
  window.setTimeout(() => root.classList.remove("theme-switching"), 0);
  return resolved;
}

export function ThemeProvider({ children }: { children: React.ReactNode }) {
  // Start with "system" and reconcile on mount — the inline script in layout.tsx
  // has already set the class, so there is no flash either way.
  const [choice, setChoiceState] = useState<ThemeChoice>("system");
  const [resolved, setResolved] = useState<"light" | "dark">("light");

  useEffect(() => {
    let stored: ThemeChoice = "system";
    try {
      const raw = localStorage.getItem(THEME_KEY);
      if (raw === "light" || raw === "dark" || raw === "system") stored = raw;
    } catch {
      /* private mode / blocked storage — system default is fine */
    }
    setChoiceState(stored);
    setResolved(apply(stored));
  }, []);

  // Follow the OS while the user is on "system".
  useEffect(() => {
    if (choice !== "system") return;
    const mq = window.matchMedia("(prefers-color-scheme: dark)");
    const onChange = () => setResolved(apply("system"));
    mq.addEventListener("change", onChange);
    return () => mq.removeEventListener("change", onChange);
  }, [choice]);

  const setChoice = useCallback((c: ThemeChoice) => {
    setChoiceState(c);
    setResolved(apply(c));
    try {
      localStorage.setItem(THEME_KEY, c);
    } catch {
      /* non-fatal: the choice just won't survive a reload */
    }
  }, []);

  const cycle = useCallback(() => {
    setChoice(choice === "light" ? "dark" : choice === "dark" ? "system" : "light");
  }, [choice, setChoice]);

  return (
    <ThemeContext.Provider value={{ choice, resolved, setChoice, cycle }}>
      {children}
    </ThemeContext.Provider>
  );
}

export function useTheme(): ThemeState {
  const ctx = useContext(ThemeContext);
  if (!ctx) throw new Error("useTheme must be used within ThemeProvider");
  return ctx;
}
