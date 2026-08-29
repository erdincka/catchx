/**
 * Shared between the server-rendered root layout and the client theme
 * provider, so it deliberately has no "use client" directive — importing a
 * value out of a client module would pull that module into the server graph.
 */
export const THEME_KEY = "catchx-theme";

export type ThemeChoice = "light" | "dark" | "system";
