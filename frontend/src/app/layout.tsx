import type { Metadata } from "next";
import { Inter, JetBrains_Mono } from "next/font/google";
import "./globals.css";
import { ThemeProvider } from "@/contexts/ThemeContext";
import { THEME_KEY } from "@/lib/theme";
import { SettingsProvider } from "@/contexts/SettingsContext";
import { ToastProvider } from "@/contexts/ToastContext";

const inter = Inter({
  variable: "--font-inter",
  subsets: ["latin"],
  display: "swap",
});

const jetbrainsMono = JetBrains_Mono({
  variable: "--font-jetbrains-mono",
  subsets: ["latin"],
  weight: ["400", "500", "600"],
  display: "swap",
});

export const metadata: Metadata = {
  title: "CatchX — HPE Data Fabric",
  description:
    "An end-to-end fraud detection pipeline on HPE Ezmeral Data Fabric: streams, DocumentDB, Iceberg and Delta Lake across a bronze/silver/gold medallion architecture.",
};

/* Runs before first paint so the correct theme is on <html> immediately.
   Without it the page paints light, then flips — obvious on a projector. */
const themeBootstrap = `
(function () {
  try {
    var c = localStorage.getItem(${JSON.stringify(THEME_KEY)});
    var dark = c === "dark" || ((c === "system" || !c) &&
      window.matchMedia("(prefers-color-scheme: dark)").matches);
    if (dark) document.documentElement.classList.add("dark");
  } catch (e) {}
})();
`;

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html
      lang="en-GB"
      suppressHydrationWarning
      className={`${inter.variable} ${jetbrainsMono.variable}`}
    >
      <body>
        {/* First element in the body so the class lands before any content
            paints. Next owns <head> in the App Router, so injecting there
            fights its head reconciliation. */}
        <script dangerouslySetInnerHTML={{ __html: themeBootstrap }} />
        <ThemeProvider>
          <SettingsProvider>
            <ToastProvider>{children}</ToastProvider>
          </SettingsProvider>
        </ThemeProvider>
      </body>
    </html>
  );
}
