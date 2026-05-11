import type { Metadata } from "next";
import { Poppins, Instrument_Serif, JetBrains_Mono } from "next/font/google";
import "./globals.css";
import { ClusterProvider } from "@/contexts/ClusterContext";
import { SettingsProvider } from "@/contexts/SettingsContext";
import { ToastProvider } from "@/contexts/ToastContext";

const poppins = Poppins({
  variable: "--font-poppins",
  subsets: ["latin"],
  weight: ["300", "400", "500", "600", "700"],
  display: "swap",
});

const instrumentSerif = Instrument_Serif({
  variable: "--font-instrument-serif",
  subsets: ["latin"],
  weight: ["400"],
  display: "swap",
});

const jetbrainsMono = JetBrains_Mono({
  variable: "--font-jetbrains-mono",
  subsets: ["latin"],
  weight: ["400", "500", "600"],
  display: "swap",
});

export const metadata: Metadata = {
  title: "NexMesh — HPE Data Fabric Capability Tour",
  description: "End-to-end demo of HPE Ezmeral Data Fabric — Iceberg, Polaris, Spark, Flink, MCP, Grafana.",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html
      lang="en-GB"
      data-scroll-behavior="smooth"
      className={`${poppins.variable} ${instrumentSerif.variable} ${jetbrainsMono.variable}`}
    >
      <body>
        <SettingsProvider>
          <ClusterProvider>
            <ToastProvider>
              {children}
            </ToastProvider>
          </ClusterProvider>
        </SettingsProvider>
      </body>
    </html>
  );
}
