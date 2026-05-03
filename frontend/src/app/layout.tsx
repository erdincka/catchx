import type { Metadata } from "next";
import { Poppins, Instrument_Serif } from "next/font/google";
import "./globals.css";
import { ClusterProvider } from "@/contexts/ClusterContext";
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

export const metadata: Metadata = {
  title: "NexMesh — Nexus Data Mesh",
  description: "NexMesh — Hybrid Data Mesh powered by HPE Data Fabric",
};

export default function RootLayout({ children }: { children: React.ReactNode }) {
  return (
    <html lang="en-GB" className={`${poppins.variable} ${instrumentSerif.variable}`}>
      <body>
        <ClusterProvider>
          <ToastProvider>
            {children}
          </ToastProvider>
        </ClusterProvider>
      </body>
    </html>
  );
}
