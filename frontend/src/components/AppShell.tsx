"use client";

import Link from "next/link";
import { usePathname } from "next/navigation";
import { useEffect, useState } from "react";
import {
  RiSunLine, RiMoonLine, RiComputerLine, RiGithubFill, RiExternalLinkLine,
} from "@remixicon/react";
import { useTheme } from "@/contexts/ThemeContext";
import { useSettings } from "@/contexts/SettingsContext";
import { StatusDot, type Tone } from "@/components/ui";
import { cx } from "@/lib/cx";

const NAV = [
  { href: "/", label: "Overview" },
  { href: "/pipeline", label: "Pipeline" },
  { href: "/setup", label: "Setup" },
];

const REPO_URL = "https://github.com/erdincka/catchx";

/* ── Theme toggle ───────────────────────────────────────────────────────────*/

function ThemeToggle() {
  const { choice, cycle } = useTheme();
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);

  // Render a stable placeholder until mounted: the server cannot know the
  // stored choice, and a mismatch here would hydrate-warn on every page.
  if (!mounted) return <div className="w-7 h-7" aria-hidden />;

  const icon =
    choice === "light" ? <RiSunLine size={15} />
    : choice === "dark" ? <RiMoonLine size={15} />
    : <RiComputerLine size={15} />;

  return (
    <button
      onClick={cycle}
      title={`Theme: ${choice} — click to change`}
      aria-label={`Theme: ${choice}. Click to change.`}
      className="w-7 h-7 grid place-items-center rounded-md text-muted
                 hover:text-text hover:bg-surface-hover transition-colors"
    >
      {icon}
    </button>
  );
}

/* ── Cluster status ─────────────────────────────────────────────────────────*/

function ClusterStatus() {
  const { configured, clusterInfo, ready, services, settings } = useSettings();
  const [mounted, setMounted] = useState(false);
  useEffect(() => setMounted(true), []);
  if (!mounted) return <div className="h-6" aria-hidden />;

  const host = settings?.cluster_host ?? "";
  const probed = Object.keys(services).length > 0;
  const requiredFailing = probed && ["cluster", "s3"].some(
    (k) => services[k] && services[k].status !== "good",
  );

  let tone: Tone = "neutral";
  let label = "Not configured";

  if (!configured) {
    tone = "bad";
    label = "Not configured";
  } else if (ready) {
    tone = "good";
    label = clusterInfo?.name || host;
  } else if (requiredFailing) {
    tone = "bad";
    label = clusterInfo?.name || host;
  } else {
    tone = "warn";
    label = clusterInfo?.name || host;
  }

  const title = !configured
    ? "No cluster configured — open Setup"
    : ready
      ? "Cluster ready"
      : requiredFailing
        ? "A required service is unreachable — open Setup"
        : "Setup incomplete — open Setup";

  return (
    <div className="flex items-center gap-2">
      <Link
        href="/setup"
        title={title}
        className="flex items-center gap-1.5 px-2 h-7 rounded-md text-[12px]
                   text-muted hover:text-text hover:bg-surface-hover transition-colors max-w-[16rem]"
      >
        <StatusDot tone={tone} pulse={ready} />
        <span className="truncate">{label}</span>
      </Link>

      {/* Opens the management console for the operator to sign into. The
          credentials are deliberately not embedded in this URL. */}
      {host && (
        <a
          href={`https://${host}:8443/app/mcs/`}
          target="_blank"
          rel="noreferrer noopener"
          title="Open the Data Fabric management console"
          className="w-7 h-7 grid place-items-center rounded-md text-subtle
                     hover:text-text hover:bg-surface-hover transition-colors"
        >
          <RiExternalLinkLine size={14} />
        </a>
      )}
    </div>
  );
}

/* ── Shell ──────────────────────────────────────────────────────────────────*/

export default function AppShell({
  children,
  footer,
}: {
  children: React.ReactNode;
  footer?: React.ReactNode;
}) {
  const pathname = usePathname();

  return (
    <div className="flex flex-col h-dvh bg-bg">
      <header className="shrink-0 h-13 border-b border-border bg-surface/85 backdrop-blur-md">
        <div className="h-full px-4 flex items-center gap-5">
          <Link href="/" className="flex items-baseline gap-2 shrink-0 group">
            <span className="text-[15px] font-semibold tracking-tight text-text">CatchX</span>
            <span className="hidden sm:inline text-[10px] uppercase tracking-[0.16em] text-subtle
                             group-hover:text-muted transition-colors">
              HPE Data Fabric
            </span>
          </Link>

          <nav className="flex items-center gap-0.5">
            {NAV.map((item) => {
              const active =
                item.href === "/" ? pathname === "/" : pathname.startsWith(item.href);
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  aria-current={active ? "page" : undefined}
                  className={cx(
                    "px-2.5 h-7 inline-flex items-center rounded-md text-[13px] transition-colors",
                    active
                      ? "bg-accent-soft text-accent-text font-medium"
                      : "text-muted hover:text-text hover:bg-surface-hover",
                  )}
                >
                  {item.label}
                </Link>
              );
            })}
          </nav>

          <div className="flex-1" />

          <ClusterStatus />
          <ThemeToggle />
          <a
            href={REPO_URL}
            target="_blank"
            rel="noreferrer noopener"
            title="Source on GitHub"
            className="w-7 h-7 grid place-items-center rounded-md text-subtle
                       hover:text-text hover:bg-surface-hover transition-colors"
          >
            <RiGithubFill size={15} />
          </a>
        </div>
      </header>

      <main className="flex-1 min-h-0 overflow-auto">{children}</main>

      {footer && (
        <div className="shrink-0 border-t border-border bg-surface">{footer}</div>
      )}
    </div>
  );
}
