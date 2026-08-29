"use client";

import { useEffect, useState } from "react";
import hljs from "highlight.js/lib/core";
import python from "highlight.js/lib/languages/python";
import { RiFileCopyLine, RiCheckLine, RiCornerDownRightLine } from "@remixicon/react";
import { Dialog } from "@/components/ui/Dialog";
import { Button, Spinner, EmptyState, Badge, Code } from "@/components/ui";
import { apiGet } from "@/lib/api";
import { cx } from "@/lib/cx";

hljs.registerLanguage("python", python);

interface ChainEntry {
  name: string;
  module: string;
  source: string;
  /** 0 is the function the user asked for; deeper entries are what it calls. */
  depth: number;
  highlights: string[];
}

interface SourceResponse {
  function_name: string;
  module: string;
  source: string;
  highlights: string[];
  chain: ChainEntry[];
}

/**
 * Shows the real server-side implementation of a pipeline step.
 *
 * The entry function is rarely where the interesting part lives — the Kafka
 * producer, the OJAI writes and the Delta merge all sit one call deeper. The
 * backend resolves that call chain from the AST, and this renders the whole
 * thing so the standard-client story is actually visible.
 */
export default function CodeViewer({
  functionName,
  onClose,
}: {
  functionName: string | null;
  onClose: () => void;
}) {
  const [data, setData] = useState<SourceResponse | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [copied, setCopied] = useState<string | null>(null);

  useEffect(() => {
    if (!functionName) return;
    const ac = new AbortController();
    setLoading(true);
    setError(null);
    setData(null);
    setCopied(null);

    apiGet<SourceResponse>(`/api/code/${functionName}`, undefined, ac.signal)
      .then(setData)
      .catch((e) => {
        if (!ac.signal.aborted) setError(e instanceof Error ? e.message : "Failed to load source");
      })
      .finally(() => {
        if (!ac.signal.aborted) setLoading(false);
      });

    return () => ac.abort();
  }, [functionName]);

  async function copy(entry: ChainEntry) {
    try {
      await navigator.clipboard.writeText(entry.source);
      setCopied(entry.name);
      window.setTimeout(() => setCopied(null), 1800);
    } catch {
      /* clipboard blocked — the code is still selectable */
    }
  }

  /** Height of the sticky call-chain bar, so a jumped-to header is not hidden under it. */
  const STICKY_OFFSET = 46;

  function jumpTo(name: string) {
    // Both elements are found from the document by attribute. Refs proved
    // unreliable here — the sections re-render whenever the Copy button's
    // state changes — and only one dialog is ever mounted at a time.
    const target = document.querySelector<HTMLElement>(
      `[data-section="${CSS.escape(name)}"]`,
    );
    const container = target?.closest<HTMLElement>("[data-dialog-scroll]");
    if (!target || !container) return;

    // scrollIntoView picks its own scroll root and left this container almost
    // untouched, so position it explicitly from measured rects instead.
    const top =
      container.scrollTop +
      target.getBoundingClientRect().top -
      container.getBoundingClientRect().top -
      STICKY_OFFSET;

    // Assigned directly rather than scrollTo({behavior:"smooth"}): the smooth
    // variant is a no-op in this container (the page sets scroll-behavior
    // globally), so the jump silently did nothing.
    container.scrollTop = Math.max(0, top);
  }

  return (
    <Dialog
      open={Boolean(functionName)}
      onClose={onClose}
      size="xl"
      title={<span className="font-mono">{functionName}</span>}
      description={
        data
          ? "The code that ran, plus the fabric client calls it makes"
          : undefined
      }
    >
      {loading && (
        <div className="flex items-center justify-center gap-2 py-16 text-muted text-[13px]">
          <Spinner /> Loading source…
        </div>
      )}

      {error && <EmptyState title="Could not load source" hint={error} />}

      {data && (
        <div className="flex flex-col">
          {/* Call chain index — makes it obvious there is more than one function
              here, and which libraries the chain reaches. */}
          {data.chain.length > 1 && (
            <div className="sticky top-0 z-10 px-4 py-2.5 border-b border-border
                            bg-surface/95 backdrop-blur-sm flex flex-wrap items-center gap-1.5">
              <span className="text-[10px] uppercase tracking-[0.14em] text-subtle mr-1">
                Call chain
              </span>
              {data.chain.map((entry, i) => (
                <span key={entry.module + entry.name} className="flex items-center gap-1.5">
                  {i > 0 && <span className="text-subtle text-[11px]">→</span>}
                  <button
                    onClick={() => jumpTo(entry.name)}
                    className="font-mono text-[11px] px-1.5 py-0.5 rounded border border-border
                               text-muted hover:text-text hover:border-border-strong
                               hover:bg-surface-hover transition-colors"
                  >
                    {entry.name}
                  </button>
                </span>
              ))}
              {data.highlights.length > 0 && (
                <span className="flex items-center gap-1 ml-auto">
                  {data.highlights.map((h) => (
                    <Badge key={h} tone="accent">{h}</Badge>
                  ))}
                </span>
              )}
            </div>
          )}

          {data.chain.map((entry) => (
            <div
              key={entry.module + entry.name}
              data-section={entry.name}
              className="border-b border-border last:border-b-0"
            >
              <div className="flex items-center gap-2 px-4 py-2 bg-surface-sunk/70 flex-wrap">
                {entry.depth > 0 && (
                  <RiCornerDownRightLine
                    size={13}
                    className="text-subtle shrink-0"
                    style={{ marginLeft: (entry.depth - 1) * 12 }}
                  />
                )}
                <span className="font-mono text-[12px] font-medium text-text">{entry.name}</span>
                <Code className="text-[10.5px]">{entry.module}</Code>
                {entry.highlights.map((h) => (
                  <Badge key={h} tone="accent">{h}</Badge>
                ))}
                {entry.depth === 0 && <Badge tone="neutral">entry point</Badge>}

                <Button
                  variant="ghost"
                  size="sm"
                  className="ml-auto"
                  onClick={() => copy(entry)}
                  icon={
                    copied === entry.name
                      ? <RiCheckLine size={12} />
                      : <RiFileCopyLine size={12} />
                  }
                >
                  {copied === entry.name ? "Copied" : "Copy"}
                </Button>
              </div>

              <pre className={cx("m-0 p-4 overflow-auto text-[12px] leading-[1.65]")}>
                <code
                  className="hljs font-mono bg-transparent"
                  dangerouslySetInnerHTML={{
                    __html: hljs.highlight(entry.source, { language: "python" }).value,
                  }}
                />
              </pre>
            </div>
          ))}
        </div>
      )}
    </Dialog>
  );
}
