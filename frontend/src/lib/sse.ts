/**
 * Single SSE reader for the backend's progress streams.
 *
 * There used to be two of these with different framing assumptions, which is
 * why one dialog's step indicators never updated. Everything now goes through
 * this one, which frames on the blank line that terminates an SSE event and
 * tolerates chunk boundaries falling anywhere.
 */

export type StepStatus = "running" | "check" | "error";

export interface SSEEvent {
  name: string;
  status: StepStatus;
  message: string;
}

export interface SSEOptions {
  signal?: AbortSignal;
  /** Method defaults to POST; the backend's streams are POST endpoints. */
  method?: "POST" | "GET";
  body?: unknown;
}

export async function readSSE(
  url: string,
  onEvent: (e: SSEEvent) => void,
  { signal, method = "POST", body }: SSEOptions = {},
): Promise<void> {
  const r = await fetch(url, {
    method,
    headers: body !== undefined ? { "Content-Type": "application/json" } : undefined,
    body: body !== undefined ? JSON.stringify(body) : undefined,
    signal,
  });

  if (!r.ok) {
    let detail = `HTTP ${r.status}`;
    try {
      const j = await r.json();
      if (j?.detail) detail = String(j.detail);
    } catch {
      /* keep the status line */
    }
    throw new Error(detail);
  }
  if (!r.body) throw new Error("Streaming is not supported by this response");

  const reader = r.body.getReader();
  const decoder = new TextDecoder();
  let buffer = "";

  try {
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      buffer += decoder.decode(value, { stream: true });

      // SSE events are separated by a blank line; \r\n\r\n covers proxies
      // that rewrite line endings.
      const parts = buffer.split(/\r?\n\r?\n/);
      buffer = parts.pop() ?? "";

      for (const part of parts) {
        for (const line of part.split(/\r?\n/)) {
          if (!line.startsWith("data:")) continue;
          const payload = line.slice(5).trim();
          if (!payload) continue;
          try {
            const parsed = JSON.parse(payload);
            if (parsed && typeof parsed.name === "string") onEvent(parsed as SSEEvent);
          } catch {
            /* ignore a malformed frame rather than killing the stream */
          }
        }
      }
    }
  } finally {
    reader.cancel().catch(() => {});
  }
}
