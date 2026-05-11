export interface SSEEvent {
  name: string;
  status: "running" | "check" | "error";
  message: string;
}

/**
 * POST to an SSE endpoint and call onEvent for each data line.
 * Works around the browser EventSource limitation (no POST support).
 */
export async function postSSE(
  url: string,
  body: Record<string, unknown>,
  onEvent: (e: SSEEvent) => void,
  signal?: AbortSignal,
): Promise<void> {
  const r = await fetch(url, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
    signal,
  });
  if (!r.ok || !r.body) throw new Error(`HTTP ${r.status}`);
  const reader = r.body.getReader();
  const decoder = new TextDecoder();
  let buf = "";
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    buf += decoder.decode(value, { stream: true });
    const parts = buf.split("\n\n");
    buf = parts.pop() ?? "";
    for (const part of parts) {
      const line = part.trim().replace(/^data:\s*/, "");
      if (!line) continue;
      try { onEvent(JSON.parse(line) as SSEEvent); } catch { /* skip malformed */ }
    }
  }
}
