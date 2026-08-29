/**
 * Streaming proxy for the backend's configure SSE endpoint.
 *
 * next.config.ts rewrites cover ordinary /api/* calls, but a rewrite can
 * buffer a streaming response, which would make setup steps arrive in one
 * lump at the end instead of live. Passing the body through explicitly keeps
 * the progress feed real-time.
 */

export const dynamic = "force-dynamic";

const BACKEND = process.env.BACKEND_URL ?? "http://localhost:8000";

export async function POST() {
  let upstream: Response;
  try {
    upstream = await fetch(`${BACKEND}/api/cluster/configure`, { method: "POST" });
  } catch (e) {
    return new Response(`Backend unreachable: ${e}`, { status: 502 });
  }

  if (!upstream.ok || !upstream.body) {
    const detail = await upstream.text().catch(() => "");
    return new Response(detail || "Upstream error", { status: upstream.status || 502 });
  }

  return new Response(upstream.body, {
    status: 200,
    headers: {
      "Content-Type": "text/event-stream",
      "Cache-Control": "no-cache, no-transform",
      "X-Accel-Buffering": "no",
      Connection: "keep-alive",
    },
  });
}
