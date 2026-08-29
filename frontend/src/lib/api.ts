/**
 * API helpers.
 *
 * The backend reads cluster host and credentials from its own settings file,
 * so nothing here sends auth headers and the browser never holds a password.
 */

export class ApiError extends Error {
  readonly status: number;
  constructor(message: string, status: number) {
    super(message);
    this.name = "ApiError";
    this.status = status;
  }
}

/** Pull the most useful message out of a FastAPI error body. */
async function errorMessage(r: Response): Promise<string> {
  try {
    const body = await r.json();
    const detail = body?.detail ?? body?.message;
    if (typeof detail === "string" && detail) return detail;
    if (Array.isArray(detail) && detail[0]?.msg) return String(detail[0].msg);
  } catch {
    /* not JSON — fall through */
  }
  return `Request failed (HTTP ${r.status})`;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let r: Response;
  try {
    r = await fetch(path, init);
  } catch (e) {
    throw new ApiError(
      e instanceof Error && e.name === "AbortError"
        ? "Request cancelled"
        : "Cannot reach the backend — is it running?",
      0,
    );
  }
  if (!r.ok) throw new ApiError(await errorMessage(r), r.status);
  if (r.status === 204) return undefined as T;
  return (await r.json()) as T;
}

export function apiGet<T = Record<string, unknown>>(
  path: string,
  params?: Record<string, string | number | undefined>,
  signal?: AbortSignal,
): Promise<T> {
  const qs = new URLSearchParams();
  for (const [k, v] of Object.entries(params ?? {})) {
    if (v !== undefined && v !== "") qs.set(k, String(v));
  }
  const q = qs.toString();
  return request<T>(q ? `${path}?${q}` : path, { signal });
}

export function apiPost<T = Record<string, unknown>>(
  path: string,
  body?: unknown,
  signal?: AbortSignal,
): Promise<T> {
  return request<T>(path, {
    method: "POST",
    headers: body !== undefined ? { "Content-Type": "application/json" } : undefined,
    body: body !== undefined ? JSON.stringify(body) : undefined,
    signal,
  });
}

export function apiPut<T = Record<string, unknown>>(path: string, body: unknown): Promise<T> {
  return request<T>(path, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
}

export function apiDelete<T = Record<string, unknown>>(path: string): Promise<T> {
  return request<T>(path, { method: "DELETE" });
}
