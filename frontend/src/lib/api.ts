export interface Credentials {
  host: string;
  user: string;
  pass: string;
}

function credHeaders(creds: Credentials): HeadersInit {
  return {
    "X-Mapr-Host": creds.host,
    "X-Mapr-User": creds.user,
    "X-Mapr-Pass": creds.pass,
    "Content-Type": "application/json",
  };
}

export async function apiGet(
  path: string,
  creds: Credentials,
  params?: Record<string, string>
): Promise<Response> {
  const url = new URL(path, window.location.origin);
  if (params) Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));
  return fetch(url.toString(), { headers: credHeaders(creds) });
}

export async function apiPost(
  path: string,
  creds: Credentials,
  body?: unknown
): Promise<Response> {
  return fetch(path, {
    method: "POST",
    headers: credHeaders(creds),
    body: body !== undefined ? JSON.stringify(body) : undefined,
  });
}

export async function apiDelete(path: string, creds: Credentials): Promise<Response> {
  return fetch(path, { method: "DELETE", headers: credHeaders(creds) });
}
