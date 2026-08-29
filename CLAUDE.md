# CLAUDE.md

Guidance for Claude Code working in this repository.

## What this is

CatchX is a **public demo** of HPE Ezmeral Data Fabric: a fraud-detection
pipeline that moves data through a bronze/silver/gold medallion architecture
using the fabric's streams, document store, table formats and global namespace.

It is presented live to customers. That shapes every decision here:

- **The audience is watching the platform, not the app.** UI polish serves the
  demo; it is never the point. When in doubt, make the Data Fabric capability
  more visible.
- **It must not break on stage.** Prefer a clear error and a recoverable state
  over a clever optimisation.
- **It must be honest.** Do not show a feature that does nothing. Several
  services were removed precisely because they only lit up a status dot.

## Non-negotiables

1. **Settings are the single source of truth.** `backend/settings.py` owns
   cluster host, credentials and targets. The browser holds no credentials and
   sends no auth headers. Never reintroduce a second store — a split between
   browser state and this file is what made "configured" in the UI mean nothing
   on the server.
2. **Never block the event loop.** The MapR OJAI, pyiceberg, deltalake and
   Kafka clients are all synchronous. Anything touching them goes through
   `to_thread` (see `backend/asyncutil.py`). Marking a function `async def`
   around blocking I/O freezes the whole API, including the metrics poll that
   drives the live UI.
3. **Python 3.8.** The PACC base image ships 3.8. No `asyncio.to_thread`, no
   PEP 604 unions at runtime. Add `from __future__ import annotations` when
   using `list[str]`-style annotations.
4. **Credentials never go in URLs** — not in query strings, not as
   `user:pass@host`. Both existed here before and both leak into logs and
   browser history.
5. **No shell strings built from caller input.** `/api/data/fs/list` takes a
   caller-supplied path; it uses `create_subprocess_exec` with the path as an
   argv element and confines it to the NFS mount.

## Architecture

Two containers on one network. The backend holds every Data Fabric dependency;
the frontend is pure UI and proxies `/api/*` to it.

```
frontend (Next.js :3000)  ──►  backend (FastAPI :8000)  ──►  Data Fabric cluster
```

### Backend (`backend/`)

| Path | Responsibility |
|------|----------------|
| `settings.py` | Persisted config, the single source of truth |
| `store.py` | `ClusterConfig` derived from settings; cluster-info cache |
| `asyncutil.py` | `to_thread` shim (3.8-safe) |
| `routes/cluster.py` | SSE client configuration and provisioning, readiness, cleanup |
| `routes/data.py` | Pipeline steps, table inspection, global namespace |
| `routes/monitoring.py` | Metrics snapshot + SSE stream |
| `routes/code.py` | Source viewer with AST-resolved call chains |
| `routes/mcp.py` | Data Fabric MCP tool discovery |
| `services/` | The actual fabric work — see below |

`services/` is what the demo is about:

- `streams.py` — Kafka producer/consumer (`confluent_kafka`)
- `tables.py` — DocumentDB via OJAI, plus Delta Lake
- `iceberger.py` — Apache Iceberg, catalogued to SQLite **in the global namespace**
- `functions.py` — refine, consolidate, fraud scoring
- `ingestion.py` — stream → bronze, CSV → Iceberg
- `monitoring.py` — telemetry from the cluster REST API
- `probes.py` — service reachability

### Frontend (`frontend/src/`)

- `lib/pipeline.ts` — **the demo, defined as data.** Steps, actions, copy, and
  completion predicates all live here. Change the demo flow here first.
- `contexts/SettingsContext.tsx` — settings, probes, readiness
- `contexts/MetricsContext.tsx` — the 3s poll, kept separate so it does not
  re-render everything
- `components/ui/` — the component kit; no component hardcodes a colour
- `app/globals.css` — semantic design tokens, light and dark

## Conventions

- **Theme:** every colour is a semantic token in `globals.css`
  (`bg-surface`, `text-muted`, `border`, `accent`, tier colours). Never write a
  hex value in a component. Both light and dark must work.
- **Step completion is derived from live metrics**, never from click history —
  so progress survives a reload and reflects the cluster.
- **Errors tell the presenter what to do next**: "No transactions in the bronze
  tier — run Ingest first", not a stack trace.
- **British English** in user-facing copy.

## Required services

Only three, deliberately:

| Service | Port | Required |
|---------|------|----------|
| Cluster REST API | 8443 | yes |
| S3 object store | 9000 | yes |
| Data Fabric MCP | 5679 | optional |

Grafana, OpenTSDB, Fluentd, Livy and an external Iceberg catalog were removed —
nothing used them. Telemetry comes from the cluster's own REST API. **Do not
add a service dependency without a code path that needs it.**

## Development

The containers run dev servers (`uvicorn --reload`, `next dev`) with source
baked into the image. Against a remote Docker context, bind mounts resolve on
the *remote* host, so sync source in instead:

```bash
docker --context <ctx> compose up -d --build
```

**Recreating the backend container drops its MapR client config and `/mapr`
NFS mount**, which then needs `POST /api/cluster/configure` again. For
iteration, copy source into the running container and let the watchers reload —
much faster, and the mount survives.

`next dev` does not fail on type errors. Run `tsc --noEmit` after syncing, and
a real `compose build` before considering anything done.

Note: the HMR websocket may not reach the browser when the app is accessed by
IP. If a change does not appear, do a full page reload rather than assuming the
edit failed.

## Gotchas that cost real time

- **OJAI is not safe for concurrent multi-store reads on one connection.**
  Counting several tables with `asyncio.gather` intermittently returned 0 for
  one of them. Tier counts are sequential for this reason. Concurrent *writes*
  to a single store are fine and are ~5x faster.
- **Table creation is visible over NFS slightly before the directory entry is.**
  Do not gate on `os.path.lexists` right after a write; read the table instead.
- **MapR clusters use wildcard certs** (`CN=*.example.com`), which an IP can
  never match. `tables.resolve_target_name` handles this for OJAI; prefer a
  hostname for the cluster where possible.
- **Metrics tier counts are cached** (`COUNT_TTL`) and invalidated by
  `monitoring.invalidate()`. Any new route that writes data must call `_touch`.

## Testing against a cluster

There is no test suite. Verify by running the pipeline end to end:

```
POST /api/cluster/configure      # SSE, 8 steps
POST /api/cluster/provision      # SSE, 3 steps
POST /api/data/customers/create
POST /api/data/transactions/create
POST /api/data/transactions/publish
POST /api/data/ingest/transactions
POST /api/data/ingest/customers
POST /api/data/refine/transactions
POST /api/data/refine/customers
POST /api/data/consolidate
POST /api/data/fraud
DELETE /api/cluster/cleanup      # reset and repeat
```

A full clean run takes roughly a minute. If a step takes dramatically longer,
suspect a per-record round trip that should be batched.
