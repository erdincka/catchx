# CatchX — HPE Data Fabric Demo App

## Overview

End-to-end fraud detection demo showcasing HPE Ezmeral Data Fabric capabilities: streaming, batch ingestion, medallion architecture (Bronze → Silver → Gold), and real-time monitoring.

## Architecture

Two containers communicating over a Docker network (`catchx-net`):

```
frontend (NiceGUI, port 3000)  ←→  backend (FastAPI, port 8000)
         ↑                                    ↓
    Browser UI                    HPE Data Fabric (MapR)
```

### Backend (`./backend/`)
- **FastAPI** on port 8000
- All HPE Data Fabric operations: MapR OJAI, Iceberg, Delta Lake, Kafka Streams
- Requires `erdincka/dfclient` base image (MapR client libs at `/opt/mapr/lib`)
- Must run `privileged: true` for NFS mount operations

### Frontend (`./frontend/`)
- **NiceGUI** on port 3000
- Pure UI — no Data Fabric libraries
- Calls backend via HTTP (credentials passed as headers per request)
- Uses `python:3.12-slim` base image

## Key Design Decisions

### Credential Flow
Credentials (host/user/password) are collected in the frontend connect dialog and stored in NiceGUI's **server-side** `app.storage.user` dict (not browser localStorage). Every API call sends them as HTTP headers:
```
X-Mapr-Host: <cluster-ip>
X-Mapr-User: <username>
X-Mapr-Pass: <password>
```
The backend is stateless — it derives `ClusterConfig` from headers via FastAPI dependency injection. Cluster info (name, IP) is cached in-memory per host in `backend/store.py`.

### Monitoring
Frontend polls `GET /api/monitoring/metrics` every 3 seconds via `ui.timer`. The backend also exposes `GET /api/monitoring/stream` (SSE) for external consumers. Monitoring activates when the "Monitor" toggle is on in the header.

### Code Viewer
Backend exposes `GET /api/code/{function_name}` — uses `inspect.getsource()` server-side. Frontend fetches and renders in a dialog with `ui.code()`. Special cases: `airflow_dag` reads a file, `nifi_template` renders a Jinja2 template.

### Routes
| URL | Description |
|-----|-------------|
| `/` | Mesh architecture interactive image (was `/mesh`) |
| `/fraud` | Fraud domain pipeline interactive image |
| `/old` | Step-by-step demo UI with monitoring panel (was `/`) |

## Directory Structure

```
catchx/
├── backend/
│   ├── main.py              # FastAPI entry point
│   ├── config.py            # All constants (shared via API)
│   ├── store.py             # ClusterConfig, cluster info cache
│   ├── routes/
│   │   ├── cluster.py       # /api/cluster/* (connect, setup, volumes, cleanup)
│   │   ├── data.py          # /api/data/* (ingest, refine, consolidate, peek)
│   │   ├── monitoring.py    # /api/monitoring/* (metrics snapshot + SSE stream)
│   │   └── code.py          # /api/code/* (source code viewer)
│   ├── services/
│   │   ├── functions.py     # Business logic (refine, consolidate, fraud)
│   │   ├── ingestion.py     # Batch + streaming ingestion
│   │   ├── mock.py          # Faker data generation + S3 upload
│   │   ├── monitoring.py    # Metric collection functions
│   │   ├── tables.py        # MapR OJAI + Delta Lake I/O
│   │   ├── iceberger.py     # Apache Iceberg operations
│   │   └── streams.py       # Confluent Kafka/MapR Streams
│   ├── images/              # Static images served at /images
│   ├── templates/           # NiFi Jinja2 templates
│   ├── DAGs/                # Airflow DAG files
│   ├── Dockerfile
│   └── requirements.txt
├── frontend/
│   ├── main.py              # NiceGUI entry point
│   ├── config.py            # Frontend constants + BACKEND_URL
│   ├── client.py            # httpx wrapper (api_get, api_post, api_delete)
│   ├── theme.py             # HPE color palette + UI helpers
│   ├── pages/
│   │   ├── mesh.py          # / route
│   │   ├── domain.py        # /fraud route
│   │   └── old.py           # /old route
│   ├── components/
│   │   ├── header.py        # Top bar with toggles and cluster link
│   │   ├── footer.py        # Volume navigation buttons
│   │   ├── gui.py           # SVG interactive images + action handlers
│   │   ├── monitoring.py    # Metric cards, ticker, ECharts, log card
│   │   ├── demo_steps.py    # Step-by-step expansion list
│   │   └── dialogs.py       # Connect dialog, settings, code viewer, table viewer
│   ├── images/              # Static images for local serving
│   ├── Dockerfile
│   └── requirements.txt
├── docker-compose.yaml      # Two-service deployment
└── CLAUDE.md                # This file
```

## Data Pipeline

```
CSV (customers) ──[batch]──► Iceberg (bronze) ──► DocumentDB (silver) ──► Delta Lake (gold)
                                                                              ↑
Kafka (transactions) ──[stream]──► DocumentDB (bronze) ──► DocumentDB (silver) ─┘
                                        ↓
                               Profiles (silver)  ──────────────────────────────┘
```

All data lives under `/mapr/<cluster_name>/demovol/{bronze,silver,gold}/`.

## Running Locally (without Docker)

**Backend:**
```bash
cd backend
uv pip install -r requirements.txt
uv run uvicorn main:app --reload --port 8000
```

**Frontend:**
```bash
cd frontend
BACKEND_URL=http://localhost:8000 uv run python main.py
```

## Building Containers

```bash
docker compose build
docker compose up -d
```

Frontend: http://localhost:3000  
Backend API docs: http://localhost:8000/docs

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `BACKEND_URL` | `http://localhost:8000` | Backend URL (set in frontend container) |
| `LD_LIBRARY_PATH` | `/opt/mapr/lib` | MapR native libs (backend only) |

## API Reference

| Method | Path | Description |
|--------|------|-------------|
| POST | `/api/cluster/connect` | Connect and cache cluster info |
| POST | `/api/cluster/setup` | Full setup (SSE stream of steps) |
| DELETE | `/api/cluster/cleanup` | Remove all volumes and streams |
| POST | `/api/data/customers/create` | Generate customer CSV |
| POST | `/api/data/transactions/create` | Generate transaction CSV |
| POST | `/api/data/transactions/publish` | Publish to Kafka stream |
| POST | `/api/data/ingest/customers` | CSV → Iceberg bronze |
| POST | `/api/data/ingest/transactions` | Stream → DocumentDB bronze |
| POST | `/api/data/refine/customers` | Bronze → Silver enrichment |
| POST | `/api/data/refine/transactions` | Bronze → Silver enrichment |
| POST | `/api/data/consolidate` | Silver → Gold Delta Lake |
| POST | `/api/data/fraud` | Fraud detection on transactions |
| GET | `/api/data/peek/{tier}/{table}` | Preview table records |
| GET | `/api/monitoring/metrics` | Current metrics snapshot |
| GET | `/api/monitoring/stream` | SSE real-time metrics stream |
| GET | `/api/code/{function_name}` | Source code for named function |

## HPE Data Fabric Features Demonstrated

| Feature | Where |
|---------|-------|
| MapR Streams (Kafka-compatible) | `services/streams.py`, publish/consume transactions |
| MapR DocumentDB (OJAI) | `services/tables.py`, bronze + silver JSON tables |
| Apache Iceberg | `services/iceberger.py`, bronze customers table |
| Delta Lake | `services/tables.py`, gold tier analytics tables |
| Global Namespace (NFS) | Footer volume buttons, `/mapr` mount |
| REST Management API | `routes/cluster.py`, volume/stream creation |
| NiFi integration | `routes/code.py` nifi_template rendering |
| Airflow DAG | `DAGs/csv_to_iceberg_DAG.py` |
| S3 Object Store | `services/mock.py` upload_to_s3 |
