# CatchX — a fraud detection pipeline on HPE Ezmeral Data Fabric

An end-to-end demo of the HPE Ezmeral Data Fabric: transactions arrive on a
stream, customers arrive as a CSV, and both move through a bronze → silver →
gold medallion architecture into a shareable data product with suspected fraud
flagged.

Every step runs against a real cluster, and the app shows the actual code that
ran — including the standard Kafka, OJAI, Iceberg and Delta Lake calls
underneath.

This is not a real fraud model. The scoring is deliberately trivial: the point
is the data platform, not the algorithm. The tools were chosen for how simply
they demonstrate the fabric, not to constrain what you would use in production —
Data Fabric speaks standard protocols, so the same code works with your own
choice of engine.

## What it demonstrates

| Capability | Where you see it |
|------------|------------------|
| Global Namespace (NFS) | Data written to `/mapr` as ordinary files; browse any tier from the app |
| Streams (Kafka API) | Transactions published and consumed with `confluent_kafka` |
| DocumentDB (OJAI) | Bronze and silver JSON tables |
| Apache Iceberg | Bronze customers, catalogued **inside the global namespace** |
| Delta Lake | Gold-tier data product, updated by merge |
| S3 object store | Access keys generated through the cluster API |
| Data Fabric MCP | Optional discovery of the fabric's agent-callable tools |

## Prerequisites

### A Data Fabric cluster

You need a running HPE Ezmeral Data Fabric cluster (7.x or later) that this app
can reach. The demo creates and destroys its own volumes, tables and streams, so
**use a lab or demo cluster, not production.**

Required packages on the cluster:

```bash
mapr-kafka                 # streams
mapr-data-access-gateway   # DocumentDB / OJAI access on :5678
mapr-nfs                   # NFSv3 server for the global namespace
```

The object store (S3, port 9000) ships with the fabric and must be running.

### Cluster account

The app needs an account with **administrative rights**, because it creates and
deletes cluster artefacts on your behalf:

| It does this | Which needs |
|--------------|-------------|
| Creates 4 volumes under `/catchx-demo` | volume create / remove |
| Creates DocumentDB tables and streams | table and stream create / delete |
| Generates an S3 access key | S3 key generation |
| Runs `configure.sh` and mounts NFS | SSH access to a cluster node, and `sudo` on the client |
| Reads cluster and stream telemetry | REST API read on :8443 |

On an isolated demo cluster the simplest choice is the cluster admin (`mapr`)
user. Otherwise create a user with volume, table and stream management rights,
and SSH access to the node you point the app at.

### Ports the app must reach

| Service | Port | Required |
|---------|------|----------|
| Cluster REST API | 8443 | yes |
| Data Access Gateway (OJAI) | 5678 | yes |
| S3 object store | 9000 | yes |
| NFS | 2049 | yes |
| SSH | 22 | yes — for client configuration |
| Data Fabric MCP | 5679 | no |

Nothing else. No Grafana, OpenTSDB, Fluentd, Livy or external Iceberg catalog —
the demo used none of them. Stream throughput and consumer lag come from the
cluster's own REST API, and the Iceberg catalog is a SQL catalog stored in the
global namespace.

The Setup page probes all of this and tells you what is missing, so you do not
have to verify it by hand first.

### Where the app runs

Docker with the Compose plugin. The backend container runs **privileged** — it
mounts the cluster's global namespace over NFS itself — so the host kernel needs
NFS support (`nfs` / `nfsd` modules available). Most Linux hosts have this;
Docker Desktop on macOS and Windows generally does not, so run it on a Linux
host or VM with network access to the cluster.

See [EXTRAS.md](./EXTRAS.md) for optional cluster extras.

## Run it

Pull the published images:

```bash
git clone https://github.com/erdincka/catchx
cd catchx
docker compose up -d
```

Or build them yourself:

```bash
docker compose up -d --build
```

The backend image is large (~5 GB) — it is built on the MapR PACC base image,
which carries the full client stack. The first pull takes a while.

Open <http://localhost:3000> and work down the **Setup** page:

1. **Cluster connection** — host, username, password. Stored on the backend, so
   it survives a browser refresh.
2. **Required services** — probe the cluster and object store.
3. **Configure the client** — deploys an SSH key, fetches the truststore, runs
   `configure.sh`, and mounts `/mapr` over NFS.
4. **Provision** — creates the demo volumes, tables and streams.
5. **Object store access** — generates S3 keys through the cluster API.

Then open **Pipeline** and run the six steps.

Prefer a hostname over an IP address: MapR clusters usually carry a wildcard
certificate that an IP can never match. The app detects this and works around it
for DocumentDB, but a hostname avoids the problem entirely.

Backend API docs: <http://localhost:8000/docs>.

## The pipeline

```
customers.csv ─────[batch]─────► Iceberg (bronze) ─────┐
                                                       ├──► silver ──► Delta Lake (gold)
transactions.csv ──[stream]──► DocumentDB (bronze) ────┘                     │
                                                                      flagged fraud
```

1. **Generate** — write customer and transaction CSVs into the global namespace
2. **Publish** — push transactions onto a fabric stream via the Kafka API
3. **Ingest** — stream into DocumentDB, batch-load the CSV into Iceberg
4. **Refine** — enrich, categorise, mask personal data, build risk profiles
5. **Consolidate** — merge into a Delta Lake data product
6. **Detect** — score transactions and flag suspected fraud

Steps unlock in order, and completion is read from the cluster rather than from
what you clicked — so a page reload, or someone else having run half the demo,
still shows the truth. **Expert** mode removes the ordering when you want to
jump straight to a particular step.

Click any populated node in the diagram to inspect its records, or the `</>`
button on a step to see the code that ran, including the fabric client calls it
makes.

Everything lives under `/catchx-demo` on the cluster. **Delete demo data** on
the Setup page removes it all so you can run again from step 4. A full clean run
takes about a minute.

## Deploying on Kubernetes

A Helm chart is in `helm/`, running the same two images
(`ghcr.io/erdincka/catchx-backend`, `ghcr.io/erdincka/catchx-frontend`) in one
pod. The backend
needs `SYS_ADMIN` for the NFS mount, and a volume mounted at `/app/data` if you
want settings to persist across restarts.

## Notes

- The backend container performs the NFS mount itself. Do not bind-mount the
  host's `/mapr` over it.
- Recreating the backend container drops the MapR client configuration and the
  mount; re-run **Configure the client** afterwards.
- Generating customers appends, so running it repeatedly grows the dataset.
- Light and dark themes follow your system setting; the toggle in the header
  overrides it.

## Working on the code

See [CLAUDE.md](./CLAUDE.md) for architecture, conventions, and the constraints
worth knowing before changing anything.
