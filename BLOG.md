# Five data engines, one namespace: a fraud pipeline on HPE Data Fabric

> **Before you read this.** This is an engineering demo, not a production
> reference architecture. I used AI extensively while building and documenting
> it, but I reviewed the implementation and validated every claim below against a
> running Data Fabric 8.1.0 cluster. The repository ships the verification steps
> and the failure modes I hit, so the claims can be checked rather than taken on
> trust.
>
> It deliberately does not try to be a data platform. There is no governance
> layer, no lineage graph, no catalogue of catalogues, and the fraud model is
> four lines of arithmetic. The goal is narrower: to show what changes when the
> storage layer stops being a thing you integrate with and starts being a thing
> you write to.

Every data platform conversation I have ends up at the same question, and it is
never "which engine". It is **how many systems is that, and who runs them?**

The honest answer, for a fairly ordinary medallion architecture, is usually
uncomfortable. You want streaming, so that is Kafka and ZooKeeper. You want a
document store for raw events. You want open table formats, so that is Iceberg,
and Iceberg wants a catalogue service, and the catalogue wants a database. You
want object storage. You want the whole thing visible as files for the tools
that only speak POSIX. You want metrics, so there is a time-series database and
a dashboard in front of it. None of these are unreasonable. Together they are a
platform team's entire year.

[CatchX](https://github.com/erdincka/catchx) is a working demonstration of the
other option. It is a fraud detection pipeline — transactions arrive on a
stream, customers arrive as a CSV, both move through bronze, silver and gold
into a shareable data product with suspected fraud flagged. That part is an
ordinary data engineering demo. The part worth an infrastructure architect's
time is that the six steps below touch **five different data engines**, and the
number of services I had to deploy to support them is zero. Streams, document
store, NFS and object store are all fabric packages on the cluster I already
had. There is no broker, no ZooKeeper, no catalogue service, no metastore
database and no time-series store anywhere in this demo.

There is almost no fraud detection in it, which is the point.

---

## The shape of the thing

Six steps, and every one of them writes to the same cluster.

![The Fraud and Risk pipeline: live record counts read from the cluster at every tier](docs/images/screenshot-pipeline.png)

| Step | Engine | Lands in |
|---|---|---|
| Generate | POSIX file I/O over NFS | the global namespace |
| Publish | Kafka producer API | a fabric stream |
| Ingest | OJAI + Apache Iceberg | bronze |
| Refine | OJAI | silver |
| Consolidate | Delta Lake | gold |
| Detect | Delta Lake merge | gold |

Five engines. One `/mapr` mount. No Kafka cluster, no catalogue service, no
metastore database, no separate object store to stand up.

A full clean run — reset, provision, and all six steps against a single-node
cluster — takes **1 minute 53 seconds**. Almost all of it is per-document
DocumentDB writes, which is a property of the demo's chattiness rather than the
platform's.

## What "one namespace" actually means

This is the claim that sounds like marketing until you run `ls` against it.

![Generating source data, then browsing the demo directory over NFS](docs/videos/catchx-01-global-namespace.gif)

The app generates its CSVs by opening a POSIX path and writing to it. No upload
step, no staging bucket, no client library. Then the same directory, listed over
NFS, shows this:

```
drwxr-xr-x 2 mapr mapr     1 bronze
lr-------- 1 mapr mapr     3 changelog -> mapr::table::3102.97.131518
-rw-r--r-- 1 root root 55713 customers.csv
drwxr-xr-x 2 mapr mapr     0 gold
-rw-r--r-- 1 root root 20480 iceberg.db
lr-------- 1 mapr mapr     3 incoming -> mapr::table::3102.94.131512
drwxr-xr-x 2 mapr mapr     0 silver
-rw-r--r-- 1 root root 18339 transactions.csv
```

A CSV, a SQLite file, three volumes and two streams, in one directory listing.
The streams are not in a broker somewhere with their own retention config and
their own ACL model — they are entries in the filesystem, and they are governed
by the same volume that governs the CSV next to them.

The Iceberg catalogue is the sharpest example. Iceberg normally needs a
catalogue service to answer "where is table X". Here it is `iceberg.db`, a
SQLite file, sitting in the namespace. Every client that can reach the mount can
reach the catalogue, which means there is no catalogue service to deploy, secure,
back up or scale. That is not the right answer for a thousand-user estate. It is
emphatically the right answer for a demo, and the fact that it is *possible* is
the platform property worth noticing: the namespace is the coordination point,
so a lot of coordination infrastructure stops being necessary.

## Standard clients, not adapters

The demo's most persuasive screen is the least dramatic one. Every step has a
`</>` button that shows the function that ran, then follows the call chain down
to whatever actually talked to the cluster.

![Opening the code viewer and following the call chain to the Kafka and OJAI clients](docs/videos/catchx-06-code-viewer.gif)

Publishing to a fabric stream is this:

```python
from confluent_kafka import Producer, KafkaException

producer = Producer({"streams.producer.default.stream": stream})
producer.produce(topic, message.encode("utf-8"))
```

That is the stock `confluent_kafka` library. Not a fork, not a shim. The entire
fabric-specific part is one configuration key naming a path in the namespace
instead of a bootstrap server list. Everything downstream of that — consumer
groups, offsets, `enable.auto.commit`, the lot — behaves the way the Kafka
documentation says it does.

I care about this more than any throughput number, because it decides what
happens when the demo ends. Standard protocols mean the migration question is
"change a connection string", and the skills question is "your team already knows
this". A platform that requires a bespoke client is a platform you have to staff
for.

## Two table formats in one tier, because the tier does not care

Bronze holds transactions in DocumentDB and customers in Iceberg, at the same
time, in the same volume.

![Ingesting into DocumentDB and Iceberg, then reading bronze customers back](docs/videos/catchx-03-bronze-documentdb-iceberg.gif)

That is not cleverness for its own sake. Streaming JSON documents and batch
columnar loads have genuinely different access patterns, and a tier that forces
one format on both is making you choose against the workload. Here the choice is
per-table, and the tier boundary stays where it belongs — a statement about data
maturity, not about storage technology.

Gold then uses Delta Lake, which any Delta-aware engine can read straight from
the namespace. Three table formats in one pipeline, none of which needed
anything installed.

## Where the data product becomes shareable

The tier boundary earns its keep in the silver step, and this is the demo's best
moment in front of a governance-minded audience.

![Refining to silver, showing masked birthdate and location alongside added subdivision codes](docs/videos/catchx-04-silver-enrich-mask.gif)

The same customer records that were fully visible in bronze — names, dates of
birth, locations — come back from silver with `birthdate` and `current_location`
reading **masked**, and with ISO 3166-2 subdivision codes added that were not
there before. Enrichment and redaction in the same pass, because they are the
same decision: what does the next consumer need, and what should it never see?

Gold goes further and drops the direct identifiers entirely, so the flagged-fraud
table an analyst consumes carries amounts, categories and timestamps but no
account numbers.

![Consolidating to Delta Lake and flagging suspected fraud](docs/videos/catchx-05-gold-delta-fraud.gif)

## Telemetry without a telemetry stack

The record counts in the diagram are not the app remembering what you clicked.
Every one of them is read from the cluster: DocumentDB counts by projecting
`_id`, Iceberg and Delta by reading the table, stream depth and consumer lag from
`stream/topic/info` and `stream/cursor/list` on the cluster's REST API.

![Publishing transactions to a fabric stream and watching the count arrive](docs/videos/catchx-02-streams.gif)

The practical consequence is that **step completion is derived from cluster
state, not from session state**. Reload the page and progress survives. Run half
the demo from `curl` and the UI shows it. Hand the laptop to a colleague
mid-presentation and it is still correct. I did not click a single button in the
UI before the screenshot above showed all six steps complete — an API run had
already done it, and the app simply reported what was true.

There is no Grafana here, no OpenTSDB, no metrics agent. For a demo that needs
live numbers, the cluster's own API was enough, and every service I did not add
is a service that cannot fail on stage.

## "Provisioned" and "working" are different questions

Now the part that actually cost me a day, because it is the transferable lesson
and because a post that only lists capabilities is an advert.

The demo has a reset button. It deleted the four volumes and recreated them, so
you could run the whole thing again from a clean cluster. It had presumably
worked at some point. When I ran it against this cluster, every DocumentDB write
after a reset failed:

```
insertOrReplace() failed with err code = 19
```

Nineteen is `ENODEV`. The code already had a retry loop for this, because a
freshly provisioned volume does take a few seconds to serve — so the first
theory was that the backoff was too short. It was not. The writes still failed
an hour later.

Second theory: the OJAI client caches its connection for the life of the
process, and the connection was stale. I restarted the application. Same error.

Third: check whether the table was actually broken, from somewhere else
entirely. It was not — `dbshell`, a fresh process on the cluster node, inserted
a document into the same table without complaint.

Which located it. The error was not in my client and not in the table. It was in
the **Data Access Gateway**, the service that speaks OJAI on port 5678:

```
Inode - syncPut() on '/catchx-demo/bronze/transactions'
        failed with error: No such device (19).
```

The gateway holds its own MapR client, and that client had resolved the path
while the *old* volume existed. Delete the volume, create a new one at the same
mount point, and the gateway keeps serving the handle it already had. Nothing
client-side can fix it; only restarting the gateway clears it.

Then the decisive experiment, which took two minutes and should have been the
first thing I did: delete and recreate a *table* at the same path, leaving the
volume alone. That works perfectly. The blast radius is volumes, not paths.

So the fix was not a longer retry or a reconnect. It was to stop deleting
volumes. The reset now removes the streams, tables and files and leaves the four
volumes in place, which is both correct and about twice as fast.

Three things from that are worth carrying elsewhere:

**A distributed storage layer is still made of services, and services have
caches.** "One namespace" is a real and useful abstraction, and it has seams. The
seam here was a component I had not thought about all day, holding state I did
not know it held.

**Retry logic encodes a theory about the failure.** The existing backoff assumed
"transient, will settle". That assumption was invisible until a failure that
looked identical but was permanent walked into it. A retry loop that cannot
distinguish the two will happily hide the second for fifteen seconds and then
report the wrong thing.

**Ask a different client.** The single most useful step in the whole
investigation was running `dbshell` on the cluster. Two minutes, and it cut the
search space in half by proving the data layer was fine. I had spent
considerably longer than two minutes theorising before I did it.

While I was in there, a related one: the same cached-connection design meant that
after *any* gateway restart, the application never recovered until it was
restarted too. That is now fixed as well — the connection is dropped and rebuilt
when a channel error is seen — and it is the more common failure in real life,
because gateways get restarted for ordinary reasons.

And a cluster-side finding for anyone benchmarking this: check whether your Data
Access Gateway is still logging at `debug`. The default `log4j2.xml` on this
cluster writes every gRPC frame and every document payload to disk. It dominated
my write latency, and no amount of application tuning would have touched it.

## What it does not show

The honest limits, because the demo is small on purpose.

| | |
|---|---|
| **Scale** | A single-node cluster and a few hundred rows. Nothing here says anything about how the fabric behaves at a thousand nodes, and I would not extrapolate. |
| **Governance** | Masking is a function in the refine step, not a policy engine. There is no lineage, no classification, no audit of who read what. |
| **The fraud model** | Deliberately trivial arithmetic. If you want a real one, that is a different post and mostly not an infrastructure problem. |
| **Multi-tenancy** | One admin account does everything. A real deployment separates the pipeline's identity from the cluster admin's. |
| **Failure behaviour** | I tested the reset path hard because it broke. I did not test node loss, network partition or a full disk. |

The one operational claim I will make firmly is the narrow one I actually
verified: reset the demo and run it again, repeatedly, and it completes with
correct counts at every tier without anything on the cluster being restarted.
That was not true when I started.

## Worth your time?

If you are choosing a data platform, the interesting question is not whether it
can do streaming, documents, tables and files. Most can, eventually, with enough
components. It is **how many of those components you are agreeing to operate**,
and whether the code you write against them is code you could write against
anything else.

CatchX answers the second question well — stock Kafka, stock OJAI, stock Iceberg
and Delta, with the fabric appearing as a path rather than as an SDK. It answers
the first question with a number: five engines, one mount, nothing extra
installed.

Then it spends a day teaching you that the abstraction has seams, and where one
of them is. Which is the more useful lesson of the two.

---

*The code, the verification steps and the failure modes are at
[github.com/erdincka/catchx](https://github.com/erdincka/catchx). It runs against
any Data Fabric 7.x or later cluster; use a lab one, since it creates and deletes
its own artefacts.*
