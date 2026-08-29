/**
 * The demo pipeline, defined as data.
 *
 * Completion is derived from live cluster metrics rather than from which
 * buttons were clicked, so progress is correct after a page reload, after a
 * backend restart, and when someone else already ran part of the demo.
 */

export type Metrics = Record<string, number | boolean>;

export type StepId =
  | "generate" | "publish" | "ingest" | "refine" | "consolidate" | "detect";

export interface StepAction {
  id: string;
  label: string;
  /** API path; POSTed with no body. */
  path: string;
  /** Reads better in the run log than the raw path. */
  describe: string;
  /** Registry key for the source-code viewer. */
  code?: string;
  /** Pull a headline number out of the response for the run log. */
  summarise?: (data: Record<string, unknown>) => string;
}

export interface PipelineStep {
  id: StepId;
  n: number;
  title: string;
  /** One line, shown under the title. */
  blurb: string;
  /** The Data Fabric capability this step demonstrates. */
  capability: string;
  /** Two or three sentences for the detail panel. */
  detail: string;
  actions: StepAction[];
  /** Tier this step writes into, for diagram highlighting. */
  tier?: "source" | "stream" | "bronze" | "silver" | "gold";
  isDone: (m: Metrics) => boolean;
  /** Preconditions. Returns null when ready, else why not. */
  blockedBy: (m: Metrics) => string | null;
}

const n = (m: Metrics, k: string): number => {
  const v = m[k];
  return typeof v === "number" ? v : 0;
};
const yes = (m: Metrics, k: string): boolean => Boolean(m[k]);

const count = (data: Record<string, unknown>, key = "count"): string => {
  const v = data[key];
  return typeof v === "number" ? v.toLocaleString() : "—";
};

export const PIPELINE: PipelineStep[] = [
  {
    id: "generate",
    n: 1,
    title: "Generate source data",
    blurb: "Write customer and transaction CSVs into the global namespace",
    capability: "Global Namespace (NFS)",
    detail:
      "Synthetic customers and transactions are written straight to /mapr over NFS. " +
      "No upload step and no staging area — the container writes to a POSIX path and " +
      "every node in the cluster sees the file immediately.",
    tier: "source",
    actions: [
      {
        id: "customers",
        label: "Customers",
        path: "/api/data/customers/create",
        describe: "Generated customers.csv",
        code: "create_customers",
        // Generation appends, so report both the new rows and the running total.
        summarise: (d) =>
          typeof d.total === "number" && d.total !== d.count
            ? `${count(d)} new · ${count(d, "total")} total`
            : `${count(d)} customers`,
      },
      {
        id: "transactions",
        label: "Transactions",
        path: "/api/data/transactions/create",
        describe: "Generated transactions.csv",
        code: "create_transactions",
        summarise: (d) => `${count(d)} transactions`,
      },
    ],
    isDone: (m) => yes(m, "source_customers") && yes(m, "source_transactions"),
    blockedBy: () => null,
  },
  {
    id: "publish",
    n: 2,
    title: "Publish to the stream",
    blurb: "Push transactions onto a Kafka-compatible fabric stream",
    capability: "MapR Streams",
    detail:
      "Transactions are produced to a Data Fabric stream using the standard Kafka " +
      "producer API — the same client code you would write for Kafka, with the stream " +
      "living inside the fabric rather than a separate cluster.",
    tier: "stream",
    actions: [
      {
        id: "publish",
        label: "Publish transactions",
        path: "/api/data/transactions/publish",
        describe: "Published to /catchx-demo/incoming",
        code: "publish_transactions",
        summarise: (d) => `${count(d)} messages published`,
      },
    ],
    isDone: (m) => n(m, "transactions_ingested") > 0,
    blockedBy: (m) =>
      yes(m, "source_transactions") ? null : "Generate transactions first.",
  },
  {
    id: "ingest",
    n: 3,
    title: "Ingest to bronze",
    blurb: "Stream into DocumentDB, batch-load the CSV into Iceberg",
    capability: "DocumentDB (OJAI) + Apache Iceberg",
    detail:
      "Two ingestion paths land in the same tier: transactions are consumed from the " +
      "stream into a DocumentDB JSON table, while customers are batch-loaded into an " +
      "Iceberg table. Bronze keeps the raw shape of both.",
    tier: "bronze",
    actions: [
      {
        id: "txn",
        label: "Transactions → DocumentDB",
        path: "/api/data/ingest/transactions",
        describe: "Consumed stream into bronze",
        code: "ingest_transactions",
        summarise: (d) => `${count(d)} records`,
      },
      {
        id: "cust",
        label: "Customers → Iceberg",
        path: "/api/data/ingest/customers",
        describe: "Batch-loaded customers into Iceberg",
        code: "ingest_customers_iceberg",
        summarise: (d) => `${count(d)} records`,
      },
    ],
    isDone: (m) => n(m, "bronze_transactions") > 0 && n(m, "bronze_customers") > 0,
    blockedBy: (m) => {
      if (n(m, "transactions_ingested") === 0) return "Publish transactions to the stream first.";
      if (!yes(m, "source_customers")) return "Generate customers first.";
      return null;
    },
  },
  {
    id: "refine",
    n: 4,
    title: "Refine to silver",
    blurb: "Enrich, categorise and mask personal data",
    capability: "DocumentDB + in-fabric compute",
    detail:
      "Silver is the curated tier. Customers gain country and subdivision codes; " +
      "birthdate and location are masked; transactions are categorised. Building " +
      "profiles here also gives each customer a running risk score.",
    tier: "silver",
    actions: [
      {
        id: "txn",
        label: "Transactions",
        path: "/api/data/refine/transactions",
        describe: "Refined transactions into silver",
        code: "refine_transactions",
        summarise: (d) =>
          typeof d.profiles === "number"
            ? `${count(d)} records · ${count(d, "profiles")} profiles`
            : `${count(d)} records`,
      },
      {
        id: "cust",
        label: "Customers",
        path: "/api/data/refine/customers",
        describe: "Enriched and masked customers into silver",
        code: "refine_customers",
        summarise: (d) => `${count(d)} records`,
      },
    ],
    isDone: (m) =>
      n(m, "silver_transactions") > 0 &&
      n(m, "silver_customers") > 0 &&
      n(m, "silver_profiles") > 0,
    blockedBy: (m) =>
      n(m, "bronze_transactions") > 0 && n(m, "bronze_customers") > 0
        ? null
        : "Ingest into the bronze tier first.",
  },
  {
    id: "consolidate",
    n: 5,
    title: "Consolidate to gold",
    blurb: "Merge into a shareable Delta Lake data product",
    capability: "Delta Lake",
    detail:
      "Gold is the shareable data product. Customers, profiles and transactions are " +
      "merged, direct identifiers are dropped, and the result is written as Delta " +
      "Lake tables that any Delta-aware engine can read from the global namespace.",
    tier: "gold",
    actions: [
      {
        id: "consolidate",
        label: "Build gold tier",
        path: "/api/data/consolidate",
        describe: "Merged silver into gold Delta tables",
        code: "create_golden",
        summarise: (d) =>
          `${count(d, "customers")} customers · ${count(d, "transactions")} transactions`,
      },
    ],
    isDone: (m) => n(m, "gold_customers") > 0 && n(m, "gold_transactions") > 0,
    blockedBy: (m) =>
      n(m, "silver_transactions") > 0 && n(m, "silver_customers") > 0
        ? null
        : "Refine into the silver tier first.",
  },
  {
    id: "detect",
    n: 6,
    title: "Detect fraud",
    blurb: "Score transactions and flag suspected fraud into gold",
    capability: "Delta Lake merge",
    detail:
      "Every bronze transaction is scored; anything above the threshold is written " +
      "to the gold transactions table with a fraud flag, in a single Delta merge. " +
      "This is the output an analyst or downstream model would consume.",
    tier: "gold",
    actions: [
      {
        id: "fraud",
        label: "Run detection",
        path: "/api/data/fraud",
        describe: "Scored transactions for fraud",
        code: "fraud_detection",
        summarise: (d) => {
          const f = typeof d.fraud_count === "number" ? d.fraud_count : 0;
          const s = typeof d.scanned === "number" ? d.scanned : 0;
          return `${f.toLocaleString()} flagged of ${s.toLocaleString()} scanned`;
        },
      },
    ],
    isDone: (m) => n(m, "gold_fraud") > 0,
    blockedBy: (m) =>
      n(m, "bronze_transactions") > 0 ? null : "Ingest into the bronze tier first.",
  },
];

export type StepState = "done" | "ready" | "blocked";

export function stepState(step: PipelineStep, m: Metrics): StepState {
  if (step.isDone(m)) return "done";
  return step.blockedBy(m) === null ? "ready" : "blocked";
}

/** Index of the step the presenter should run next, or -1 when all are done. */
export function nextStepIndex(m: Metrics): number {
  return PIPELINE.findIndex((s) => !s.isDone(m));
}
