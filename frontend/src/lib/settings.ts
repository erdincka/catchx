export interface Endpoints {
  s3_endpoint: string;
  polaris_url: string;
  livy_url: string;
  grafana_url: string;
  opentsdb_url: string;
  fluentd_host: string;
  mcp_server_url: string;
}

export interface Credentials {
  cluster_user: string;
  cluster_pass: string;
  // S3 keys are auto-generated — not entered by the user
  s3_access_key: string;
  s3_secret: string;
  polaris_credential: string;
}

export interface DemoTargets {
  base_volume: string;
  s3_bucket: string;
  stream_path: string;
  polaris_warehouse: string;
}

export interface FeatureFlags {
  catalog: "polaris";
  verify_ssl: boolean;
}

export interface Settings {
  cluster_host: string;
  endpoints: Endpoints;
  credentials: Credentials;
  targets: DemoTargets;
  flags: FeatureFlags;
}

export type ServiceStatus = "good" | "degraded" | "failed" | "unknown";

export interface ServiceProbe {
  status: ServiceStatus;
  latency_ms: number;
  detail: string;
  url: string;
}

export type ServiceMatrix = Record<string, ServiceProbe>;

export const SERVICE_NAMES = [
  "cluster",
  "s3",
  "polaris",
  "livy",
  "grafana",
  "opentsdb",
  "fluentd",
  "mcp",
] as const;

export type ServiceName = (typeof SERVICE_NAMES)[number];

// Human-readable labels and the port/path shown as the standard address
export const SERVICE_META: Record<ServiceName, { label: string; portHint: string }> = {
  cluster:  { label: "Data Fabric Cluster",  portHint: ":8443 (REST API)" },
  s3:       { label: "S3 Object Store",      portHint: ":9000 (MinIO/ECS)" },
  polaris:  { label: "Polaris Catalog",      portHint: ":8181 (REST catalog)" },
  livy:     { label: "Livy (Spark gateway)", portHint: ":8998" },
  grafana:  { label: "Grafana",              portHint: ":3000" },
  opentsdb: { label: "OpenTSDB",             portHint: ":4242" },
  fluentd:  { label: "Fluentd forward",      portHint: ":24224 (TCP)" },
  mcp:      { label: "Data Fabric MCP",      portHint: ":5679/s3" },
};
