import type { NextConfig } from "next";

const isDev = process.env.NODE_ENV !== "production";
const defaultDevOrigin = "nexmesh.demo.lon-pcai.twlon.com";
const envOrigins = process.env.ALLOWED_DEV_ORIGINS?.split(",").map((origin) => origin.trim()).filter(Boolean);
const allowedDevOrigins = envOrigins && envOrigins.length > 0 ? envOrigins : [defaultDevOrigin];

const nextConfig: NextConfig = {
  // Dev-only: accept HMR/asset requests from allowed origins when proxied
  // through ingress hostnames (k8s, ngrok, lan IPs, etc.).
  // Production builds ignore this field — see Next.js allowedDevOrigins docs.
  ...(isDev ? { allowedDevOrigins } : {}),

  async rewrites() {
    const backendUrl = process.env.BACKEND_URL ?? "http://localhost:8000";
    return [
      {
        source: "/api/:path*",
        destination: `${backendUrl}/api/:path*`,
      },
    ];
  },
};

export default nextConfig;
