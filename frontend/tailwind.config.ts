import type { Config } from "tailwindcss";

const config: Config = {
  content: ["./src/**/*.{js,ts,jsx,tsx,mdx}"],
  presets: [require('./nexus-theme-preset')],
  plugins: [],
};

export default config;
