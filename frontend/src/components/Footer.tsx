"use client";

import { useCluster } from "@/contexts/ClusterContext";
import { VOLUME_BRONZE, VOLUME_SILVER, VOLUME_GOLD } from "@/lib/constants";
import { RiGithubLine, RiFolderOpenLine } from "@remixicon/react";

interface FooterProps {
  onVolumeExplore: (label: string, path: string) => void;
}

export default function Footer({ onVolumeExplore }: FooterProps) {
  const { clusterInfo } = useCluster();
  const name = clusterInfo?.name ?? "";
  const base = `/mapr/${name}/demovol`;

  const volumes = [
    { label: "GNS",    path: "/mapr",          always: true },
    { label: "Domain", path: base,              always: false },
    { label: "Bronze", path: `${base}/${VOLUME_BRONZE}`, always: false },
    { label: "Silver", path: `${base}/${VOLUME_SILVER}`, always: false },
    { label: "Gold",   path: `${base}/${VOLUME_GOLD}`,   always: false },
  ];

  return (
    <footer
      className="flex items-center py-1 px-4 gap-3 shrink-0"
      style={{
        background: "rgba(10, 10, 10, 0.98)",
        borderTop: "1px solid rgba(255,255,255,0.06)",
        height: 32,
      }}
    >
      <div className="flex items-center gap-1">
        <RiFolderOpenLine size={12} className="text-neutrals-dark" />
        <span className="font-sans text-[10px] text-neutrals-dark uppercase tracking-[0.15em] mr-1">
          Volumes
        </span>
        {volumes
          .filter((v) => v.always || name)
          .map(({ label, path }) => (
            <button
              key={label}
              onClick={() => onVolumeExplore(label, path)}
              className="font-sans text-[10px] px-2 py-0.5 rounded text-neutrals-medium hover:text-brand-vivid hover:bg-white/5 transition-colors duration-200 uppercase tracking-wide"
            >
              {label}
            </button>
          ))}
      </div>

      <div className="flex-1" />

      {clusterInfo && (
        <span className="font-sans font-light text-[10px] text-neutrals-dark uppercase tracking-[0.12em]">
          {clusterInfo.name}
        </span>
      )}

      <a
        href="https://github.com/erdincka/nexmesh"
        target="_blank"
        rel="noreferrer"
        className="text-neutrals-dark hover:text-brand-vivid transition-colors duration-200"
        title="GitHub"
      >
        <RiGithubLine size={14} />
      </a>
    </footer>
  );
}
