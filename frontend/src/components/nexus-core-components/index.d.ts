import type { CSSProperties, ReactNode } from "react";

export interface NavItem {
  id: string;
  label: string;
  subItems?: string[];
}

export interface NexusGlobalNavProps {
  navItems?: NavItem[];
  leftSlot?: ReactNode;
  rightSlot?: ReactNode;
  onItemClick?: (id: string) => void;
  onSubItemClick?: (subItem: string, parentId: string) => void;
  className?: string;
  style?: CSSProperties;
}

export interface NexusCardProps {
  children?: ReactNode;
  variant?: "editorial" | "feature" | "status" | "display";
  active?: boolean;
  glowColour?: "vivid" | "contrast" | "neutral";
  backgroundImage?: string;
  overlay?: boolean;
  overlayStyle?: "bottom-up" | "hero-left" | "site" | "none";
  width?: number | string;
  height?: number | string;
  aspectRatio?: string;
  hoverScale?: boolean;
  onClick?: (e: React.MouseEvent) => void;
  className?: string;
  style?: CSSProperties;
  id?: string;
}

export interface NexusSectionDividerProps {
  direction?: "horizontal" | "vertical";
  thickness?: number | string;
  length?: number | string;
  maxLength?: string;
  title?: string;
  className?: string;
  style?: CSSProperties;
  lineStyle?: CSSProperties;
  startColour?: string;
  endColour?: string;
}

export declare function NexusGlobalNav(props: NexusGlobalNavProps): JSX.Element;
export declare function NexusCard(props: NexusCardProps): JSX.Element;
export declare function NexusSectionDivider(props: NexusSectionDividerProps): JSX.Element;
