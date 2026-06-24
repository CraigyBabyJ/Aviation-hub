"use client";

import { motion } from "framer-motion";
import { cn } from "@/lib/utils";

interface HubPanelProps {
  children: React.ReactNode;
  className?: string;
  cornerAccent?: boolean;
  delay?: number;
  noPadding?: boolean;
}

export function HubPanel({
  children,
  className,
  cornerAccent = false,
  delay = 0,
  noPadding = false,
}: HubPanelProps) {
  return (
    <motion.div
      initial={{ opacity: 0, y: 12 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.5, ease: "easeOut", delay }}
      className={cn(
        "hub-panel",
        cornerAccent && "corner-accent",
        !noPadding && "p-5",
        className
      )}
    >
      {children}
    </motion.div>
  );
}
