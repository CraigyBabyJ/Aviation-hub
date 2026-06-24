"use client";

import { RefreshCw, AlertTriangle } from "lucide-react";
import { cn } from "@/lib/utils";

interface LiveStatusProps {
  loading: boolean;
  error: string | null;
  lastUpdated: Date | null;
  onRefresh?: () => void;
  className?: string;
}

export function LiveStatus({ loading, error, lastUpdated, onRefresh, className }: LiveStatusProps) {
  const timeStr = lastUpdated
    ? lastUpdated.toLocaleTimeString("en-GB", { hour: "2-digit", minute: "2-digit", second: "2-digit", hour12: false }) + "z"
    : null;

  return (
    <div className={cn("flex items-center gap-2", className)}>
      {error ? (
        <div className="flex items-center gap-1.5 text-red-500/80">
          <AlertTriangle size={10} />
          <span className="text-[9px] tracking-wide">Hub unreachable</span>
        </div>
      ) : (
        <>
          {timeStr && (
            <span className="text-[9px] text-zinc-700 tabular-nums tracking-wide">
              {timeStr}
            </span>
          )}
          <button
            onClick={onRefresh}
            disabled={loading}
            className="text-zinc-700 hover:text-zinc-400 transition-colors disabled:opacity-40"
            title="Refresh"
          >
            <RefreshCw size={10} className={cn(loading && "animate-spin")} />
          </button>
        </>
      )}
    </div>
  );
}

/** Skeleton shimmer for loading states */
export function Skeleton({ className }: { className?: string }) {
  return (
    <div className={cn("animate-pulse rounded bg-white/[0.04]", className)} />
  );
}
