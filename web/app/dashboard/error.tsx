"use client";

import { useEffect } from "react";
import { AlertTriangle, RefreshCw } from "lucide-react";

export default function DashboardError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    console.error("[AvHub dashboard error]", error);
  }, [error]);

  return (
    <div className="max-w-[1600px] mx-auto px-6 py-16 flex items-center justify-center min-h-[60vh]">
      <div className="hub-panel max-w-lg w-full p-8 space-y-5">
        <div className="flex items-center gap-3">
          <AlertTriangle size={16} className="text-red-500 flex-shrink-0" />
          <h2 className="text-sm font-bold tracking-[0.2em] uppercase text-white">
            Dashboard Error
          </h2>
        </div>

        <div className="p-3 rounded-lg bg-red-950/10 border border-red-600/15">
          <p className="text-[11px] text-zinc-500 font-mono break-all">
            {error?.message ?? "An unexpected error occurred in the dashboard."}
          </p>
        </div>

        <p className="text-[10px] text-zinc-700 tracking-wide">
          The hub service may be unreachable. Ensure the Aviation Hub widget server
          is running on port 4010.
        </p>

        <button
          onClick={reset}
          className="flex items-center gap-2 px-4 py-2 rounded-lg border border-white/[0.08] bg-white/[0.03] hover:bg-white/[0.06] text-[11px] tracking-widest uppercase text-zinc-400 hover:text-white transition-colors"
        >
          <RefreshCw size={11} />
          Retry
        </button>
      </div>
    </div>
  );
}
