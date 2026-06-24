"use client";

import { useEffect } from "react";
import { AlertTriangle, RefreshCw } from "lucide-react";

export default function Error({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    console.error("[AvHub error]", error);
  }, [error]);

  return (
    <div className="min-h-screen flex items-center justify-center px-6">
      <div className="hub-panel max-w-md w-full p-8 text-center space-y-5">
        <div className="flex justify-center">
          <div className="w-12 h-12 rounded-full border border-red-600/30 bg-red-950/20 flex items-center justify-center">
            <AlertTriangle size={20} className="text-red-500" />
          </div>
        </div>

        <div>
          <h2 className="text-sm font-bold tracking-[0.2em] uppercase text-white mb-2">
            Something went wrong
          </h2>
          <p className="text-[11px] text-zinc-600 tracking-wide">
            {error?.message ?? "An unexpected error occurred."}
          </p>
          {error?.digest && (
            <p className="text-[9px] text-zinc-800 mt-2 font-mono">{error.digest}</p>
          )}
        </div>

        <button
          onClick={reset}
          className="flex items-center gap-2 mx-auto px-4 py-2 rounded-lg border border-white/[0.08] bg-white/[0.03] hover:bg-white/[0.06] text-[11px] tracking-widest uppercase text-zinc-400 hover:text-white transition-colors"
        >
          <RefreshCw size={11} />
          Try again
        </button>
      </div>
    </div>
  );
}
