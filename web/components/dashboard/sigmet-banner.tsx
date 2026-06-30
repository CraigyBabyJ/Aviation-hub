"use client";

import { useState } from "react";
import { AlertTriangle, ChevronDown, ChevronUp, X } from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";
import { useHub } from "@/lib/hooks/use-hub";
import type { SigmetsResponse, Sigmet } from "@/lib/api-types";
import { cn } from "@/lib/utils";

// High-severity hazards use accent colour; others use static zinc shades
const HIGH_SEVERITY = new Set(["TS", "VA", "TC", "RDOACT_CLD"]);
const HAZARD_LABELS: Record<string, { label: string; color?: string }> = {
  TS:         { label: "Thunderstorm" },
  SEV_TURB:   { label: "Severe Turbulence", color: "text-zinc-300" },
  SEV_ICE:    { label: "Severe Icing",      color: "text-zinc-300" },
  ICE:        { label: "Icing",             color: "text-zinc-400" },
  TURB:       { label: "Turbulence",        color: "text-zinc-400" },
  VA:         { label: "Volcanic Ash" },
  TC:         { label: "Tropical Cyclone" },
  RDOACT_CLD: { label: "Radioactive Cloud" },
};

function formatSigmetTime(iso: string) {
  return new Date(iso).toLocaleTimeString("en-GB", {
    hour: "2-digit", minute: "2-digit", timeZone: "UTC", hour12: false,
  }) + "z";
}

function SigmetChip({ sig }: { sig: Sigmet }) {
  const hazard        = HAZARD_LABELS[sig.hazard] ?? { label: sig.hazard, color: "text-zinc-400" };
  const isHighSeverity = HIGH_SEVERITY.has(sig.hazard);

  return (
    <div className={cn(
      "flex-shrink-0 flex items-center gap-2 px-3 py-1.5 rounded border text-[10px]",
      isHighSeverity
        ? "border-[rgba(var(--accent),0.30)] bg-[rgba(var(--accent),0.08)]"
        : "border-white/[0.06] bg-white/[0.02]"
    )}>
      <span
        className={cn("font-bold tracking-wider", !isHighSeverity && hazard.color)}
        style={isHighSeverity ? { color: "var(--accent-text)" } : {}}
      >{sig.hazard}</span>
      <span className="text-zinc-600">·</span>
      <span className="text-zinc-500">{sig.fir}</span>
      <span className="text-zinc-700 text-[9px] tabular-nums hidden sm:block">
        {formatSigmetTime(sig.valid_from)}–{formatSigmetTime(sig.valid_to)}
      </span>
    </div>
  );
}

export function SigmetBanner() {
  const { data, loading } = useHub<SigmetsResponse>("sigmets", {}, { interval: 300_000 });
  const [expanded, setExpanded] = useState(false);
  const [dismissed, setDismissed] = useState(false);

  const sigmets = data?.sigmets ?? [];
  const tsCount = sigmets.filter((s) => s.hazard === "TS").length;
  const sevCount = sigmets.filter((s) => ["VA", "TC", "RDOACT_CLD"].includes(s.hazard)).length;

  if (loading || dismissed || sigmets.length === 0) return null;

  return (
    <motion.div
      initial={{ opacity: 0, y: -8 }}
      animate={{ opacity: 1, y: 0 }}
      exit={{ opacity: 0, y: -8 }}
      transition={{ duration: 0.3 }}
      className="mb-4 rounded-xl overflow-hidden transition-[border-color,background-color] duration-500"
      style={{ border: "1px solid rgba(var(--accent),0.20)", backgroundColor: "rgba(var(--accent),0.06)" }}
    >
      {/* Header bar */}
      <div className="flex items-center justify-between gap-3 px-4 py-2.5">
        <div className="flex items-center gap-3">
          <AlertTriangle size={12} className="flex-shrink-0 transition-[color] duration-500" style={{ color: "var(--accent-text)" }} />
          <span className="text-[10px] font-bold tracking-[0.18em] uppercase transition-[color] duration-500" style={{ color: "var(--accent-text)" }}>
            Active SIGMETs
          </span>
          <span className="text-[9px] text-zinc-600 tracking-wider">
            {sigmets.length} active
            {tsCount > 0 && ` · ${tsCount} TS`}
            {sevCount > 0 && ` · ${sevCount} severe`}
          </span>
        </div>
        <div className="flex items-center gap-2">
          <button
            onClick={() => setExpanded(!expanded)}
            className="flex items-center gap-1 text-zinc-600 hover:text-zinc-400 transition-colors text-[10px] tracking-wider"
          >
            {expanded ? <ChevronUp size={12} /> : <ChevronDown size={12} />}
            <span className="hidden sm:block">{expanded ? "Collapse" : "Expand"}</span>
          </button>
          <button onClick={() => setDismissed(true)} className="text-zinc-700 hover:text-zinc-500 transition-colors ml-1">
            <X size={12} />
          </button>
        </div>
      </div>

      {/* Scrolling chips */}
      <div className="flex gap-2 overflow-x-auto px-4 pb-2.5 scrollbar-none">
        {sigmets.slice(0, expanded ? 999 : 8).map((sig) => (
          <SigmetChip key={sig.id} sig={sig} />
        ))}
      </div>

      {/* Expanded full list */}
      <AnimatePresence>
        {expanded && (
          <motion.div
            initial={{ height: 0, opacity: 0 }}
            animate={{ height: "auto", opacity: 1 }}
            exit={{ height: 0, opacity: 0 }}
            transition={{ duration: 0.2 }}
            className="overflow-hidden"
          >
            <div className="px-4 pb-3 border-t border-white/[0.04] pt-3 max-h-48 overflow-y-auto">
              {sigmets.map((sig) => (
                <div key={sig.id} className="hub-row py-2">
                  <div className="flex items-start gap-3">
                    <span
                      className={cn("text-[9px] font-bold w-16 flex-shrink-0 tracking-wider", !HIGH_SEVERITY.has(sig.hazard) && (HAZARD_LABELS[sig.hazard]?.color ?? "text-zinc-400"))}
                      style={HIGH_SEVERITY.has(sig.hazard) ? { color: "var(--accent-text)" } : {}}
                    >
                      {sig.hazard}
                      {sig.qualifier && <span className="text-zinc-700"> {sig.qualifier}</span>}
                    </span>
                    <div className="flex-1 min-w-0">
                      <p className="text-[10px] text-zinc-400">{sig.fir_name}</p>
                      <p className="text-[9px] text-zinc-700 tabular-nums">
                        {formatSigmetTime(sig.valid_from)} → {formatSigmetTime(sig.valid_to)}
                        {sig.top_ft && ` · FL${Math.round(sig.top_ft / 100)}`}
                      </p>
                    </div>
                  </div>
                </div>
              ))}
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </motion.div>
  );
}
