"use client";

import { useState, useRef, useEffect } from "react";
import { Search, X, Wind, Eye, Gauge, Radio, Plane } from "lucide-react";
import { motion, AnimatePresence } from "framer-motion";
import { cn } from "@/lib/utils";
import type { AirportStatus } from "@/lib/api-types";

async function fetchAirportStatus(icao: string): Promise<AirportStatus | null> {
  try {
    const res = await fetch(`/api/hub/airport/status?icao=${icao.toUpperCase()}`);
    if (!res.ok) return null;
    return res.json();
  } catch {
    return null;
  }
}

function FlightCatDot({ cat }: { cat: string | null }) {
  if (!cat) return null;
  const color = cat === "IFR" ? "bg-red-500" : cat === "MVFR" ? "bg-zinc-400" : "bg-zinc-600";
  return (
    <span className={cn("inline-block w-2 h-2 rounded-full mr-1.5", color)} />
  );
}

function ControllerBadge({ callsign, facility }: { callsign: string; facility: string }) {
  const colors: Record<string, string> = {
    CTR: "bg-red-950/60 text-red-300 border-red-600/30",
    APP: "bg-zinc-800 text-white border-white/10",
    TWR: "bg-zinc-800 text-zinc-300 border-white/[0.08]",
    GND: "bg-zinc-900 text-zinc-400 border-white/[0.06]",
    DEL: "bg-zinc-900 text-zinc-500 border-white/[0.04]",
  };
  return (
    <span className={cn("text-[8px] font-bold tracking-widest px-1.5 py-0.5 rounded border", colors[facility] ?? colors.GND)}>
      {callsign.split("_").slice(-1)[0]}
    </span>
  );
}

export function AirportSearch() {
  const [query, setQuery] = useState("");
  const [result, setResult] = useState<AirportStatus | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [open, setOpen] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);

  async function handleSearch(icao: string) {
    const code = icao.trim().toUpperCase();
    if (code.length < 3) return;
    setLoading(true);
    setError(null);
    const data = await fetchAirportStatus(code);
    setLoading(false);
    if (!data) {
      setError(`No data found for ${code}`);
      setResult(null);
    } else {
      setResult(data);
      setError(null);
    }
  }

  function handleKey(e: React.KeyboardEvent) {
    if (e.key === "Enter") handleSearch(query);
    if (e.key === "Escape") { setOpen(false); setQuery(""); setResult(null); }
  }

  // Global keyboard shortcut: / to open
  useEffect(() => {
    const handler = (e: KeyboardEvent) => {
      if (e.key === "/" && !open && !(e.target instanceof HTMLInputElement)) {
        e.preventDefault();
        setOpen(true);
        setTimeout(() => inputRef.current?.focus(), 50);
      }
    };
    window.addEventListener("keydown", handler);
    return () => window.removeEventListener("keydown", handler);
  }, [open]);

  return (
    <>
      {/* Search trigger button */}
      <button
        onClick={() => { setOpen(true); setTimeout(() => inputRef.current?.focus(), 50); }}
        className="flex items-center gap-2 px-3 py-1.5 rounded-lg border border-white/[0.07] bg-white/[0.02] hover:border-white/[0.12] hover:bg-white/[0.04] transition-colors group"
      >
        <Search size={12} className="text-zinc-600 group-hover:text-zinc-400 transition-colors" />
        <span className="text-[10px] text-zinc-600 group-hover:text-zinc-400 transition-colors tracking-wider hidden sm:block">
          Airport lookup
        </span>
        <kbd className="hidden sm:block text-[8px] text-zinc-700 bg-white/[0.04] px-1 py-0.5 rounded border border-white/[0.06]">/</kbd>
      </button>

      {/* Modal overlay */}
      <AnimatePresence>
        {open && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 0.15 }}
            className="fixed inset-0 z-[200] flex items-start justify-center pt-[15vh] px-4"
            onClick={(e) => { if (e.target === e.currentTarget) { setOpen(false); setResult(null); setQuery(""); } }}
          >
            {/* Backdrop */}
            <div className="absolute inset-0 bg-black/60 backdrop-blur-sm" />

            {/* Panel */}
            <motion.div
              initial={{ opacity: 0, y: -12, scale: 0.98 }}
              animate={{ opacity: 1, y: 0, scale: 1 }}
              exit={{ opacity: 0, y: -8, scale: 0.98 }}
              transition={{ duration: 0.2, ease: "easeOut" }}
              className="relative w-full max-w-lg hub-panel !p-0 overflow-hidden"
            >
              {/* Search input */}
              <div className="flex items-center gap-3 px-4 py-3 border-b border-white/[0.06]">
                <Search size={14} className={cn("flex-shrink-0", loading ? "text-zinc-500 animate-pulse" : "text-zinc-600")} />
                <input
                  ref={inputRef}
                  value={query}
                  onChange={(e) => setQuery(e.target.value.toUpperCase().slice(0, 4))}
                  onKeyDown={handleKey}
                  placeholder="ICAO code (e.g. EGLL) — press Enter"
                  className="flex-1 bg-transparent text-sm text-white placeholder-zinc-700 outline-none tracking-widest"
                  maxLength={4}
                  autoComplete="off"
                />
                {query && (
                  <button onClick={() => { setQuery(""); setResult(null); setError(null); }} className="text-zinc-700 hover:text-zinc-400">
                    <X size={12} />
                  </button>
                )}
                <button onClick={() => handleSearch(query)} className="text-[9px] uppercase tracking-widest text-zinc-600 hover:text-zinc-400 px-2 py-1 border border-white/[0.06] rounded transition-colors">
                  Lookup
                </button>
                <button
                  onClick={() => { setOpen(false); setResult(null); setQuery(""); setError(null); }}
                  className="ml-1 flex items-center justify-center w-6 h-6 rounded text-zinc-600 hover:text-zinc-300 hover:bg-white/[0.06] transition-colors"
                  aria-label="Close search"
                >
                  <X size={14} />
                </button>
              </div>

              {/* Error */}
              {error && (
                <div className="px-4 py-3 text-[11px] text-red-500/80">{error}</div>
              )}

              {/* Result */}
              {result && (
                <div className="p-4 space-y-4">
                  {/* Header */}
                  <div className="flex items-start justify-between gap-3">
                    <div>
                      <div className="flex items-center gap-2 mb-1">
                        <h3 className="text-xl font-bold tracking-widest text-white">{result.airport}</h3>
                        <FlightCatDot cat={result.flight_category} />
                        {result.flight_category && (
                          <span className={cn("text-[9px] font-bold tracking-widest", result.flight_category === "IFR" ? "text-red-400" : "text-zinc-400")}>
                            {result.flight_category}
                          </span>
                        )}
                      </div>
                      {result.has_atis && (
                        <p className="text-[10px] text-zinc-600">
                          ATIS {result.atis_callsign} · {result.atis_frequency} MHz
                        </p>
                      )}
                    </div>
                    <div className="text-right">
                      <p className="text-2xl font-bold text-white">{result.controller_count}</p>
                      <p className="text-[9px] text-zinc-600 uppercase tracking-wider">controllers</p>
                    </div>
                  </div>

                  {/* METAR */}
                  {result.raw_metar && (
                    <div className="p-3 rounded-lg bg-white/[0.02] border border-white/[0.06]">
                      <p className="text-[8px] uppercase tracking-widest text-zinc-700 mb-1">METAR</p>
                      <p className="text-[10px] text-zinc-400 font-mono leading-relaxed break-all">{result.raw_metar}</p>
                    </div>
                  )}

                  {/* Active controllers */}
                  {result.controllers?.length > 0 && (
                    <div>
                      <p className="text-[8px] uppercase tracking-widest text-zinc-700 mb-2 flex items-center gap-1.5">
                        <Radio size={9} /> Active Positions
                      </p>
                      <div className="flex flex-wrap gap-2">
                        {result.controllers.map((c) => (
                          <div key={c.callsign} className="flex items-center gap-1.5">
                            <ControllerBadge callsign={c.callsign} facility={c.facility_label} />
                            <span className="text-[10px] text-zinc-500">{c.frequency}</span>
                          </div>
                        ))}
                      </div>
                    </div>
                  )}

                  {/* Weather summary */}
                  {result.wx_summary && (
                    <p className="text-[10px] text-zinc-500">{result.wx_summary}</p>
                  )}
                </div>
              )}

              {/* Empty state */}
              {!result && !error && !loading && (
                <div className="px-4 py-6 text-center">
                  <p className="text-[10px] text-zinc-700 tracking-wider">Enter a 4-letter ICAO code and press Enter</p>
                </div>
              )}
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
    </>
  );
}
