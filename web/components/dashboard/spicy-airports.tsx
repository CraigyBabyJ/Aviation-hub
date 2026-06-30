"use client";

import { Flame } from "lucide-react";
import { HubPanel } from "@/components/ui/hub-panel";
import { SectionHeader } from "@/components/ui/section-header";
import { LiveStatus, Skeleton } from "@/components/ui/live-status";
import { cn } from "@/lib/utils";
import type { RankedResponse, RankedAirport, RankedController } from "@/lib/api-types";

const CHALLENGE_COLORS = {
  extreme: "text-red-400",
  hard: "text-zinc-200",
  moderate: "text-zinc-400",
  easy: "text-zinc-600",
} as const;

const CHALLENGE_BADGE = {
  extreme: "border-red-600/40 bg-red-950/30 text-red-400",
  hard: "border-white/10 bg-white/[0.04] text-zinc-300",
  moderate: "border-white/[0.06] bg-transparent text-zinc-400",
  easy: "border-white/[0.04] bg-transparent text-zinc-600",
} as const;

function IntensityBar({ level }: { level: RankedAirport["challenge_level"] }) {
  const fill = level === "extreme" ? 4 : level === "hard" ? 3 : level === "moderate" ? 2 : level === "easy" ? 1 : 0;
  const label = level ? level.charAt(0).toUpperCase() + level.slice(1) : "Unknown";
  return (
    <div
      className="flex items-end gap-0.5 relative group cursor-default"
      title={`Weather difficulty: ${label}`}
    >
      {[1, 2, 3, 4].map((i) => (
        <div
          key={i}
          className={cn(
            "w-0.5 rounded-sm",
            i <= fill
              ? level === "extreme" ? "bg-red-500" : "bg-zinc-500"
              : "bg-white/[0.06]",
            i === 1 ? "h-1.5" : i === 2 ? "h-2" : i === 3 ? "h-2.5" : "h-3"
          )}
        />
      ))}
      <div className="absolute top-full right-0 mt-1.5 hidden group-hover:block z-20 pointer-events-none">
        <div className="bg-zinc-900 border border-white/10 rounded px-2 py-1 text-[9px] tracking-widest uppercase text-zinc-400 whitespace-nowrap">
          Wx: {label}
        </div>
      </div>
    </div>
  );
}

function AirportRow({ airport, rank }: { airport: RankedAirport; rank: number }) {
  return (
    <div className={cn("hub-row flex items-center gap-3 h-[52px]", rank === 0 && "border-t border-white/[0.04]")}>
      <span className="text-[10px] text-zinc-700 w-4 text-right flex-shrink-0 tabular-nums">{rank + 1}</span>

      <div className={cn("flex-shrink-0 px-2 py-0.5 rounded border text-[11px] font-bold tracking-widest", CHALLENGE_BADGE[airport.challenge_level])}>
        {airport.airport}
      </div>

      <div className="flex-1 min-w-0">
        <p className="text-[10px] text-zinc-500">{airport.country}</p>
      </div>

      <div className="flex items-center gap-3 flex-shrink-0">
        <div className="text-right">
          <p className={cn("text-sm font-bold tabular-nums", CHALLENGE_COLORS[airport.challenge_level])}>
            {airport.inbounds}
          </p>
          <p className="text-[9px] text-zinc-700 uppercase tracking-wide">in</p>
        </div>
        <div className="relative group text-right cursor-default">
          <p className="text-sm font-bold tabular-nums text-zinc-400">{airport.controller_count}</p>
          <p className="text-[9px] text-zinc-700 uppercase tracking-wide">atc</p>
          {airport.controllers && airport.controllers.length > 0 && (
            <div className="absolute top-full right-0 mt-1.5 hidden group-hover:block z-30 pointer-events-none">
              <div className="bg-zinc-900 border border-white/10 rounded-lg px-3 py-2 shadow-xl min-w-[160px]">
                {airport.controllers.map((c: RankedController) => (
                  <div key={c.callsign} className="flex items-center gap-2 py-0.5">
                    <span className="text-[9px] font-bold text-zinc-500 w-8 flex-shrink-0">{c.facility}</span>
                    <span className="text-[10px] text-white truncate">{c.name || c.callsign}</span>
                  </div>
                ))}
              </div>
            </div>
          )}
        </div>
        <IntensityBar level={airport.challenge_level} />
      </div>

      {airport.challenge_level === "extreme" && (
        <Flame size={12} className="text-red-500/70 flex-shrink-0" />
      )}
    </div>
  );
}

interface SpicyAirportsProps {
  data?:        RankedResponse;
  loading?:     boolean;
  error?:       string | null | undefined;
  lastUpdated?: Date | null;
  refresh?:     () => void;
}

export function SpicyAirports({ data, loading = false, error, lastUpdated, refresh }: SpicyAirportsProps) {
  const airports = data?.airports ?? [];

  return (
    <HubPanel cornerAccent delay={0.05} className="flex flex-col h-full">
      <div className="flex items-start justify-between gap-2 mb-4">
        <SectionHeader
          title="Airports Ranked by Traffic"
          subtitle="Busiest on the network right now"
          live
          count={airports.length || undefined}
          className="mb-0"
        />
        <LiveStatus loading={loading} error={error ?? null} lastUpdated={lastUpdated ?? null} onRefresh={refresh} />
      </div>

      <div className="flex flex-col gap-0 flex-1 overflow-y-auto -mx-5 px-5">
        {loading && !data
          ? Array.from({ length: 6 }).map((_, i) => (
              <div key={i} className="hub-row py-3 flex items-center gap-3">
                <Skeleton className="w-4 h-3" />
                <Skeleton className="w-14 h-5" />
                <Skeleton className="flex-1 h-3" />
                <Skeleton className="w-20 h-4" />
              </div>
            ))
          : airports.map((a, i) => <AirportRow key={a.airport} airport={a} rank={i} />)}
      </div>
    </HubPanel>
  );
}
