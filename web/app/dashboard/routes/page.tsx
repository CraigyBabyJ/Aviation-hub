"use client";

import { useMemo, useState } from "react";

import { HubPanel } from "@/components/ui/hub-panel";
import { LiveStatus, Skeleton } from "@/components/ui/live-status";
import { SectionHeader } from "@/components/ui/section-header";
import { useHub } from "@/lib/hooks/use-hub";
import type { AirportCoverage, SuggestedRoute, VatsimRoutesResponse } from "@/lib/api-types";
import { cn } from "@/lib/utils";

const REGIONS = [
  { key: "uk", label: "UK" },
  { key: "europe", label: "Europe" },
  { key: "us", label: "US" },
  { key: "south_usa", label: "S USA" },
] as const;

const BADGES: { key: keyof AirportCoverage; label: string }[] = [
  { key: "hasDel", label: "DEL" },
  { key: "hasGnd", label: "GND" },
  { key: "hasTwr", label: "TWR" },
  { key: "hasApp", label: "APP" },
  { key: "hasCtr", label: "CTR" },
  { key: "hasAtis", label: "ATIS" },
];

function minutesLabel(minutes: number) {
  const hours = Math.floor(minutes / 60);
  const mins = minutes % 60;
  if (!hours) return `${mins} min`;
  if (!mins) return `${hours}h`;
  return `${hours}h ${mins}m`;
}

function CoverageBadges({ coverage }: { coverage: AirportCoverage }) {
  return (
    <div className="flex flex-wrap gap-1.5">
      {BADGES.map((badge) => (
        <span
          key={badge.label}
          className={cn(
            "rounded border px-1.5 py-0.5 text-[9px] font-bold tracking-widest",
            coverage[badge.key]
              ? "border-red-500/35 bg-red-500/10 text-red-200"
              : "border-white/[0.05] bg-white/[0.02] text-zinc-700"
          )}
        >
          {badge.label}
        </span>
      ))}
    </div>
  );
}

function RouteCard({ route }: { route: SuggestedRoute }) {
  return (
    <div className="hub-row p-4">
      <div className="flex items-start justify-between gap-4 mb-3">
        <div className="flex items-center gap-3 min-w-0">
          <div>
            <p className="text-[10px] uppercase tracking-[0.22em] text-zinc-600 mb-1">Departure</p>
            <p className="text-lg font-bold tracking-[0.16em] text-white">{route.departure.icao}</p>
            <p className="text-[11px] text-zinc-500 truncate">{route.departure.name || route.departure.country || "Live airport"}</p>
          </div>
          <div className="text-zinc-700 text-lg">→</div>
          <div>
            <p className="text-[10px] uppercase tracking-[0.22em] text-zinc-600 mb-1">Arrival</p>
            <p className="text-lg font-bold tracking-[0.16em] text-white">{route.arrival.icao}</p>
            <p className="text-[11px] text-zinc-500 truncate">{route.arrival.name || route.arrival.country || "Live airport"}</p>
          </div>
        </div>

        <div className="text-right flex-shrink-0">
          <p className="text-xl font-bold text-white tabular-nums">{minutesLabel(route.estimated_minutes)}</p>
          <p className="text-[10px] text-zinc-600">{route.distance_nm} nm</p>
        </div>
      </div>

      <div className="grid grid-cols-1 md:grid-cols-2 gap-3">
        <div>
          <p className="text-[10px] uppercase tracking-[0.18em] text-zinc-700 mb-1">
            {route.departure.icao} live services
          </p>
          <CoverageBadges coverage={route.departure.coverage} />
        </div>
        <div>
          <p className="text-[10px] uppercase tracking-[0.18em] text-zinc-700 mb-1">
            {route.arrival.icao} live services
          </p>
          <CoverageBadges coverage={route.arrival.coverage} />
        </div>
      </div>
    </div>
  );
}

export default function RoutesPage() {
  const [region, setRegion] = useState<(typeof REGIONS)[number]["key"]>("uk");
  const [minutes, setMinutes] = useState(90);

  const query = useMemo(
    () => ({ region, minutes: String(minutes), limit: "10" }),
    [region, minutes]
  );

  const { data, loading, error, lastUpdated, refresh } = useHub<VatsimRoutesResponse>(
    "vatsim/routes",
    query,
    { interval: 30_000 }
  );

  const routes = data?.routes ?? [];

  return (
    <div className="max-w-[1200px] mx-auto px-4 sm:px-6 py-6">
      <HubPanel cornerAccent className="mb-4">
        <div className="flex items-start justify-between gap-3 mb-5">
          <SectionHeader
            title="Route Finder"
            subtitle="Pick a region and target flight time for live-ATC ideas"
            live
            count={routes.length || undefined}
            className="mb-0"
          />
          <LiveStatus loading={loading} error={error} lastUpdated={lastUpdated} onRefresh={refresh} />
        </div>

        <div className="grid grid-cols-1 lg:grid-cols-[1fr_280px] gap-6">
          <div>
            <p className="text-[10px] uppercase tracking-[0.22em] text-zinc-600 mb-2">Region</p>
            <div className="flex flex-wrap gap-2">
              {REGIONS.map((item) => (
                <button
                  key={item.key}
                  type="button"
                  onClick={() => setRegion(item.key)}
                  className={cn(
                    "rounded-full border px-3 py-1.5 text-[10px] font-bold tracking-[0.18em] uppercase transition-colors",
                    region === item.key
                      ? "border-red-500/40 bg-red-500/10 text-red-200"
                      : "border-white/[0.06] bg-white/[0.02] text-zinc-500 hover:text-zinc-300"
                  )}
                >
                  {item.label}
                </button>
              ))}
            </div>
          </div>

          <div>
            <div className="flex items-center justify-between mb-2">
              <p className="text-[10px] uppercase tracking-[0.22em] text-zinc-600">Flight time</p>
              <p className="text-sm font-bold text-white tabular-nums">{minutesLabel(minutes)}</p>
            </div>
            <input
              type="range"
              min={30}
              max={240}
              step={15}
              value={minutes}
              onChange={(event) => setMinutes(Number(event.target.value))}
              className="w-full accent-red-500"
            />
            <p className="mt-2 text-[11px] text-zinc-600">
              Estimated using live airport coverage and a simple airliner-style cruise assumption.
            </p>
          </div>
        </div>
      </HubPanel>

      <div className="space-y-3">
        {loading && !routes.length
          ? Array.from({ length: 4 }).map((_, index) => (
              <HubPanel key={index}>
                <Skeleton className="w-40 h-5 mb-3" />
                <Skeleton className="w-full h-16 mb-3" />
                <div className="grid grid-cols-2 gap-3">
                  <Skeleton className="w-full h-10" />
                  <Skeleton className="w-full h-10" />
                </div>
              </HubPanel>
            ))
          : routes.map((route) => <RouteCard key={`${route.departure.icao}-${route.arrival.icao}`} route={route} />)}

        {!loading && !routes.length && (
          <HubPanel>
            <p className="text-sm text-zinc-500">
              No live-ATC routes matched that region and target time right now. Try another region or a wider time band.
            </p>
          </HubPanel>
        )}
      </div>
    </div>
  );
}
