"use client";

import { useState } from "react";

import { HubPanel } from "@/components/ui/hub-panel";
import { SectionHeader } from "@/components/ui/section-header";
import { LiveStatus, Skeleton } from "@/components/ui/live-status";
import { useHub } from "@/lib/hooks/use-hub";
import { cn } from "@/lib/utils";
import type { AirportCoverage, VatsimAirportsResponse, VatsimAirportListItem } from "@/lib/api-types";

const FILTER_DEFS = [
  { key: "hasAnyAtc", label: "Any ATC", query: "any_atc" },
  { key: "hasTwr", label: "Tower", query: "tower" },
  { key: "hasGroundOps", label: "Ground ops", query: "ground_ops" },
  { key: "hasRadar", label: "Approach / Center", query: "radar" },
  { key: "fullCoverage", label: "Full coverage", query: "full_coverage" },
  { key: "hasAtis", label: "ATIS", query: "atis" },
] as const;

const SERVICE_BADGES = [
  { key: "hasDel", label: "DEL" },
  { key: "hasGnd", label: "GND" },
  { key: "hasTwr", label: "TWR" },
  { key: "hasApp", label: "APP" },
  { key: "hasCtr", label: "CTR" },
  { key: "hasAtis", label: "ATIS" },
] as const;

type FilterKey = (typeof FILTER_DEFS)[number]["key"];

function FilterChip({
  label,
  active,
  onClick,
}: {
  label: string;
  active: boolean;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={onClick}
      className={cn(
        "rounded-full border px-2.5 py-1 text-[9px] font-bold tracking-[0.18em] uppercase transition-colors",
        active
          ? "border-[rgba(var(--accent),0.4)] bg-[rgba(var(--accent),0.1)] text-white"
          : "border-white/[0.06] bg-white/[0.02] text-zinc-500 hover:border-white/[0.14] hover:text-zinc-300"
      )}
    >
      {label}
    </button>
  );
}

function ServiceBadge({
  label,
  active,
}: {
  label: string;
  active: boolean;
}) {
  return (
    <span
      className={cn(
        "rounded border px-1.5 py-0.5 text-[9px] font-bold tracking-widest",
        active
          ? "border-[rgba(var(--accent),0.35)] bg-[rgba(var(--accent),0.1)] text-white"
          : "border-white/[0.05] bg-white/[0.02] text-zinc-700"
      )}
    >
      {label}
    </span>
  );
}

function AirportRow({ airport }: { airport: VatsimAirportListItem }) {
  const activeServices = SERVICE_BADGES.filter(({ key }) => airport.coverage[key]).length;

  return (
    <div className="hub-row py-3">
      <div className="flex items-start justify-between gap-3">
        <div className="min-w-0 flex-1">
          <div className="flex items-center gap-2 mb-1">
            <span className="rounded border border-white/[0.08] px-2 py-0.5 text-[10px] font-bold tracking-[0.2em] text-white">
              {airport.icao}
            </span>
            <span className="text-[10px] text-zinc-600 truncate">
              {airport.name || airport.country || "Live coverage"}
            </span>
          </div>
          <div className="flex flex-wrap gap-1.5">
            {SERVICE_BADGES.map((badge) => (
              <ServiceBadge
                key={badge.label}
                label={badge.label}
                active={airport.coverage[badge.key]}
              />
            ))}
          </div>
          {airport.controllers.length > 0 && (
            <div className="mt-1.5 flex flex-wrap gap-x-2 gap-y-0.5">
              {airport.controllers.map((c) => (
                <span key={c.callsign} className="text-[9px] text-zinc-600">
                  <span className="text-zinc-500 font-mono">{c.facility_label}</span>
                  {c.name ? ` ${c.name}` : ""}
                </span>
              ))}
            </div>
          )}
        </div>

        <div className="flex items-center gap-3 flex-shrink-0">
          <div className="text-right">
            <p className="text-sm font-bold tabular-nums text-white">{airport.controller_count}</p>
            <p className="text-[9px] uppercase tracking-wide text-zinc-700">ATC</p>
          </div>
          <div className="text-right">
            <p className="text-sm font-bold tabular-nums text-zinc-400">{activeServices}</p>
            <p className="text-[9px] uppercase tracking-wide text-zinc-700">svc</p>
          </div>
        </div>
      </div>
    </div>
  );
}

interface IvaoController { callsign: string; airport: string; facility_label: string; frequency: string; rating_label: string; logon_minutes: number; atis: string }
interface IvaoControllersResponse { controllers: IvaoController[]; total: number; ts: string }

function IvaoAtcPanel({ loading, error, lastUpdated, refresh, controllers }: {
  loading: boolean; error: string | null; lastUpdated: Date | null; refresh: () => void;
  controllers: IvaoController[];
}) {
  const FAC_COLOR: Record<string, string> = { CTR:"#ef4444", APP:"#f97316", TWR:"#fbbf24", GND:"#34d399", DEL:"#60a5fa", ATIS:"#a78bfa" };
  return (
    <HubPanel delay={0.2} className="flex flex-col h-full" noPadding>
      <div className="p-5 pb-3 flex items-start justify-between gap-2">
        <SectionHeader title="ATC Coverage" subtitle="Live IVAO controllers" live count={controllers.length || undefined} className="mb-0" />
        <LiveStatus loading={loading} error={error} lastUpdated={lastUpdated} onRefresh={refresh} />
      </div>
      <div className="px-5 pb-2 text-[10px] text-zinc-600">{controllers.length} controllers online</div>
      <div className="flex-1 overflow-y-auto px-5 pb-3">
        {loading && !controllers.length
          ? Array.from({ length: 6 }).map((_, i) => <div key={i} className="hub-row py-3"><Skeleton className="w-full h-8" /></div>)
          : controllers.map((c) => {
            const color = FAC_COLOR[c.facility_label] ?? "#94a3b8";
            return (
              <div key={c.callsign} className="hub-row py-2.5 flex items-center gap-2 text-xs">
                <span className="font-mono font-bold text-white text-[11px] w-28 flex-shrink-0 truncate">{c.callsign}</span>
                <span className="text-[9px] px-1.5 py-0.5 rounded-full font-bold" style={{ color, background:`${color}18`, border:`1px solid ${color}40` }}>{c.facility_label}</span>
                <span className="font-mono text-zinc-500 text-[10px]">{c.frequency}</span>
                <span className="ml-auto text-[9px] text-zinc-600">{c.rating_label}</span>
              </div>
            );
          })}
        {!loading && !controllers.length && <div className="hub-row py-6 text-center text-[11px] text-zinc-600">No IVAO controllers online.</div>}
      </div>
    </HubPanel>
  );
}

export function AtcPanel({ network = "vatsim" }: { network?: string }) {
  const ivao = useHub<IvaoControllersResponse>("ivao-controllers", {}, { interval: 30_000 });

  const [filters, setFilters] = useState<Record<FilterKey, boolean>>({
    hasAnyAtc: false, hasTwr: false, hasGroundOps: false, hasRadar: false, fullCoverage: false, hasAtis: false,
  });

  const queryParams: Record<string, string> = { limit: "80", sort: "coverage" };
  for (const filter of FILTER_DEFS) {
    if (filters[filter.key]) queryParams[filter.query] = "1";
  }

  const { data, loading, error, lastUpdated, refresh } = useHub<VatsimAirportsResponse>(
    "vatsim/airports", queryParams, { interval: 30_000 }
  );

  if (network === "ivao") {
    return <IvaoAtcPanel
      loading={ivao.loading} error={ivao.error} lastUpdated={ivao.lastUpdated} refresh={ivao.refresh}
      controllers={ivao.data?.controllers ?? []}
    />;
  }

  const airports = data?.airports ?? [];
  const totalControllers = airports.reduce((sum, airport) => sum + airport.controller_count, 0);
  const activeFilterCount = FILTER_DEFS.filter((filter) => filters[filter.key]).length;

  return (
    <HubPanel delay={0.2} className="flex flex-col h-full" noPadding>
      <div className="p-5 pb-3 flex items-start justify-between gap-2">
        <SectionHeader
          title="ATC Coverage"
          subtitle="Filter airports by live VATSIM services"
          live
          count={airports.length || undefined}
          className="mb-0"
        />
        <LiveStatus
          loading={loading}
          error={error}
          lastUpdated={lastUpdated}
          onRefresh={refresh}
        />
      </div>

      <div className="px-5 pb-3">
        <div className="flex flex-wrap gap-2">
          {FILTER_DEFS.map((filter) => (
            <FilterChip
              key={filter.key}
              label={filter.label}
              active={filters[filter.key]}
              onClick={() => setFilters((current) => ({ ...current, [filter.key]: !current[filter.key] }))}
            />
          ))}
        </div>
        <div className="mt-2 flex items-center justify-between text-[10px] text-zinc-600">
          <span>
            {totalControllers} controllers across {airports.length} airports
          </span>
          <span>{activeFilterCount ? `${activeFilterCount} filters active` : "Coverage-rich first"}</span>
        </div>
      </div>

      <div className="flex-1 overflow-y-auto px-5 pb-3">
        {loading && !airports.length
          ? Array.from({ length: 6 }).map((_, i) => (
              <div key={i} className="hub-row py-3">
                <div className="flex items-center gap-3">
                  <Skeleton className="w-14 h-5" />
                  <Skeleton className="flex-1 h-4" />
                  <Skeleton className="w-16 h-8" />
                </div>
                <div className="mt-2 flex gap-1.5">
                  {Array.from({ length: 6 }).map((__, badgeIndex) => (
                    <Skeleton key={badgeIndex} className="w-9 h-5" />
                  ))}
                </div>
              </div>
            ))
          : airports.map((airport) => <AirportRow key={airport.icao} airport={airport} />)}

        {!loading && !airports.length && (
          <div className="hub-row py-6 text-center text-[11px] text-zinc-600">
            No airports match the current live coverage filters.
          </div>
        )}
      </div>
    </HubPanel>
  );
}
