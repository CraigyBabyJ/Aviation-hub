"use client";

import { useState } from "react";
import { Wind, Eye, Gauge, Thermometer, ChevronDown } from "lucide-react";
import { HubPanel } from "@/components/ui/hub-panel";
import { SectionHeader } from "@/components/ui/section-header";
import { LiveStatus, Skeleton } from "@/components/ui/live-status";
import { useHub } from "@/lib/hooks/use-hub";
import { cn } from "@/lib/utils";
import type { AirportStatus } from "@/lib/api-types";

interface WeatherSummaryResponse {
  generated_at: string;
  airports: AirportStatus[];
}

function parseMetar(raw: string) {
  if (!raw) return null;
  const parts = raw.replace(/^METAR\s+/, "").split(/\s+/);

  const windPart = parts.find((p) => /\d{3}\d{2}(G\d+)?KT/.test(p)) ?? "";
  const visPart = parts.find((p) => p === "9999" || /^\d{4}$/.test(p)) ?? "";
  const tempPart = parts.find((p) => /^M?\d+\/M?\d+$/.test(p)) ?? "";
  const qnhPart = parts.find((p) => /^Q\d{4}$/.test(p)) ?? "";
  const cavok = raw.includes("CAVOK");

  const windDir = windPart.slice(0, 3);
  const windSpd = windPart.slice(3, 5);
  const gust = windPart.includes("G") ? windPart.split("G")[1].replace("KT", "") : null;
  const [temp, dew] = tempPart.split("/");
  const qnh = qnhPart.slice(1);
  const vis = cavok
    ? "CAVOK"
    : visPart === "9999"
    ? "10km+"
    : visPart
    ? `${(+visPart / 1000).toFixed(1)}km`
    : "—";

  const isIfr = raw.includes(" IFR") || raw.includes("LIFR");
  const isMvfr = raw.includes("MVFR");
  const flightCat = isIfr ? "IFR" : isMvfr ? "MVFR" : "VFR";

  return { windDir, windSpd, gust, temp, dew, qnh, vis, flightCat };
}

function FlightCatBadge({ cat }: { cat: string }) {
  const styles: Record<string, string> = {
    IFR: "bg-red-950/40 border-red-600/30 text-red-400",
    MVFR: "bg-zinc-900 border-white/10 text-zinc-300",
    VFR: "bg-zinc-900 border-white/[0.06] text-zinc-500",
  };
  return (
    <span
      className={cn(
        "text-[8px] font-bold tracking-widest px-1.5 py-0.5 rounded border",
        styles[cat] ?? styles.VFR
      )}
    >
      {cat}
    </span>
  );
}

function MetaItem({
  icon,
  label,
  value,
}: {
  icon: React.ReactNode;
  label: string;
  value: string;
}) {
  return (
    <div className="flex flex-col gap-0.5">
      <div className="flex items-center gap-1 text-zinc-700">
        {icon}
        <span className="text-[8px] uppercase tracking-widest">{label}</span>
      </div>
      <span className="text-[11px] text-zinc-300 font-medium tabular-nums">{value}</span>
    </div>
  );
}

function WeatherRow({
  status,
  expanded,
  onToggle,
}: {
  status: AirportStatus;
  expanded: boolean;
  onToggle: () => void;
}) {
  const parsed = parseMetar(status.raw_metar);

  return (
    <div className="hub-row">
      <button
        onClick={onToggle}
        className="w-full flex items-center gap-3 h-[52px] text-left hover:bg-white/[0.015] transition-colors -mx-5 px-5"
      >
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-0.5">
            <span className="text-[11px] font-bold tracking-widest text-white">
              {status.airport}
            </span>
            {status.has_atc && (
              <span className="w-1 h-1 rounded-full bg-red-500 flex-shrink-0" />
            )}
          </div>
          <p className="text-[10px] text-zinc-600 truncate font-mono">
            {status.raw_metar?.replace(/^METAR\s+/, "").slice(0, 55)}…
          </p>
        </div>
        <div className="flex items-center gap-2 flex-shrink-0">
          {parsed && <FlightCatBadge cat={parsed.flightCat} />}
          {parsed?.qnh && (
            <span className="text-[10px] text-zinc-500 tabular-nums hidden sm:block">
              {parsed.qnh}
            </span>
          )}
          <ChevronDown
            size={12}
            className={cn(
              "text-zinc-700 transition-transform duration-200",
              expanded && "rotate-180"
            )}
          />
        </div>
      </button>

      {expanded && parsed && (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-2 py-2.5 -mx-5 px-5 bg-white/[0.01]">
          <MetaItem
            icon={<Thermometer size={9} />}
            label="TEMP"
            value={`${parsed.temp ?? "?"}° / ${parsed.dew ?? "?"}°`}
          />
          <MetaItem
            icon={<Wind size={9} />}
            label="WIND"
            value={`${parsed.windDir}°/${parsed.windSpd}kt${parsed.gust ? ` G${parsed.gust}kt` : ""}`}
          />
          <MetaItem icon={<Eye size={9} />} label="VIS" value={parsed.vis} />
          <MetaItem icon={<Gauge size={9} />} label="QNH" value={`${parsed.qnh} hPa`} />
          {status.raw_taf && (
            <div className="col-span-2 sm:col-span-4 pt-1">
              <p className="text-[8px] text-zinc-700 uppercase tracking-widest mb-1">TAF</p>
              <p className="text-[9px] text-zinc-500 font-mono leading-relaxed break-all">
                {status.raw_taf.replace(/^TAF\s+/, "").slice(0, 200)}…
              </p>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export function WeatherPanel({ icaos }: { icaos?: string[] }) {
  const params: Record<string, string> = icaos && icaos.length > 0 ? { icaos: icaos.join(",") } : {};
  const { data, loading, error, lastUpdated, refresh } =
    useHub<WeatherSummaryResponse>("weather-summary", params, { interval: 60_000 });

  const airports = data?.airports ?? [];
  const [expanded, setExpanded] = useState<string | null>(null);

  return (
    <HubPanel delay={0.1} className="flex flex-col h-full">
      <div className="flex items-start justify-between gap-2 mb-4">
        <SectionHeader
          title="Weather"
          subtitle="Click a row for full METAR / TAF"
          live
          className="mb-0"
        />
        <LiveStatus
          loading={loading}
          error={error}
          lastUpdated={lastUpdated}
          onRefresh={refresh}
        />
      </div>

      <div className="flex-1 overflow-y-auto -mx-5 px-5">
        {loading && !airports.length
          ? Array.from({ length: 5 }).map((_, i) => (
              <div key={i} className="hub-row py-3">
                <Skeleton className="w-full h-8" />
              </div>
            ))
          : airports.map((a) => (
              <WeatherRow
                key={a.airport}
                status={a}
                expanded={expanded === a.airport}
                onToggle={() =>
                  setExpanded(expanded === a.airport ? null : a.airport)
                }
              />
            ))}
      </div>
    </HubPanel>
  );
}
