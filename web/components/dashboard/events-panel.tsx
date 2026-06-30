"use client";

import { ExternalLink } from "lucide-react";
import { HubPanel } from "@/components/ui/hub-panel";
import { SectionHeader } from "@/components/ui/section-header";
import { LiveStatus, Skeleton } from "@/components/ui/live-status";
import { useHub } from "@/lib/hooks/use-hub";
import type { EventsResponse, VatsimEvent } from "@/lib/api-types";

interface IvaoEvent {
  id: string; title: string; date_label: string; time_label: string;
  airports: string[]; description: string; image: string; url: string; starts_at: string | null;
}

function formatTime(iso: string) {
  return new Date(iso).toLocaleTimeString("en-GB", {
    hour: "2-digit", minute: "2-digit", timeZone: "UTC", hour12: false,
  }) + "z";
}

function formatDate(iso: string) {
  return new Date(iso).toLocaleDateString("en-GB", {
    day: "2-digit", month: "short", timeZone: "UTC",
  });
}

function isLive(event: VatsimEvent) {
  const now = Date.now();
  return new Date(event.start_time_utc).getTime() <= now && new Date(event.end_time_utc).getTime() >= now;
}

function isSoon(event: VatsimEvent) {
  const diff = new Date(event.start_time_utc).getTime() - Date.now();
  return diff > 0 && diff < 2 * 60 * 60 * 1000; // within 2 hours
}

function parseAirports(raw: string): string[] {
  try { return JSON.parse(raw); } catch { return []; }
}

function EventTypeBadge({ type }: { type: string }) {
  const t = type.toUpperCase();
  const styles: Record<string, string> = {
    "FRIDAY NIGHT OPS": "border-red-600/40 bg-red-950/30 text-red-400",
    "CROSS THE POND": "border-white/10 bg-white/[0.04] text-white",
    "EVENT": "border-white/[0.06] text-zinc-500",
  };
  const label = t === "FRIDAY NIGHT OPS" ? "FNO" : t === "CROSS THE POND" ? "CTP" : type;
  const styleKey = Object.keys(styles).find((k) => t.includes(k)) ?? "EVENT";
  return (
    <span className={`text-[8px] font-bold tracking-widest uppercase px-1.5 py-0.5 rounded border ${styles[styleKey]}`}>
      {label}
    </span>
  );
}

function EventCard({ event }: { event: VatsimEvent }) {
  const live = isLive(event);
  const soon = isSoon(event);
  const airports = parseAirports(event.airports_json);

  return (
    <div className={`group relative p-3 rounded-lg border transition-colors duration-200 ${
      live
        ? "accent-card-active border-red-600/30 bg-red-950/10 hover:border-red-600/50"
        : "border-white/[0.05] bg-white/[0.015] hover:border-white/[0.1] hover:bg-white/[0.03]"
    }`}>
      {/* Status + date */}
      <div className="flex items-start justify-between gap-2 mb-2">
        <div className="flex items-center gap-2">
          {live && <span className="accent-dot live-dot w-1.5 h-1.5 rounded-full bg-red-500 block flex-shrink-0" />}
          {soon && !live && <span className="text-[8px] uppercase tracking-widest text-zinc-500 border border-white/[0.06] px-1.5 py-0.5 rounded">Soon</span>}
          <EventTypeBadge type={event.event_type} />
        </div>
        <div className="flex items-center gap-1.5 flex-shrink-0">
          <span className="text-[9px] text-zinc-600 tabular-nums">{formatDate(event.start_time_utc)}</span>
          {event.link_url && (
            <a href={event.link_url} target="_blank" rel="noopener noreferrer" className="text-zinc-700 hover:text-zinc-400 transition-colors">
              <ExternalLink size={9} />
            </a>
          )}
        </div>
      </div>

      {/* Name */}
      <p className="text-xs text-white font-semibold leading-snug mb-2">{event.name}</p>

      {/* Time + airports */}
      <div className="flex items-center justify-between gap-2">
        <span className="text-[10px] text-zinc-500 tabular-nums">
          {formatTime(event.start_time_utc)} – {formatTime(event.end_time_utc)}
        </span>
        <div className="flex items-center gap-1 flex-wrap justify-end">
          {airports.slice(0, 4).map((icao) => (
            <span key={icao} className="text-[8px] font-mono tracking-wider text-zinc-600 px-1 border border-white/[0.04] rounded">
              {icao}
            </span>
          ))}
          {airports.length > 4 && <span className="text-[8px] text-zinc-700">+{airports.length - 4}</span>}
        </div>
      </div>

      {/* Short desc on hover */}
      {event.short_description && (
        <p className="hidden group-hover:block text-[9px] text-zinc-600 mt-2 leading-relaxed line-clamp-2">
          {event.short_description}
        </p>
      )}
    </div>
  );
}

export function EventsPanel({ network = "vatsim" }: { network?: string }) {
  const { data, loading, error, lastUpdated, refresh } = useHub<EventsResponse>(
    "vatsim/events",
    { days: "14", limit: "15" },
    { interval: 120_000 }
  );

  const events = data?.events ?? [];

  const ivao = useHub<{ events: IvaoEvent[]; count: number }>("ivao-events", {}, { interval: 300_000 });

  if (network === "ivao") {
    const ivaoEvents = ivao.data?.events ?? [];
    return (
      <HubPanel delay={0.25} className="flex flex-col h-full" cornerAccent>
        <div className="flex items-start justify-between gap-2 mb-4">
          <SectionHeader
            title="Upcoming Events"
            subtitle="IVAO scheduled operations"
            count={ivao.data?.count}
            className="mb-0"
          />
          <LiveStatus loading={ivao.loading} error={ivao.error} lastUpdated={ivao.lastUpdated} onRefresh={ivao.refresh} />
        </div>
        <div className="flex flex-col gap-2 flex-1 overflow-y-auto">
          {ivao.loading && !ivaoEvents.length
            ? Array.from({ length: 3 }).map((_, i) => (
                <div key={i} className="hub-row p-3"><Skeleton className="w-full h-14" /></div>
              ))
            : ivaoEvents.map((ev) => (
                <a
                  key={ev.id}
                  href={ev.url}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="hub-row p-3 block hover:bg-white/[0.03] transition-colors group"
                >
                  <div className="flex items-start justify-between gap-2">
                    <span className="text-xs font-semibold text-white group-hover:text-blue-300 transition-colors line-clamp-1">{ev.title}</span>
                    <ExternalLink className="w-3 h-3 text-zinc-600 flex-shrink-0 mt-0.5" />
                  </div>
                  <div className="flex items-center gap-2 mt-1 flex-wrap">
                    {ev.date_label && (
                      <span className="text-[10px] text-blue-400 font-mono">{ev.date_label}</span>
                    )}
                    {ev.time_label && (
                      <span className="text-[10px] text-zinc-500">{ev.time_label}</span>
                    )}
                    {ev.airports.slice(0, 3).map((a) => (
                      <span key={a} className="text-[9px] px-1.5 py-0.5 rounded bg-zinc-800/80 text-zinc-400 font-mono">{a}</span>
                    ))}
                  </div>
                </a>
              ))}
          {!ivao.loading && !ivaoEvents.length && (
            <div className="hub-row p-6 text-center text-[11px] text-zinc-600">No IVAO events found.</div>
          )}
        </div>
      </HubPanel>
    );
  }

  return (
    <HubPanel delay={0.25} className="flex flex-col h-full" cornerAccent>
      <div className="flex items-start justify-between gap-2 mb-4">
        <SectionHeader
          title="Upcoming Events"
          subtitle="VATSIM scheduled operations"
          count={data?.count}
          className="mb-0"
        />
        <LiveStatus loading={loading} error={error} lastUpdated={lastUpdated} onRefresh={refresh} />
      </div>

      <div className="flex flex-col gap-2 flex-1 overflow-y-auto">
        {loading && !events.length
          ? Array.from({ length: 4 }).map((_, i) => <Skeleton key={i} className="h-20 rounded-lg" />)
          : events.map((ev) => <EventCard key={ev.event_id} event={ev} />)
        }
        {!loading && !events.length && (
          <p className="text-[11px] text-zinc-700 text-center py-8 tracking-widest uppercase">No events found</p>
        )}
      </div>
    </HubPanel>
  );
}
