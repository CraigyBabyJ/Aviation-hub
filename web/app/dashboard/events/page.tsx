"use client";

import { useEffect, useState, useMemo, useCallback } from "react";
import { useNetwork } from "@/lib/network-context";

interface VatsimEvent {
  event_id:          string;
  name:              string;
  event_type:        string;
  start_time_utc:    string;
  end_time_utc:      string;
  short_description: string;
  link_url:          string;
  airports_json:     string;
}

interface EventsResponse {
  count:   number;
  events:  VatsimEvent[];
}

type Filter = "all" | "now" | "today" | "upcoming";

function parseAirports(json: string): string[] {
  try { return JSON.parse(json) as string[]; } catch { return []; }
}

function isNow(e: VatsimEvent): boolean {
  const now = Date.now();
  return new Date(e.start_time_utc).getTime() <= now && new Date(e.end_time_utc).getTime() >= now;
}

function isToday(e: VatsimEvent): boolean {
  const today = new Date().toDateString();
  return new Date(e.start_time_utc).toDateString() === today;
}

function formatDate(utc: string): string {
  return new Date(utc).toLocaleDateString("en-GB", {
    weekday: "short", day: "numeric", month: "short",
  });
}

function formatTime(utc: string): string {
  return new Date(utc).toLocaleTimeString("en-GB", {
    hour: "2-digit", minute: "2-digit", timeZoneName: "short",
  });
}

function timeUntil(utc: string): string {
  const diff = new Date(utc).getTime() - Date.now();
  if (diff <= 0) return "Now";
  const h = Math.floor(diff / 3_600_000);
  const m = Math.floor((diff % 3_600_000) / 60_000);
  if (h >= 24) {
    const d = Math.floor(h / 24);
    return `${d}d ${h % 24}h`;
  }
  return h > 0 ? `${h}h ${m}m` : `${m}m`;
}

interface IvaoEvent {
  id: string; title: string; date_label: string; time_label: string;
  airports: string[]; description: string; image: string; url: string; starts_at: string | null;
}

function IvaoEventsView() {
  const [events, setEvents] = useState<IvaoEvent[]>([]);
  const [loading, setLoading] = useState(true);
  const [query, setQuery] = useState("");

  useEffect(() => {
    setLoading(true);
    fetch("/api/hub/ivao-events")
      .then(r => r.json())
      .then(d => { setEvents(d.events ?? []); setLoading(false); })
      .catch(() => setLoading(false));
  }, []);

  const filtered = useMemo(() => {
    const q = query.toLowerCase();
    if (!q) return events;
    return events.filter(e =>
      e.title.toLowerCase().includes(q) ||
      e.airports.some(a => a.toLowerCase().includes(q))
    );
  }, [events, query]);

  return (
    <div className="px-4 md:px-8 py-6 max-w-[1400px] mx-auto">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 mb-6">
        <div>
          <div className="flex items-center gap-2 mb-1">
            <span className="accent-dot w-2 h-2 rounded-full animate-pulse block transition-[background-color] duration-500" style={{ backgroundColor: "var(--accent-hex)" }} />
            <h1 className="text-xl font-bold tracking-tight text-white">IVAO Events · <span style={{ color: "var(--accent-text)" }}>IVAO</span></h1>
          </div>
          <p className="text-xs text-zinc-500">{loading ? "Loading…" : `${events.length} upcoming events`}</p>
        </div>
        <input
          type="text" placeholder="Search events, airports…" value={query}
          onChange={e => setQuery(e.target.value)}
          className="accent-search w-full sm:w-72 px-3 py-2 rounded-lg bg-zinc-900 border border-white/[0.08] text-sm text-zinc-200 placeholder:text-zinc-600 focus:outline-none transition-[border-color] duration-500"
        />
      </div>
      {loading && <div className="py-20 text-center text-zinc-600 text-xs tracking-widest uppercase">Loading IVAO events…</div>}
      {!loading && filtered.length === 0 && <div className="py-20 text-center text-zinc-600 text-xs tracking-widest uppercase">No events found</div>}
      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
        {filtered.map(e => (
          <a key={e.id} href={e.url} target="_blank" rel="noopener noreferrer"
            className="group relative rounded-xl border border-white/[0.08] bg-white/[0.02] p-5 flex flex-col gap-3 hover:border-white/[0.14] transition-colors no-underline"
          >
            {e.image && (
              <div className="w-full h-32 rounded-lg overflow-hidden bg-zinc-900">
                <img src={e.image} alt={e.title} className="w-full h-full object-cover opacity-70 group-hover:opacity-90 transition-opacity" />
              </div>
            )}
            <h2 className="text-sm font-bold text-white leading-snug">{e.title}</h2>
            <div className="flex items-center gap-3 text-[10px] text-zinc-500">
              <span>{e.date_label}</span>
              {e.time_label && <><span className="text-zinc-700">·</span><span className="font-mono">{e.time_label}</span></>}
            </div>
            {e.airports.length > 0 && (
              <div className="flex flex-wrap gap-1.5">
                {e.airports.map(a => (
                  <span key={a} className="text-[10px] font-mono font-bold tracking-wider px-2 py-0.5 rounded bg-white/[0.05] text-zinc-300 border border-white/[0.06]">{a}</span>
                ))}
              </div>
            )}
            <div className="mt-auto pt-2 border-t border-white/[0.06]">
              <span className="text-[9px] tracking-widest uppercase text-zinc-600 group-hover:text-zinc-400 transition-colors">View on ivao.events →</span>
            </div>
          </a>
        ))}
      </div>
    </div>
  );
}

export default function EventsPage() {
  const { network } = useNetwork();
  const [events, setEvents]       = useState<VatsimEvent[]>([]);
  const [loading, setLoading]     = useState(true);
  const [lastUpdate, setLastUpdate] = useState<Date | null>(null);
  const [query, setQuery]         = useState("");
  const [filter, setFilter]       = useState<Filter>("all");

  const fetchEvents = useCallback(async () => {
    try {
      const res  = await fetch("/api/hub/events");
      const data: EventsResponse = await res.json();
      if (data.events) {
        setEvents(data.events);
        setLastUpdate(new Date());
      }
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchEvents();
    const iv = setInterval(fetchEvents, 300_000);
    return () => clearInterval(iv);
  }, [fetchEvents]);

  if (network === "ivao") return <IvaoEventsView />;

  const filtered = useMemo(() => {
    const q = query.toLowerCase();
    return events.filter((e) => {
      if (filter === "now"      && !isNow(e))                      return false;
      if (filter === "today"    && !isToday(e))                    return false;
      if (filter === "upcoming" && (isNow(e) || isToday(e)))       return false;
      if (q && !e.name.toLowerCase().includes(q) &&
               !e.short_description.toLowerCase().includes(q) &&
               !e.airports_json.toLowerCase().includes(q))         return false;
      return true;
    });
  }, [events, query, filter]);

  const counts = useMemo(() => ({
    now:      events.filter(isNow).length,
    today:    events.filter((e) => isToday(e) && !isNow(e)).length,
    upcoming: events.filter((e) => !isToday(e)).length,
  }), [events]);

  return (
    <div className="px-4 md:px-8 py-6 max-w-[1400px] mx-auto">

      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 mb-6">
        <div>
          <div className="flex items-center gap-2 mb-1">
            <span className="accent-dot w-2 h-2 rounded-full bg-red-500 animate-pulse block transition-[background-color] duration-500" />
            <h1 className="text-xl font-bold tracking-tight text-white">Events · <span style={{ color: "var(--accent-text)" }}>VATSIM</span></h1>
          </div>
          <p className="text-xs text-zinc-500">
            {loading ? "Loading…" : `${events.length} events in the next 30 days`}
            {lastUpdate && (
              <span className="ml-2 text-zinc-600">· updated {lastUpdate.toLocaleTimeString()}</span>
            )}
          </p>
        </div>
        <input
          type="text"
          placeholder="Search events, airports…"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          className="accent-search w-full sm:w-72 px-3 py-2 rounded-lg bg-zinc-900 border border-white/[0.08] text-sm text-zinc-200 placeholder:text-zinc-600 focus:outline-none focus:border-red-500/50 transition-[border-color] duration-500"
        />
      </div>

      {/* Summary stat cards */}
      {!loading && (
        <div className="grid grid-cols-3 gap-3 mb-6">
          {[
            { label: "Happening Now", value: counts.now,      color: "#ef4444" },
            { label: "Later Today",   value: counts.today,    color: "#f97316" },
            { label: "Upcoming",      value: counts.upcoming, color: "#60a5fa" },
          ].map(({ label, value, color }) => (
            <div
              key={label}
              className="rounded-xl border border-white/[0.06] bg-white/[0.02] px-4 py-3"
              style={{ borderColor: `${color}30` }}
            >
              <p className="text-[9px] tracking-widest uppercase text-zinc-500 mb-1">{label}</p>
              <p className="text-2xl font-bold" style={{ color }}>{value}</p>
            </div>
          ))}
        </div>
      )}

      {/* Filter pills */}
      <div className="flex flex-wrap gap-2 mb-6">
        {(
          [
            { key: "all",      label: `All · ${events.length}` },
            { key: "now",      label: `Now · ${counts.now}`,      color: "#ef4444" },
            { key: "today",    label: `Today · ${counts.today}`,  color: "#f97316" },
            { key: "upcoming", label: `Upcoming · ${counts.upcoming}`, color: "#60a5fa" },
          ] as { key: Filter; label: string; color?: string }[]
        ).map(({ key, label, color }) => {
          const active = filter === key;
          return (
            <button
              key={key}
              onClick={() => setFilter(key)}
              className="px-3 py-1 rounded-full text-[10px] tracking-widest uppercase font-medium border transition-colors"
              style={
                active && color
                  ? { color, borderColor: `${color}60`, background: `${color}18` }
                  : active
                  ? { color: "var(--accent-text)", borderColor: `rgba(var(--accent),0.5)`, background: `rgba(var(--accent),0.12)` }
                  : { color: "#71717a", borderColor: "rgba(255,255,255,0.06)" }
              }
            >
              {label}
            </button>
          );
        })}
      </div>

      {/* Events grid */}
      {loading && (
        <div className="py-20 text-center text-zinc-600 text-xs tracking-widest uppercase">
          Loading events…
        </div>
      )}

      {!loading && filtered.length === 0 && (
        <div className="py-20 text-center text-zinc-600 text-xs tracking-widest uppercase">
          No events match your filter
        </div>
      )}

      <div className="grid grid-cols-1 md:grid-cols-2 xl:grid-cols-3 gap-4">
        {filtered.map((e) => {
          const live     = isNow(e);
          const airports = parseAirports(e.airports_json);
          const starts   = new Date(e.start_time_utc);
          const ended    = new Date(e.end_time_utc).getTime() < Date.now();

          return (
            <div
              key={e.event_id}
              className="relative rounded-xl border border-white/[0.08] bg-white/[0.02] p-5 flex flex-col gap-3 hover:border-white/[0.14] transition-colors"
              style={live ? { borderColor: "rgba(var(--accent),0.35)", background: "rgba(var(--accent),0.04)" } : {}}
            >
              {/* Live badge */}
              {live && (
                <div className="absolute top-4 right-4 flex items-center gap-1.5 px-2 py-0.5 rounded-full transition-[background-color,border-color] duration-500"
                  style={{ background: "rgba(var(--accent),0.18)", border: "1px solid rgba(var(--accent),0.4)" }}>
                  <span className="accent-dot w-1.5 h-1.5 rounded-full bg-red-500 animate-pulse block transition-[background-color] duration-500" />
                  <span className="text-[9px] tracking-widest uppercase font-medium transition-[color] duration-500" style={{ color: "var(--accent-text)" }}>Live</span>
                </div>
              )}

              {/* Event type badge */}
              <div className="flex items-center gap-2">
                <span className="text-[9px] tracking-widest uppercase text-zinc-600 border border-white/[0.06] px-2 py-0.5 rounded-full">
                  {e.event_type}
                </span>
                {!live && !ended && (
                  <span className="text-[9px] tracking-widest uppercase text-zinc-500">
                    in {timeUntil(e.start_time_utc)}
                  </span>
                )}
                {ended && (
                  <span className="text-[9px] tracking-widest uppercase text-zinc-700">Ended</span>
                )}
              </div>

              {/* Name */}
              <h2 className="text-sm font-bold text-white leading-snug pr-12">{e.name}</h2>

              {/* Description */}
              {e.short_description && (
                <p className="text-xs text-zinc-500 leading-relaxed line-clamp-3">
                  {e.short_description}
                </p>
              )}

              {/* Airport tags */}
              {airports.length > 0 && (
                <div className="flex flex-wrap gap-1.5">
                  {airports.map((icao) => (
                    <span
                      key={icao}
                      className="text-[10px] font-mono font-bold tracking-wider px-2 py-0.5 rounded bg-white/[0.05] text-zinc-300 border border-white/[0.06]"
                    >
                      {icao}
                    </span>
                  ))}
                </div>
              )}

              {/* Time row */}
              <div className="mt-auto pt-2 border-t border-white/[0.06] flex items-center justify-between gap-2">
                <div>
                  <p className="text-[9px] tracking-widest uppercase text-zinc-600 mb-0.5">
                    {formatDate(e.start_time_utc)}
                  </p>
                  <p className="text-[10px] font-mono text-zinc-400">
                    {formatTime(e.start_time_utc)} → {formatTime(e.end_time_utc)}
                  </p>
                </div>
                {e.link_url && (
                  <a
                    href={e.link_url}
                    target="_blank"
                    rel="noopener noreferrer"
                    className="text-[9px] tracking-widest uppercase text-zinc-500 hover:text-zinc-200 border border-white/[0.08] hover:border-white/20 px-3 py-1 rounded-lg transition-colors whitespace-nowrap"
                  >
                    Details →
                  </a>
                )}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
