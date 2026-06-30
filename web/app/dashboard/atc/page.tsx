"use client";

import { useEffect, useState, useMemo, useCallback } from "react";
import { useNetwork } from "@/lib/network-context";

interface Controller {
  callsign:       string;
  name:           string;
  cid:            string | number;
  frequency:      string;
  facility:       number;
  facility_label: string;
  rating:         number;
  rating_label:   string;
  airport:        string;
  logon_time:     string;
  logon_minutes:  number;
  visual_range:   number;
  atis:           string;
}

type SortKey = "callsign" | "airport" | "facility" | "frequency" | "rating" | "logon_minutes";
type SortDir = "asc" | "desc";

const FACILITY_COLOR: Record<string, string> = {
  CTR:  "#ef4444",
  APP:  "#f97316",
  TWR:  "#fbbf24",
  GND:  "#34d399",
  DEL:  "#60a5fa",
  ATIS: "#a78bfa",
  FSS:  "#f472b6",
  OBS:  "#52525b",
};

const FACILITY_ORDER = ["CTR", "APP", "TWR", "GND", "DEL", "ATIS", "FSS"];

function formatLogon(mins: number): string {
  if (mins < 60) return `${mins}m`;
  const h = Math.floor(mins / 60);
  const m = mins % 60;
  return m > 0 ? `${h}h ${m}m` : `${h}h`;
}

export default function AtcPage() {
  const { network } = useNetwork();
  const [controllers, setControllers] = useState<Controller[]>([]);
  const [loading, setLoading]         = useState(true);
  const [lastUpdate, setLastUpdate]   = useState<Date | null>(null);
  const [query, setQuery]             = useState("");
  const [sortKey, setSortKey]         = useState<SortKey>("facility");
  const [sortDir, setSortDir]         = useState<SortDir>("desc");
  const [facilityFilter, setFacilityFilter] = useState("all");
  const [expandedAtis, setExpandedAtis]     = useState<string | null>(null);

  const fetchControllers = useCallback(async () => {
    setLoading(true);
    try {
      const endpoint = network === "ivao" ? "/api/hub/ivao-controllers" : "/api/hub/controllers";
      const res  = await fetch(endpoint);
      const data = await res.json();
      if (data.controllers) {
        setControllers(data.controllers);
        setLastUpdate(new Date());
      }
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchControllers();
    const iv = setInterval(fetchControllers, 60_000);
    return () => clearInterval(iv);
  }, [fetchControllers, network]);

  const filtered = useMemo(() => {
    const q = query.toLowerCase();
    return controllers
      .filter((c) => {
        if (facilityFilter !== "all" && c.facility_label !== facilityFilter) return false;
        if (!q) return true;
        return (
          c.callsign.toLowerCase().includes(q) ||
          c.airport.toLowerCase().includes(q) ||
          c.name.toLowerCase().includes(q) ||
          c.frequency.includes(q)
        );
      })
      .sort((a, b) => {
        let av: string | number = a[sortKey];
        let bv: string | number = b[sortKey];
        if (typeof av === "number" && typeof bv === "number") {
          return sortDir === "asc" ? av - bv : bv - av;
        }
        av = String(av).toLowerCase();
        bv = String(bv).toLowerCase();
        return sortDir === "asc" ? av.localeCompare(bv) : bv.localeCompare(av);
      });
  }, [controllers, query, sortKey, sortDir, facilityFilter]);

  function toggleSort(key: SortKey) {
    if (sortKey === key) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    else { setSortKey(key); setSortDir(key === "facility" || key === "logon_minutes" ? "desc" : "asc"); }
  }

  const SortIcon = ({ k }: { k: SortKey }) =>
    sortKey !== k ? (
      <span className="text-zinc-600 ml-1">↕</span>
    ) : (
      <span className="accent-arrow text-red-500 ml-1 transition-[color] duration-500">{sortDir === "asc" ? "↑" : "↓"}</span>
    );

  const facilityCounts = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const c of controllers) {
      counts[c.facility_label] = (counts[c.facility_label] ?? 0) + 1;
    }
    return counts;
  }, [controllers]);

  // Summary stats
  const stats = useMemo(() => ({
    ctr:  controllers.filter((c) => c.facility_label === "CTR").length,
    app:  controllers.filter((c) => c.facility_label === "APP").length,
    twr:  controllers.filter((c) => c.facility_label === "TWR").length,
    gnd:  controllers.filter((c) => c.facility_label === "GND").length,
  }), [controllers]);

  return (
    <div className="px-4 md:px-8 py-6 max-w-[1600px] mx-auto">

      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 mb-6">
        <div>
          <div className="flex items-center gap-2 mb-1">
            <span className="accent-dot w-2 h-2 rounded-full bg-red-500 animate-pulse block transition-[background-color] duration-500" />
            <h1 className="text-xl font-bold tracking-tight text-white">Live ATC · <span style={{ color: "var(--accent-text)" }}>{network.toUpperCase()}</span></h1>
          </div>
          <p className="text-xs text-zinc-500">
            {loading ? "Loading…" : `${controllers.length} controllers online`}
            {lastUpdate && (
              <span className="ml-2 text-zinc-600">· updated {lastUpdate.toLocaleTimeString()}</span>
            )}
          </p>
        </div>
        <input
          type="text"
          placeholder="Search callsign, airport, name…"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          className="accent-search w-full sm:w-72 px-3 py-2 rounded-lg bg-zinc-900 border border-white/[0.08] text-sm text-zinc-200 placeholder:text-zinc-600 focus:outline-none focus:border-red-500/50 transition-[border-color] duration-500"
        />
      </div>

      {/* Summary stat pills */}
      {!loading && (
        <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-6">
          {[
            { label: "Centre", value: stats.ctr, color: "#ef4444" },
            { label: "Approach", value: stats.app, color: "#f97316" },
            { label: "Tower", value: stats.twr, color: "#fbbf24" },
            { label: "Ground", value: stats.gnd, color: "#34d399" },
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

      {/* Facility filter pills */}
      <div className="flex flex-wrap gap-2 mb-5">
        <button
          onClick={() => setFacilityFilter("all")}
          className={`px-3 py-1 rounded-full text-[10px] tracking-widest uppercase font-medium border transition-colors ${
            facilityFilter === "all"
              ? "accent-pill-active bg-red-600/20 border-red-500/50 text-red-400"
              : "border-white/[0.08] text-zinc-500 hover:text-zinc-300"
          }`}
        >
          All · {controllers.length}
        </button>
        {FACILITY_ORDER.filter((f) => (facilityCounts[f] ?? 0) > 0).map((f) => {
          const color = FACILITY_COLOR[f] ?? "#94a3b8";
          const active = facilityFilter === f;
          return (
            <button
              key={f}
              onClick={() => setFacilityFilter(active ? "all" : f)}
              className="px-3 py-1 rounded-full text-[10px] tracking-widest uppercase font-medium border transition-colors"
              style={active
                ? { color, borderColor: `${color}60`, background: `${color}18` }
                : { color: "#71717a", borderColor: "rgba(255,255,255,0.06)" }}
            >
              {f} · {facilityCounts[f] ?? 0}
            </button>
          );
        })}
      </div>

      {/* Table */}
      <div className="rounded-xl border border-white/[0.08] overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-white/[0.08] bg-white/[0.02]">
                {(
                  [
                    { key: "callsign",      label: "Callsign" },
                    { key: "airport",       label: "Airport" },
                    { key: "facility",      label: "Position" },
                    { key: "frequency",     label: "Freq" },
                    { key: "rating",        label: "Rating" },
                    { key: "logon_minutes", label: "Online" },
                  ] as { key: SortKey; label: string }[]
                ).map(({ key, label }) => (
                  <th
                    key={key}
                    onClick={() => toggleSort(key)}
                    className="px-4 py-3 text-left text-[10px] tracking-widest uppercase text-zinc-500 cursor-pointer hover:text-zinc-300 select-none whitespace-nowrap"
                  >
                    {label}
                    <SortIcon k={key} />
                  </th>
                ))}
                <th className="px-4 py-3 text-left text-[10px] tracking-widest uppercase text-zinc-500">Controller</th>
                <th className="px-4 py-3 text-left text-[10px] tracking-widest uppercase text-zinc-500">ATIS</th>
              </tr>
            </thead>
            <tbody>
              {loading && (
                <tr>
                  <td colSpan={8} className="px-4 py-12 text-center text-zinc-600 text-xs tracking-widest uppercase">
                    Loading live ATC…
                  </td>
                </tr>
              )}
              {!loading && filtered.length === 0 && (
                <tr>
                  <td colSpan={8} className="px-4 py-12 text-center text-zinc-600 text-xs tracking-widest uppercase">
                    No controllers match your filter
                  </td>
                </tr>
              )}
              {filtered.map((c, i) => {
                const color = FACILITY_COLOR[c.facility_label] ?? "#94a3b8";
                const hasAtis = !!c.atis;
                const atisOpen = expandedAtis === c.callsign;
                return (
                  <>
                    <tr
                      key={c.callsign}
                      className={`border-b border-white/[0.04] hover:bg-white/[0.03] transition-colors ${
                        i % 2 === 0 ? "" : "bg-white/[0.015]"
                      }`}
                    >
                      <td className="px-4 py-2.5 font-mono font-bold text-white tracking-wider text-xs">
                        {c.callsign}
                      </td>
                      <td className="px-4 py-2.5 font-mono text-xs text-zinc-300">
                        {c.airport || <span className="text-zinc-600">—</span>}
                      </td>
                      <td className="px-4 py-2.5">
                        <span
                          className="text-[9px] tracking-widest uppercase font-medium px-2 py-0.5 rounded-full"
                          style={{ color, background: `${color}18`, border: `1px solid ${color}40` }}
                        >
                          {c.facility_label}
                        </span>
                      </td>
                      <td className="px-4 py-2.5 font-mono text-xs text-zinc-300 tabular-nums">
                        {c.frequency}
                      </td>
                      <td className="px-4 py-2.5">
                        <span className="text-[9px] tracking-widest uppercase font-medium text-zinc-400 px-2 py-0.5 rounded bg-white/[0.05]">
                          {c.rating_label}
                        </span>
                      </td>
                      <td className="px-4 py-2.5 font-mono text-xs text-zinc-400 tabular-nums">
                        {formatLogon(c.logon_minutes)}
                      </td>
                      <td className="px-4 py-2.5 text-xs text-zinc-400 max-w-[160px] truncate">
                        {c.name || <span className="text-zinc-600">—</span>}
                      </td>
                      <td className="px-4 py-2.5">
                        {hasAtis ? (
                          <button
                            onClick={() => setExpandedAtis(atisOpen ? null : c.callsign)}
                            className="text-[9px] tracking-widest uppercase text-zinc-500 hover:text-zinc-200 border border-white/[0.08] hover:border-white/20 px-2 py-0.5 rounded transition-colors"
                          >
                            {atisOpen ? "Hide" : "ATIS"}
                          </button>
                        ) : (
                          <span className="text-zinc-700 text-[10px]">—</span>
                        )}
                      </td>
                    </tr>
                    {atisOpen && hasAtis && (
                      <tr key={`${c.callsign}-atis`} className="border-b border-white/[0.04] bg-zinc-900/60">
                        <td colSpan={8} className="px-6 py-3">
                          <p className="text-[10px] tracking-widest uppercase text-zinc-600 mb-1">ATIS · {c.callsign}</p>
                          <p className="text-xs text-zinc-300 font-mono leading-relaxed">{c.atis}</p>
                        </td>
                      </tr>
                    )}
                  </>
                );
              })}
            </tbody>
          </table>
        </div>

        {!loading && filtered.length > 0 && (
          <div className="px-4 py-2.5 border-t border-white/[0.06] bg-white/[0.01]">
            <p className="text-[10px] text-zinc-600 tracking-widest uppercase">
              Showing {filtered.length} of {controllers.length} controllers · refreshes every 60 s
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
