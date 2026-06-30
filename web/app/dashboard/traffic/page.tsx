"use client";

import { useEffect, useState, useMemo, useCallback } from "react";
import { getAircraftCategory } from "@/lib/aircraft-icons";
import { useNetwork } from "@/lib/network-context";

interface Pilot {
  c: string;  // callsign
  la: number; // lat
  lo: number; // lon
  h: number;  // heading
  a: number;  // altitude
  s: number;  // groundspeed
  t: string;  // aircraft type
  d: string;  // departure
  r: string;  // arrival
}

type SortKey = "c" | "t" | "d" | "r" | "a" | "s";
type SortDir = "asc" | "desc";

const CAT_LABEL: Record<string, string> = {
  heavy:      "Heavy",
  widebody:   "Widebody",
  narrowbody: "Narrowbody",
  regional:   "Regional",
  bizjet:     "Bizjet",
  turboprop:  "Turboprop",
  light:      "Light",
  helicopter: "Helicopter",
  military:   "Military",
};

const CAT_COLOR: Record<string, string> = {
  heavy:      "#ef4444",
  widebody:   "#f97316",
  narrowbody: "#60a5fa",
  regional:   "#a78bfa",
  bizjet:     "#34d399",
  turboprop:  "#fbbf24",
  light:      "#94a3b8",
  helicopter: "#f472b6",
  military:   "#fb923c",
};

export default function TrafficPage() {
  const { network } = useNetwork();
  const [pilots, setPilots]     = useState<Pilot[]>([]);
  const [loading, setLoading]   = useState(true);
  const [lastUpdate, setLastUpdate] = useState<Date | null>(null);
  const [query, setQuery]       = useState("");
  const [sortKey, setSortKey]   = useState<SortKey>("c");
  const [sortDir, setSortDir]   = useState<SortDir>("asc");
  const [catFilter, setCatFilter] = useState<string>("all");

  const fetchPilots = useCallback(async () => {
    setLoading(true);
    try {
      const endpoint = network === "ivao" ? "/api/hub/ivao-pilots" : "/api/hub/pilots";
      const res  = await fetch(endpoint);
      const data = await res.json();
      if (data.pilots) {
        setPilots(data.pilots);
        setLastUpdate(new Date());
      }
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    fetchPilots();
    const iv = setInterval(fetchPilots, 60_000);
    return () => clearInterval(iv);
  }, [fetchPilots, network]);

  const filtered = useMemo(() => {
    const q = query.toLowerCase();
    return pilots
      .filter((p) => {
        if (catFilter !== "all" && getAircraftCategory(p.t) !== catFilter) return false;
        if (!q) return true;
        return (
          p.c.toLowerCase().includes(q) ||
          p.t.toLowerCase().includes(q) ||
          p.d.toLowerCase().includes(q) ||
          p.r.toLowerCase().includes(q)
        );
      })
      .sort((a, b) => {
        let av: string | number = a[sortKey] ?? "";
        let bv: string | number = b[sortKey] ?? "";
        if (sortKey === "a" || sortKey === "s") {
          av = Number(av); bv = Number(bv);
          return sortDir === "asc" ? (av as number) - (bv as number) : (bv as number) - (av as number);
        }
        av = String(av).toLowerCase();
        bv = String(bv).toLowerCase();
        return sortDir === "asc" ? av.localeCompare(bv) : bv.localeCompare(av);
      });
  }, [pilots, query, sortKey, sortDir, catFilter]);

  function toggleSort(key: SortKey) {
    if (sortKey === key) setSortDir((d) => (d === "asc" ? "desc" : "asc"));
    else { setSortKey(key); setSortDir("asc"); }
  }

  const SortIcon = ({ k }: { k: SortKey }) =>
    sortKey !== k ? (
      <span className="text-zinc-600 ml-1">↕</span>
    ) : (
      <span className="accent-arrow text-red-500 ml-1 transition-[color] duration-500">{sortDir === "asc" ? "↑" : "↓"}</span>
    );

  // Count by category for filter pills
  const catCounts = useMemo(() => {
    const counts: Record<string, number> = {};
    for (const p of pilots) {
      const cat = getAircraftCategory(p.t);
      counts[cat] = (counts[cat] ?? 0) + 1;
    }
    return counts;
  }, [pilots]);

  const categories = Object.keys(CAT_LABEL).filter((c) => (catCounts[c] ?? 0) > 0);

  return (
    <div className="px-4 md:px-8 py-6 max-w-[1600px] mx-auto">

      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-3 mb-6">
        <div>
          <div className="flex items-center gap-2 mb-1">
            <span className="accent-dot w-2 h-2 rounded-full bg-red-500 animate-pulse block transition-[background-color] duration-500" />
            <h1 className="text-xl font-bold tracking-tight text-white">Live Traffic · <span style={{ color: "var(--accent-text)" }}>{network.toUpperCase()}</span></h1>
          </div>
          <p className="text-xs text-zinc-500">
            {loading ? "Loading…" : `${pilots.length.toLocaleString()} aircraft online`}
            {lastUpdate && (
              <span className="ml-2 text-zinc-600">
                · updated {lastUpdate.toLocaleTimeString()}
              </span>
            )}
          </p>
        </div>

        {/* Search */}
        <input
          type="text"
          placeholder="Search callsign, type, airport…"
          value={query}
          onChange={(e) => setQuery(e.target.value)}
          className="accent-search w-full sm:w-72 px-3 py-2 rounded-lg bg-zinc-900 border border-white/[0.08] text-sm text-zinc-200 placeholder:text-zinc-600 focus:outline-none focus:border-red-500/50 transition-[border-color] duration-500"
        />
      </div>

      {/* Category filter pills */}
      <div className="flex flex-wrap gap-2 mb-5">
        <button
          onClick={() => setCatFilter("all")}
          className={`px-3 py-1 rounded-full text-[10px] tracking-widest uppercase font-medium border transition-colors ${
            catFilter === "all"
              ? "accent-pill-active bg-red-600/20 border-red-500/50 text-red-400"
              : "border-white/[0.08] text-zinc-500 hover:text-zinc-300"
          }`}
        >
          All · {pilots.length}
        </button>
        {categories.map((cat) => (
          <button
            key={cat}
            onClick={() => setCatFilter(cat === catFilter ? "all" : cat)}
            className={`px-3 py-1 rounded-full text-[10px] tracking-widest uppercase font-medium border transition-colors ${
              catFilter === cat
                ? "border-current"
                : "border-white/[0.06] text-zinc-500 hover:text-zinc-300"
            }`}
            style={catFilter === cat ? { color: CAT_COLOR[cat], borderColor: `${CAT_COLOR[cat]}60`, background: `${CAT_COLOR[cat]}15` } : {}}
          >
            {CAT_LABEL[cat]} · {catCounts[cat] ?? 0}
          </button>
        ))}
      </div>

      {/* Table */}
      <div className="rounded-xl border border-white/[0.08] overflow-hidden">
        <div className="overflow-x-auto">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-white/[0.08] bg-white/[0.02]">
                {(
                  [
                    { key: "c", label: "Callsign" },
                    { key: "t", label: "Type" },
                    { key: "d", label: "DEP" },
                    { key: "r", label: "ARR" },
                    { key: "a", label: "Altitude" },
                    { key: "s", label: "GS (kt)" },
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
                <th className="px-4 py-3 text-left text-[10px] tracking-widest uppercase text-zinc-500">Category</th>
                <th className="px-4 py-3 text-left text-[10px] tracking-widest uppercase text-zinc-500">HDG</th>
              </tr>
            </thead>
            <tbody>
              {loading && (
                <tr>
                  <td colSpan={8} className="px-4 py-12 text-center text-zinc-600 text-xs tracking-widest uppercase">
                    Loading live traffic…
                  </td>
                </tr>
              )}
              {!loading && filtered.length === 0 && (
                <tr>
                  <td colSpan={8} className="px-4 py-12 text-center text-zinc-600 text-xs tracking-widest uppercase">
                    No aircraft match your filter
                  </td>
                </tr>
              )}
              {filtered.map((p, i) => {
                const cat   = getAircraftCategory(p.t);
                const color = CAT_COLOR[cat] ?? "#94a3b8";
                return (
                  <tr
                    key={p.c}
                    className={`border-b border-white/[0.04] hover:bg-white/[0.03] transition-colors ${
                      i % 2 === 0 ? "" : "bg-white/[0.015]"
                    }`}
                  >
                    <td className="px-4 py-2.5 font-mono font-bold text-white tracking-wider text-xs">
                      {p.c}
                    </td>
                    <td className="px-4 py-2.5 font-mono text-xs text-zinc-300">
                      {p.t || <span className="text-zinc-600">—</span>}
                    </td>
                    <td className="px-4 py-2.5 font-mono text-xs text-zinc-400">
                      {p.d || <span className="text-zinc-600">—</span>}
                    </td>
                    <td className="px-4 py-2.5 font-mono text-xs text-zinc-400">
                      {p.r || <span className="text-zinc-600">—</span>}
                    </td>
                    <td className="px-4 py-2.5 font-mono text-xs text-zinc-300">
                      {p.a > 0 ? `${p.a.toLocaleString()} ft` : <span className="text-zinc-600">GND</span>}
                    </td>
                    <td className="px-4 py-2.5 font-mono text-xs text-zinc-300">
                      {p.s > 0 ? p.s : <span className="text-zinc-600">0</span>}
                    </td>
                    <td className="px-4 py-2.5">
                      <span
                        className="text-[9px] tracking-widest uppercase font-medium px-2 py-0.5 rounded-full"
                        style={{ color, background: `${color}18`, border: `1px solid ${color}40` }}
                      >
                        {CAT_LABEL[cat]}
                      </span>
                    </td>
                    <td className="px-4 py-2.5 font-mono text-xs text-zinc-600">
                      {p.h}°
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </div>

        {/* Footer count */}
        {!loading && filtered.length > 0 && (
          <div className="px-4 py-2.5 border-t border-white/[0.06] bg-white/[0.01]">
            <p className="text-[10px] text-zinc-600 tracking-widest uppercase">
              Showing {filtered.length.toLocaleString()} of {pilots.length.toLocaleString()} aircraft · refreshes every 60 s
            </p>
          </div>
        )}
      </div>
    </div>
  );
}
