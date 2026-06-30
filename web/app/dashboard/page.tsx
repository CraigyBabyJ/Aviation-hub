"use client";

import { SpicyAirports } from "@/components/dashboard/spicy-airports";
import { WeatherPanel } from "@/components/dashboard/weather-panel";
import { AtcPanel } from "@/components/dashboard/atc-panel";
import { EventsPanel } from "@/components/dashboard/events-panel";
import { BookingsPanel } from "@/components/dashboard/bookings-panel";
import { SigmetBanner } from "@/components/dashboard/sigmet-banner";
import { MapPanel } from "@/components/ui/map-panel";
import { StatCard } from "@/components/ui/stat-card";
import { NetworkToggle } from "@/components/ui/network-toggle";
import { useHub } from "@/lib/hooks/use-hub";
import { useNetwork } from "@/lib/network-context";
import type { RankedResponse, EventsResponse, BookingsResponse } from "@/lib/api-types";
import { motion } from "framer-motion";
import Link from "next/link";

function StatsBar({ network }: { network: string }) {
  const rankedEndpoint = network === "ivao" ? "ivao-ranked" : "airports/ranked";
  const { data: ranked } = useHub<RankedResponse>(rankedEndpoint, { limit: "20" }, { interval: 30_000 });
  const { data: events }   = useHub<EventsResponse>("vatsim/events", { days: "1", limit: "50" }, { interval: 120_000 });
  const { data: bookings } = useHub<BookingsResponse>("vatsim/bookings", { limit: "50" }, { interval: 120_000 });

  const totalControllers = ranked?.airports.reduce((s, a) => s + a.controller_count, 0) ?? null;
  const totalInbounds    = ranked?.airports.reduce((s, a) => s + a.inbounds, 0) ?? null;
  const busiest          = ranked?.airports[0];

  return (
    <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 mb-4">
      <StatCard
        label="Active Controllers"
        value={totalControllers !== null ? totalControllers.toString() : "—"}
        subtext={`Top 20 airports · ${network.toUpperCase()}`}
        trend="up"
      />
      <StatCard
        label="Aircraft Inbound"
        value={totalInbounds !== null ? totalInbounds.toString() : "—"}
        subtext="Across ranked airports"
      />
      <StatCard
        label="Events (24h)"
        value={network === "ivao" ? "N/A" : (events?.count !== undefined ? events.count.toString() : "—")}
        subtext={network === "ivao" ? "See Events panel for IVAO" : `${bookings?.count ?? "—"} bookings upcoming`}
      />
      <StatCard
        label="Busiest Airport"
        value={busiest?.airport ?? "—"}
        subtext={busiest ? `${busiest.inbounds} inbound · ${busiest.controller_count} ATC` : "Loading…"}
        accent
      />
    </div>
  );
}

export default function DashboardPage() {
  const { network } = useNetwork();
  const rankedEndpoint = network === "ivao" ? "ivao-ranked" : "airports/ranked";

  const { data: ranked, loading: rankedLoading, error: rankedError, lastUpdated: rankedUpdated, refresh: rankedRefresh } =
    useHub<RankedResponse>(rankedEndpoint, { limit: "10" }, { interval: 30_000 });
  const rankedIcaos = ranked?.airports.map((a) => a.airport) ?? [];

  return (
    <div className="max-w-[1600px] mx-auto px-4 sm:px-6 py-6">

      {/* Page header */}
      <motion.div
        initial={{ opacity: 0, y: -6 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.4 }}
        className="grid grid-cols-1 sm:grid-cols-[1fr_auto_1fr] items-end gap-4 mb-5"
      >
        <div>
          <p className="text-[9px] tracking-[0.28em] uppercase text-zinc-700 mb-1">Operations Console</p>
          <h1 className="text-2xl font-bold tracking-[0.12em] uppercase text-white leading-none">
            Aviation Hub
          </h1>
          <p className="text-[11px] text-zinc-500 mt-1.5 leading-snug max-w-xs">
            Live flight sim traffic, ATC coverage &amp; events for VATSIM and IVAO.
          </p>
        </div>
        <div className="justify-self-start sm:justify-self-center flex flex-col items-center gap-3">
          <NetworkToggle />
          <Link
            href="/discord-bot"
            className="rounded-lg px-7 py-2 text-sm font-bold uppercase tracking-[0.22em] transition-all duration-500 hover:text-white"
            style={{
              border:     "1px solid rgba(var(--accent),0.40)",
              background: "rgba(var(--accent),0.10)",
              color:      "var(--accent-text)",
            }}
          >
            Add Discord Bot
          </Link>
        </div>
        <div className="text-right hidden sm:block">
          <p className="text-[9px] tracking-widest uppercase text-zinc-700">Network</p>
          <p className={`text-[10px] tracking-wide font-bold ${network === "ivao" ? "text-blue-400" : "text-red-400"}`}>
            {network.toUpperCase()} · Live
          </p>
        </div>
      </motion.div>

      {/* ── SIGMET alert banner ───────────────────────────────── */}
      <SigmetBanner />

      {/* ── Quick stats ──────────────────────────────────────── */}
      <StatsBar network={network} />

      {/* ── Row 1: Ranked airports + Weather ─────────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 mb-4">
        <div className="min-h-[420px] flex flex-col">
          <SpicyAirports data={ranked ?? undefined} loading={rankedLoading} error={rankedError ?? null} lastUpdated={rankedUpdated} refresh={rankedRefresh} />
        </div>
        <div className="min-h-[420px] flex flex-col">
          <WeatherPanel icaos={rankedIcaos} />
        </div>
      </div>

      {/* ── Row 2: Map + ATC ─────────────────────────────────── */}
      <div className="grid grid-cols-1 xl:grid-cols-[1fr_360px] gap-4 mb-4">
        <MapPanel className="h-[520px]" network={network} />
        <div className="h-[520px]">
          <AtcPanel network={network} />
        </div>
      </div>

      {/* ── Row 3: Events + Bookings ─────────────────────────── */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-4 pb-8">
        <div className="min-h-[380px] flex flex-col">
          <EventsPanel network={network} />
        </div>
        <div className="min-h-[380px] flex flex-col">
          <BookingsPanel network={network} />
        </div>
      </div>

    </div>
  );
}
