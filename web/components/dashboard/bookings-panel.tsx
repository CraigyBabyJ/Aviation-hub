"use client";

import { Clock, GraduationCap, BookOpen } from "lucide-react";
import { HubPanel } from "@/components/ui/hub-panel";
import { SectionHeader } from "@/components/ui/section-header";
import { LiveStatus, Skeleton } from "@/components/ui/live-status";
import { useHub } from "@/lib/hooks/use-hub";
import { cn } from "@/lib/utils";
import type { BookingsResponse, Booking } from "@/lib/api-types";

const POSITION_COLORS: Record<string, string> = {
  CTR: "text-red-400 border-red-600/30 bg-red-950/20",
  APP: "text-zinc-300 border-white/10 bg-white/[0.03]",
  DEP: "text-zinc-300 border-white/10 bg-white/[0.03]",
  TWR: "text-zinc-400 border-white/[0.07] bg-white/[0.02]",
  GND: "text-zinc-500 border-white/[0.05] bg-transparent",
  DEL: "text-zinc-600 border-white/[0.04] bg-transparent",
  FSS: "text-zinc-600 border-white/[0.04] bg-transparent",
};

function formatSlotTime(iso: string) {
  return new Date(iso).toLocaleTimeString("en-GB", {
    hour: "2-digit", minute: "2-digit", timeZone: "UTC", hour12: false,
  }) + "z";
}

function BookingTypeBadge({ type }: { type: Booking["booking_type"] }) {
  if (type === "training") return <GraduationCap size={9} className="text-zinc-600" aria-label="Training" />;
  if (type === "exam") return <BookOpen size={9} className="text-zinc-500" aria-label="Exam" />;
  return null;
}

function isActive(booking: Booking) {
  const now = Date.now();
  return new Date(booking.starts_at_utc).getTime() <= now && new Date(booking.ends_at_utc).getTime() >= now;
}

function BookingRow({ booking }: { booking: Booking }) {
  const active = isActive(booking);
  const icao = booking.airport_icao ?? booking.fir_icao ?? "—";

  return (
    <div className={cn("hub-row grid grid-cols-[auto_1fr_auto] gap-3 py-2.5 items-center", active && "bg-red-950/5")}>
      {/* Position type badge */}
      <span className={cn("text-[9px] font-bold tracking-widest px-1.5 py-0.5 rounded border w-8 text-center flex-shrink-0", POSITION_COLORS[booking.position_type] ?? POSITION_COLORS.GND)}>
        {booking.position_type}
      </span>

      {/* Callsign + airport */}
      <div className="min-w-0">
        <div className="flex items-center gap-2 mb-0.5">
          <span className="text-[11px] font-semibold text-white truncate">{booking.callsign}</span>
          {active && <span className="live-dot w-1 h-1 rounded-full bg-red-500 flex-shrink-0" />}
          <BookingTypeBadge type={booking.booking_type} />
        </div>
        <p className="text-[9px] text-zinc-700">{icao}</p>
      </div>

      {/* Time slot */}
      <div className="flex items-center gap-1 flex-shrink-0">
        <Clock size={9} className="text-zinc-700" />
        <span className="text-[10px] text-zinc-500 tabular-nums whitespace-nowrap">
          {formatSlotTime(booking.starts_at_utc)} – {formatSlotTime(booking.ends_at_utc)}
        </span>
      </div>
    </div>
  );
}

export function BookingsPanel() {
  const { data, loading, error, lastUpdated, refresh } = useHub<BookingsResponse>(
    "vatsim/bookings",
    { limit: "20" },
    { interval: 120_000 }
  );

  const bookings = data?.bookings ?? [];

  return (
    <HubPanel delay={0.3} className="flex flex-col h-full">
      <div className="flex items-start justify-between gap-2 mb-4">
        <SectionHeader
          title="ATC Bookings"
          subtitle="Upcoming slot reservations"
          count={data?.count}
          className="mb-0"
        />
        <LiveStatus loading={loading} error={error} lastUpdated={lastUpdated} onRefresh={refresh} />
      </div>

      {/* Column labels */}
      <div className="grid grid-cols-[auto_1fr_auto] gap-3 pb-2 mb-1 border-b border-white/[0.04]">
        <span className="text-[8px] w-8 tracking-[0.2em] uppercase text-zinc-700">TYPE</span>
        <span className="text-[8px] tracking-[0.2em] uppercase text-zinc-700">POSITION</span>
        <span className="text-[8px] tracking-[0.2em] uppercase text-zinc-700 text-right">SLOT (UTC)</span>
      </div>

      <div className="flex-1 overflow-y-auto -mx-5 px-5">
        {loading && !bookings.length
          ? Array.from({ length: 6 }).map((_, i) => (
              <div key={i} className="hub-row py-3 grid grid-cols-[auto_1fr_auto] gap-3">
                <Skeleton className="w-8 h-5" />
                <Skeleton className="h-4" />
                <Skeleton className="w-24 h-4" />
              </div>
            ))
          : bookings.map((b) => <BookingRow key={b.booking_id} booking={b} />)}
        {!loading && !bookings.length && (
          <p className="text-center text-[11px] text-zinc-700 py-8 tracking-widest uppercase">No bookings</p>
        )}
      </div>
    </HubPanel>
  );
}
