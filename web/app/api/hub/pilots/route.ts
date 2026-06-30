import { NextResponse } from "next/server";

// Server-side cache to avoid hammering the VATSIM feed on every map refresh
let _cache: { data: unknown; ts: number } | null = null;
const CACHE_TTL = 55_000;

export async function GET() {
  const now = Date.now();
  if (_cache && now - _cache.ts < CACHE_TTL) {
    return NextResponse.json(_cache.data);
  }

  try {
    const res = await fetch("https://data.vatsim.net/v3/vatsim-data.json", {
      next: { revalidate: 55 },
    });
    if (!res.ok) {
      return NextResponse.json({ error: "vatsim_feed_error" }, { status: 502 });
    }
    const full = await res.json();

    // Return only the fields the map needs — keeps payload small (~80KB vs ~10MB)
    const pilots = (full.pilots ?? []).map((p: Record<string, unknown>) => {
      const fp = p.flight_plan as Record<string, unknown> | null | undefined;
      return {
        c:  p.callsign,               // callsign
        la: p.latitude,               // lat
        lo: p.longitude,              // lon
        h:  p.heading,                // heading (degrees true)
        a:  p.altitude,               // altitude (ft)
        s:  p.groundspeed,            // groundspeed (knots)
        t:  fp?.aircraft_short ?? "", // ICAO aircraft type (e.g. B738, A320)
        d:  fp?.departure  ?? "",     // departure airport ICAO
        r:  fp?.arrival    ?? "",     // arrival airport ICAO
      };
    });

    const data = {
      pilots,
      ts: (full.general as Record<string, unknown>)?.update_timestamp ?? new Date().toISOString(),
    };
    _cache = { data, ts: now };
    return NextResponse.json(data);
  } catch (err) {
    return NextResponse.json({ error: String(err) }, { status: 503 });
  }
}
