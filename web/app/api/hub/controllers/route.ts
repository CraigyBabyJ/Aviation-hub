import { NextResponse } from "next/server";

let _cache: { data: unknown; ts: number } | null = null;
const CACHE_TTL = 55_000;

const FACILITY_LABEL: Record<number, string> = {
  0: "OBS",
  1: "FSS",
  2: "DEL",
  3: "GND",
  4: "TWR",
  5: "APP",
  6: "CTR",
  7: "ATIS",
};

const RATING_LABEL: Record<number, string> = {
  1: "OBS", 2: "S1", 3: "S2", 4: "S3",
  5: "C1",  7: "C3", 8: "I1", 10: "I3",
  11: "SUP", 12: "ADM",
};

function logonMinutes(logon_time: string): number {
  return Math.floor((Date.now() - new Date(logon_time).getTime()) / 60_000);
}

export async function GET() {
  const now = Date.now();
  if (_cache && now - _cache.ts < CACHE_TTL) {
    return NextResponse.json(_cache.data);
  }

  try {
    const res = await fetch("https://data.vatsim.net/v3/vatsim-data.json", {
      next: { revalidate: 55 },
    });
    if (!res.ok) return NextResponse.json({ error: "vatsim_feed_error" }, { status: 502 });

    const full = await res.json();

    const controllers = (full.controllers ?? [])
      .filter((c: Record<string, unknown>) => Number(c.facility) !== 0) // exclude observers
      .map((c: Record<string, unknown>) => {
        const facility = Number(c.facility);
        const callsign = String(c.callsign ?? "");
        // Extract airport ICAO from callsign prefix (e.g. EGLL_TWR → EGLL)
        const airport  = callsign.split("_")[0] ?? "";
        return {
          callsign,
          name:      c.name     ?? "",
          cid:       c.cid      ?? "",
          frequency: c.frequency ?? "",
          facility,
          facility_label: FACILITY_LABEL[facility] ?? "—",
          rating:         Number(c.rating ?? 0),
          rating_label:   RATING_LABEL[Number(c.rating ?? 0)] ?? "—",
          airport,
          logon_time:     c.logon_time ?? "",
          logon_minutes:  logonMinutes(String(c.logon_time ?? "")),
          visual_range:   c.visual_range ?? 0,
          atis:           Array.isArray(c.text_atis) ? (c.text_atis as string[]).join(" ") : "",
        };
      })
      // Sort by facility desc (CTR first), then callsign
      .sort((a: { facility: number; callsign: string }, b: { facility: number; callsign: string }) =>
        b.facility - a.facility || a.callsign.localeCompare(b.callsign)
      );

    const data = {
      controllers,
      total: controllers.length,
      ts:    (full.general as Record<string, unknown>)?.update_timestamp ?? new Date().toISOString(),
    };
    _cache = { data, ts: now };
    return NextResponse.json(data);
  } catch (err) {
    return NextResponse.json({ error: String(err) }, { status: 503 });
  }
}
