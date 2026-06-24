import { NextResponse } from "next/server";

const HUB_BASE = process.env.AVIATION_HUB_BASE_URL ?? "http://127.0.0.1:4010";

export interface AtcLiveController {
  callsign: string;
  name: string;
  facility_label: string;
  frequency: string;
  logon_time: string;
  airport: string;
}

export interface AtcLiveResponse {
  generated_at: string;
  total_controllers: number;
  airports: {
    icao: string;
    controllers: AtcLiveController[];
  }[];
}

/**
 * Legacy aggregated endpoint. Now backed by the server-side coverage list so
 * callers get airports ordered by live coverage richness without N+1 fetches.
 */
export async function GET() {
  try {
    const coverage = await fetch(`${HUB_BASE}/api/vatsim/airports?limit=6&any_atc=1&sort=coverage`, {
      next: { revalidate: 0 },
    }).then((r) => r.json());

    const airportData: AtcLiveResponse["airports"] = (coverage.airports ?? []).map(
      (airport: {
        icao: string;
        controllers?: { callsign: string; name: string; facility_label: string; frequency: string; logon_time: string }[];
      }) => ({
        icao: airport.icao,
        controllers: (airport.controllers ?? []).map((c) => ({ ...c, airport: airport.icao })),
      })
    );

    const total = airportData.reduce((sum: number, airport) => sum + airport.controllers.length, 0);

    return NextResponse.json({
      generated_at: new Date().toISOString(),
      total_controllers: total,
      airports: airportData,
    } satisfies AtcLiveResponse);
  } catch (err) {
    return NextResponse.json({ error: "hub_unreachable", detail: String(err) }, { status: 503 });
  }
}
