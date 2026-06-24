import { NextRequest, NextResponse } from "next/server";

const HUB_BASE = process.env.AVIATION_HUB_BASE_URL ?? "http://127.0.0.1:4010";

const DEFAULT_ICAOS = ["EGLL", "EDDF", "LFPG", "EHAM", "LEMD", "LIRF"];

/**
 * Aggregated endpoint: fetches airport/status for multiple ICAOs in parallel.
 * Query: ?icaos=EGLL,EDDF,LFPG  (optional — defaults to a fixed list)
 */
export async function GET(request: NextRequest) {
  const param = request.nextUrl.searchParams.get("icaos");
  const icaos = param ? param.split(",").map((s) => s.trim().toUpperCase()) : DEFAULT_ICAOS;

  try {
    const results = await Promise.all(
      icaos.map(async (icao) => {
        try {
          const data = await fetch(`${HUB_BASE}/api/airport/status?icao=${icao}`, {
            next: { revalidate: 0 },
          }).then((r) => r.json());
          return data;
        } catch {
          return null;
        }
      })
    );

    return NextResponse.json({
      generated_at: new Date().toISOString(),
      airports: results.filter(Boolean),
    });
  } catch (err) {
    return NextResponse.json({ error: "hub_unreachable", detail: String(err) }, { status: 503 });
  }
}
