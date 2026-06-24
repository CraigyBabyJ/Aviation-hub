import { NextRequest, NextResponse } from "next/server";

const HUB_BASE = process.env.AVIATION_HUB_BASE_URL ?? "http://127.0.0.1:4010";

/**
 * Generic proxy to the Aviation Hub widget server.
 * GET /api/hub/vatsim/events?days=14 → proxies to http://127.0.0.1:4010/api/vatsim/events?days=14
 * GET /api/hub/airports/ranked?limit=20 → proxies to http://127.0.0.1:4010/api/airports/ranked?limit=20
 *
 * Special: /api/hub/map-airports returns ranked airports enriched with lat/lon from /api/station
 */
export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ path: string[] }> }
) {
  const { path } = await params;
  const pathStr = path.join("/");
  const searchParams = request.nextUrl.searchParams;

  // Special enriched endpoint for the map
  if (pathStr === "map-airports") {
    return handleMapAirports(searchParams);
  }

  const hubUrl = new URL(`/api/${pathStr}`, HUB_BASE);
  searchParams.forEach((v, k) => hubUrl.searchParams.set(k, v));

  try {
    const res = await fetch(hubUrl.toString(), {
      next: { revalidate: 0 }, // always fresh
    });
    const contentType = res.headers.get("content-type") ?? "application/json";

    // Binary responses (satellite images)
    if (contentType.includes("image/")) {
      const buf = await res.arrayBuffer();
      return new NextResponse(buf, {
        status: res.status,
        headers: { "content-type": contentType },
      });
    }

    const data = await res.json();
    return NextResponse.json(data, { status: res.status });
  } catch (err) {
    return NextResponse.json(
      { error: "hub_unreachable", detail: String(err) },
      { status: 503 }
    );
  }
}

async function handleMapAirports(params: URLSearchParams) {
  const limit = params.get("limit") ?? "30";
  try {
    const ranked = await fetch(
      `${HUB_BASE}/api/airports/ranked?limit=${limit}`,
      { next: { revalidate: 0 } }
    ).then((r) => r.json());

    const airports: { airport: string; controller_count: number; inbounds: number; manned: boolean; challenge_level: string }[] =
      ranked.airports ?? [];

    // Fetch station info (lat/lon) for each airport in parallel
    const enriched = await Promise.all(
      airports.map(async (a) => {
        try {
          const station = await fetch(
            `${HUB_BASE}/api/station?icao=${a.airport}`,
            { next: { revalidate: 300 } } // station info stable for 5 min
          ).then((r) => r.json());
          return {
            ...a,
            lat: station.latitude_deg as number,
            lon: station.longitude_deg as number,
            name: station.name as string,
            iata: station.iata as string,
          };
        } catch {
          return null;
        }
      })
    );

    return NextResponse.json({
      airports: enriched.filter(Boolean),
      generated_at: ranked.generated_at,
    });
  } catch (err) {
    return NextResponse.json(
      { error: "hub_unreachable", detail: String(err) },
      { status: 503 }
    );
  }
}
