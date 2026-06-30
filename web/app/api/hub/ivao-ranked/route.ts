import { NextResponse } from "next/server";

const HUB_BASE = process.env.AVIATION_HUB_BASE_URL ?? "http://127.0.0.1:4010";

export async function GET(req: Request) {
  const limit = Number(new URL(req.url).searchParams.get("limit") ?? "10");

  try {
    // Read controllers and pilots from DB (updated every 60s by the fetcher — no live IVAO API call)
    const [ctlRes, pilotRes] = await Promise.all([
      fetch(`${HUB_BASE}/api/ivao/controllers`, { next: { revalidate: 55 } }),
      fetch(`${HUB_BASE}/api/ivao/pilots`,      { next: { revalidate: 55 } }),
    ]);
    if (!ctlRes.ok || !pilotRes.ok) return NextResponse.json({ error: "hub_error" }, { status: 502 });

    const { controllers } = await ctlRes.json() as { controllers: { callsign: string; name: string; facility_label: string; airport: string }[] };
    const { pilots }      = await pilotRes.json() as { pilots: { c: string; r: string }[] };

    // Group controllers by airport
    type CtlInfo = { controllers: { callsign: string; name: string; facility: string }[]; };
    const map = new Map<string, CtlInfo>();
    for (const c of controllers) {
      const icao = c.airport;
      if (!icao || icao.length < 3) continue;
      if (!map.has(icao)) map.set(icao, { controllers: [] });
      map.get(icao)!.controllers.push({ callsign: c.callsign, name: c.name, facility: c.facility_label });
    }

    // Count inbounds per airport
    const inboundMap = new Map<string, number>();
    for (const p of pilots) {
      if (p.r) inboundMap.set(p.r, (inboundMap.get(p.r) ?? 0) + 1);
    }

    const airports = [...map.entries()]
      .sort((a, b) => b[1].controllers.length - a[1].controllers.length)
      .slice(0, limit)
      .map(([icao, info]) => ({
        airport:          icao,
        iata:             "",
        name:             "",
        country:          "",
        manned:           info.controllers.length > 0,
        controller_count: info.controllers.length,
        controllers:      info.controllers,
        inbounds:         inboundMap.get(icao) ?? 0,
        challenge_level:  "easy" as string,
        overall_score:    0,
        flight_category:  null as string | null,
        wx_summary:       "",
        lat:              0,
        lon:              0,
      }));

    const icaoList = airports.map(a => a.airport).join(",");

    // Enrich with weather + station info in parallel (best effort, hub backend only)
    const [weatherRes] = await Promise.allSettled([
      fetch(`${HUB_BASE}/api/airports/weather?icaos=${icaoList}`).then(r => r.json()),
    ]);
    const weatherByAp: Record<string, { overall_score: number; challenge_level: string; flight_category: string | null; wx_summary: string }> =
      weatherRes.status === "fulfilled" ? (weatherRes.value?.airports ?? {}) : {};

    await Promise.all(airports.map(async (a) => {
      const wx = weatherByAp[a.airport];
      if (wx) {
        (a as Record<string, unknown>).challenge_level = wx.challenge_level ?? "easy";
        (a as Record<string, unknown>).overall_score   = wx.overall_score ?? 0;
        (a as Record<string, unknown>).flight_category = wx.flight_category ?? null;
        (a as Record<string, unknown>).wx_summary      = wx.wx_summary ?? "";
      }
      try {
        const s = await fetch(`${HUB_BASE}/api/station?icao=${a.airport}`, { next: { revalidate: 3600 } }).then(r => r.json());
        if (s?.name)          a.name    = s.name;
        if (s?.country)       a.country = s.country;
        if (s?.iata)          a.iata    = s.iata;
        if (s?.latitude_deg)  a.lat     = s.latitude_deg;
        if (s?.longitude_deg) a.lon     = s.longitude_deg;
      } catch { /* best effort */ }
    }));

    return NextResponse.json({ airports, total: airports.length, ts: new Date().toISOString() });
  } catch (err) {
    return NextResponse.json({ error: String(err) }, { status: 503 });
  }
}
