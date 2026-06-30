import { NextResponse } from "next/server";

const HUB_BASE = process.env.AVIATION_HUB_BASE_URL ?? "http://127.0.0.1:4010";

export async function GET(req: Request) {
  const icao = new URL(req.url).searchParams.get("icao") ?? "";
  const url  = icao
    ? `${HUB_BASE}/api/ivao/bookings?icao=${encodeURIComponent(icao.toUpperCase())}`
    : `${HUB_BASE}/api/ivao/bookings`;
  try {
    const res = await fetch(url, { next: { revalidate: 300 } });
    if (!res.ok) return NextResponse.json({ error: "hub_error" }, { status: 502 });
    return NextResponse.json(await res.json());
  } catch (err) {
    return NextResponse.json({ error: String(err) }, { status: 503 });
  }
}
