import { NextResponse } from "next/server";

const HUB_BASE = process.env.AVIATION_HUB_BASE_URL ?? "http://127.0.0.1:4010";

let _cache: { data: unknown; ts: number } | null = null;
const CACHE_TTL = 300_000; // 5 min — events don't change often

export async function GET() {
  const now = Date.now();
  if (_cache && now - _cache.ts < CACHE_TTL) {
    return NextResponse.json(_cache.data);
  }

  try {
    const res = await fetch(`${HUB_BASE}/api/vatsim/events`, {
      next: { revalidate: 300 },
    });
    if (!res.ok) return NextResponse.json({ error: "hub_unreachable" }, { status: 502 });
    const data = await res.json();
    _cache = { data, ts: now };
    return NextResponse.json(data);
  } catch (err) {
    return NextResponse.json({ error: String(err) }, { status: 503 });
  }
}
