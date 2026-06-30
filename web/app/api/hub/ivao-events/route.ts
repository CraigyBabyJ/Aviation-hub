import { NextResponse } from "next/server";

const HUB_BASE = process.env.AVIATION_HUB_BASE_URL ?? "http://127.0.0.1:4010";

export async function GET() {
  try {
    const res = await fetch(`${HUB_BASE}/api/ivao/events?limit=30`, { next: { revalidate: 300 } });
    if (!res.ok) return NextResponse.json({ error: "hub_error" }, { status: 502 });
    return NextResponse.json(await res.json());
  } catch (err) {
    return NextResponse.json({ error: String(err) }, { status: 503 });
  }
}
