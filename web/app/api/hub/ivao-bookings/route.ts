import { NextResponse } from "next/server";
import fs from "fs";
import path from "path";

const CLIENT_ID    = "c69d816a-4157-4fdc-92bc-3eefed92937f";
const TOKEN_FILE   = path.resolve("/home/craig/projects/Aviation-hub/secrets/ivao_tokens.json");
const TOKEN_URL    = "https://api.ivao.aero/v2/oauth/token";
const BOOKINGS_URL = "https://api.ivao.aero/v2/atc/bookings/daily";

let _cache: { data: unknown; ts: number } | null = null;
const CACHE_TTL = 300_000; // 5 min

interface TokenStore {
  refresh_token: string;
  access_token:  string;
  expires_at:    number; // unix ms
}

function readTokens(): TokenStore {
  try {
    return JSON.parse(fs.readFileSync(TOKEN_FILE, "utf-8"));
  } catch {
    return { refresh_token: "", access_token: "", expires_at: 0 };
  }
}

function writeTokens(t: TokenStore) {
  fs.writeFileSync(TOKEN_FILE, JSON.stringify(t, null, 2), { mode: 0o600 });
}

async function getAccessToken(): Promise<string> {
  const store = readTokens();
  // Use cached access token if still valid (with 60s buffer)
  if (store.access_token && store.expires_at > Date.now() + 60_000) {
    return store.access_token;
  }
  if (!store.refresh_token) throw new Error("No IVAO refresh token stored");

  const res  = await fetch(TOKEN_URL, {
    method:  "POST",
    headers: { "Content-Type": "application/x-www-form-urlencoded" },
    body:    new URLSearchParams({
      grant_type:    "refresh_token",
      refresh_token: store.refresh_token,
      client_id:     CLIENT_ID,
    }),
  });
  if (!res.ok) throw new Error(`Token refresh failed: ${res.status}`);
  const data = await res.json() as { access_token: string; refresh_token: string; expires_in: number };

  writeTokens({
    access_token:  data.access_token,
    refresh_token: data.refresh_token, // rotating — always store the new one
    expires_at:    Date.now() + data.expires_in * 1000,
  });
  return data.access_token;
}

const IVAO_RATING: Record<number, string> = {
  1: "AS1", 2: "AS2", 3: "AS3", 4: "ADC",
  5: "APC", 6: "ACC", 7: "SEC", 8: "SAI", 9: "CAI",
};

export async function GET() {
  const now = Date.now();
  if (_cache && now - _cache.ts < CACHE_TTL) return NextResponse.json(_cache.data);

  try {
    const token = await getAccessToken();

    // Fetch today + next 2 days
    const dates = [0, 1, 2].map((d) => {
      const dt = new Date(now + d * 86_400_000);
      return dt.toISOString().slice(0, 10);
    });

    const results = await Promise.all(
      dates.map((date) =>
        fetch(`${BOOKINGS_URL}?date=${date}`, {
          headers: { Authorization: `Bearer ${token}` },
        }).then((r) => (r.ok ? r.json() : []))
      )
    );

    const seen = new Set<number>();
    const bookings = results.flat().filter((b: { id: number }) => {
      if (seen.has(b.id)) return false;
      seen.add(b.id);
      return true;
    }).map((b: {
      id: number; atcPosition: string; startDate: string; endDate: string;
      voice: boolean; training: string | null;
      atcPositionRef?: { airportId: string; frequency: number; atcCallsign: string };
      user?: { id: number; divisionId: string; firstName: string; lastName: string; rating?: { atcRatingId: number } };
    }) => ({
      id:          b.id,
      callsign:    b.atcPosition,
      airport:     b.atcPositionRef?.airportId ?? (b.atcPosition ?? "").split("_")[0],
      frequency:   b.atcPositionRef?.frequency ?? 0,
      position:    b.atcPositionRef?.atcCallsign ?? "",
      starts_at:   b.startDate,
      ends_at:     b.endDate,
      training:    b.training,
      voice:       b.voice,
      controller:  b.user ? `${b.user.firstName} ${b.user.lastName}` : "",
      rating:      IVAO_RATING[b.user?.rating?.atcRatingId ?? 0] ?? "—",
      division:    b.user?.divisionId ?? "",
    })).sort((a, b) => new Date(a.starts_at).getTime() - new Date(b.starts_at).getTime());

    const data = { bookings, count: bookings.length, ts: new Date().toISOString() };
    _cache = { data, ts: now };
    return NextResponse.json(data);
  } catch (err) {
    return NextResponse.json({ error: String(err) }, { status: 503 });
  }
}
