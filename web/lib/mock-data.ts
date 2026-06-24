// ---------------------------------------------------------------------------
// Mock data — swap these fetchers for real calls to the Aviation Hub API
// ---------------------------------------------------------------------------

export type SpicyAirport = {
  icao: string;
  name: string;
  country: string;
  controllers: number;
  traffic: number;
  weather: string;
  intensity: "extreme" | "high" | "moderate";
};

export type WeatherSummary = {
  icao: string;
  name: string;
  condition: string;
  temp: string;
  wind: string;
  visibility: string;
  qnh: string;
};

export type AtcController = {
  callsign: string;
  frequency: string;
  name: string;
  rating: string;
  online_since: string;
  facility: "DEL" | "GND" | "TWR" | "APP" | "CTR" | "FSS";
};

export type VatsimEvent = {
  id: string;
  name: string;
  start: string;
  end: string;
  airports: string[];
  type: string;
};

export type Booking = {
  id: string;
  callsign: string;
  position: string;
  start: string;
  end: string;
  cid: string;
  name: string;
};

// ---------------------------------------------------------------------------

export const MOCK_SPICY_AIRPORTS: SpicyAirport[] = [
  { icao: "EGLL", name: "London Heathrow", country: "GB", controllers: 8, traffic: 47, weather: "OVC 800ft", intensity: "extreme" },
  { icao: "EDDF", name: "Frankfurt Main", country: "DE", controllers: 6, traffic: 38, weather: "BKN 1200ft", intensity: "high" },
  { icao: "LFPG", name: "Paris CDG", country: "FR", controllers: 5, traffic: 31, weather: "FEW 2500ft", intensity: "high" },
  { icao: "EHAM", name: "Amsterdam Schiphol", country: "NL", controllers: 4, traffic: 29, weather: "SCT 1800ft", intensity: "high" },
  { icao: "LEMD", name: "Madrid Barajas", country: "ES", controllers: 3, traffic: 22, weather: "CAVOK", intensity: "moderate" },
  { icao: "LIRF", name: "Rome Fiumicino", country: "IT", controllers: 3, traffic: 19, weather: "FEW 3000ft", intensity: "moderate" },
];

export const MOCK_WEATHER: WeatherSummary[] = [
  { icao: "EGLL", name: "Heathrow", condition: "Overcast", temp: "8°C", wind: "250/18kt", visibility: "4000m", qnh: "1003" },
  { icao: "EDDF", name: "Frankfurt", condition: "Broken", temp: "5°C", wind: "200/12kt", visibility: "8000m", qnh: "1009" },
  { icao: "LFPG", name: "Paris CDG", condition: "Few clouds", temp: "11°C", wind: "180/08kt", visibility: "10km+", qnh: "1014" },
  { icao: "EHAM", name: "Schiphol", condition: "Scattered", temp: "7°C", wind: "270/15kt", visibility: "6000m", qnh: "1005" },
];

export const MOCK_ATC: AtcController[] = [
  { callsign: "EGLL_APP", frequency: "119.725", name: "Heathrow Approach", rating: "C1", online_since: "2h 14m", facility: "APP" },
  { callsign: "EGLL_TWR", frequency: "118.500", name: "Heathrow Tower", rating: "S3", online_since: "1h 47m", facility: "TWR" },
  { callsign: "EGLL_GND", frequency: "121.900", name: "Heathrow Ground", rating: "S2", online_since: "1h 47m", facility: "GND" },
  { callsign: "EGTT_CTR", frequency: "135.575", name: "London Control", rating: "C3", online_since: "3h 05m", facility: "CTR" },
  { callsign: "EDDF_APP", frequency: "120.800", name: "Frankfurt Approach", rating: "C1", online_since: "0h 52m", facility: "APP" },
  { callsign: "EDDF_TWR", frequency: "119.900", name: "Frankfurt Tower", rating: "S3", online_since: "0h 52m", facility: "TWR" },
  { callsign: "LFPG_APP", frequency: "125.600", name: "Paris Approach", rating: "C1", online_since: "1h 33m", facility: "APP" },
  { callsign: "EDGG_CTR", frequency: "134.130", name: "Langen Radar", rating: "C3", online_since: "2h 41m", facility: "CTR" },
];

export const MOCK_EVENTS: VatsimEvent[] = [
  {
    id: "1",
    name: "Friday Night Operations — London",
    start: "2026-04-11T18:00:00Z",
    end: "2026-04-11T22:00:00Z",
    airports: ["EGLL", "EGKK", "EGLC"],
    type: "FNO",
  },
  {
    id: "2",
    name: "Cross the Pond Westbound 2026",
    start: "2026-04-12T14:00:00Z",
    end: "2026-04-12T22:00:00Z",
    airports: ["EGLL", "EHAM", "LFPG", "KJFK", "KBOS"],
    type: "CTP",
  },
  {
    id: "3",
    name: "Euronight — Frankfurt Hub",
    start: "2026-04-13T19:00:00Z",
    end: "2026-04-13T23:00:00Z",
    airports: ["EDDF", "EDDK", "EDDM"],
    type: "EVENT",
  },
  {
    id: "4",
    name: "Iberia Fiesta — Madrid & Barcelona",
    start: "2026-04-19T16:00:00Z",
    end: "2026-04-19T21:00:00Z",
    airports: ["LEMD", "LEBL", "LEPA"],
    type: "EVENT",
  },
];

export const MOCK_BOOKINGS: Booking[] = [
  { id: "1", callsign: "EGLL_TWR", position: "Heathrow Tower", start: "18:00z", end: "22:00z", cid: "1234567", name: "James H." },
  { id: "2", callsign: "EGLL_APP", position: "Heathrow Approach", start: "18:00z", end: "22:00z", cid: "1234568", name: "Sarah K." },
  { id: "3", callsign: "EGTT_CTR", position: "London Control", start: "17:30z", end: "21:30z", cid: "1234569", name: "Mikael S." },
  { id: "4", callsign: "EDDF_TWR", position: "Frankfurt Tower", start: "18:00z", end: "21:00z", cid: "1234570", name: "Thomas B." },
  { id: "5", callsign: "LFPG_APP", position: "Paris Approach", start: "19:00z", end: "22:00z", cid: "1234571", name: "Claire D." },
  { id: "6", callsign: "EDGG_CTR", position: "Langen Radar", start: "16:00z", end: "20:00z", cid: "1234572", name: "Erik L." },
];

// Facility badge colours
export const FACILITY_COLORS: Record<AtcController["facility"], string> = {
  DEL: "bg-zinc-700 text-zinc-300",
  GND: "bg-zinc-600 text-zinc-200",
  TWR: "bg-red-900/60 text-red-300",
  APP: "bg-zinc-800 text-white",
  CTR: "bg-red-950/80 text-red-200",
  FSS: "bg-zinc-900 text-zinc-400",
};

export const INTENSITY_COLORS = {
  extreme: "text-red-400",
  high: "text-zinc-300",
  moderate: "text-zinc-500",
};
