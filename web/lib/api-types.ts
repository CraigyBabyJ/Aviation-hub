// ---------------------------------------------------------------------------
// Types matching the Aviation Hub widget server responses (port 4010)
// ---------------------------------------------------------------------------

export interface AirportCoverage {
  hasDel: boolean;
  hasGnd: boolean;
  hasTwr: boolean;
  hasApp: boolean;
  hasCtr: boolean;
  hasAtis: boolean;
  hasAnyAtc: boolean;
  hasGroundOps: boolean;
  hasRadar: boolean;
  fullCoverage: boolean;
}

export interface RankedAirport {
  airport: string;
  manned: boolean;
  controller_count: number;
  has_atis: boolean;
  coverage: AirportCoverage;
  coverage_score: number;
  inbounds: number;
  departures: number;
  upcoming_bookings: number;
  upcoming_events: number;
  upcoming_score: number;
  overall_score: number;
  challenge_level: "easy" | "moderate" | "hard" | "extreme";
  country: string;
  rank_score: number;
}

export interface RankedResponse {
  generated_at: string;
  count: number;
  airports: RankedAirport[];
}

export interface SpicyWidget {
  generated_at: string;
  airliner: SpicyAirportDetail;
  ga: SpicyAirportDetail;
}

export interface SpicyAirportDetail {
  airport: string;
  name: string;
  country: string;
  region: string;
  overall_score: number;
  challenge_level: "easy" | "moderate" | "hard" | "extreme";
  has_snow: number;
  has_thunderstorm: number;
  is_gusty: number;
  is_low_visibility: number;
  is_low_ceiling: number;
  wind_gust_kt: number;
  visibility_meters: number;
  flight_category: string;
  primary_condition: string;
  spicy_rank: number;
}

export interface VatsimController {
  callsign: string;
  name: string;
  facility: number;
  facility_label: "DEL" | "GND" | "TWR" | "APP" | "DEP" | "CTR" | "FSS";
  frequency: string;
  rating: number;
  server: string;
  logon_time: string;
  last_updated: string;
}

export interface AtisEntry {
  callsign: string;
  atis_code: string;
  frequency: string;
  text: string;
  last_updated: string;
}

export interface VatsimAirportResponse {
  icao: string;
  source: string;
  controller_count: number;
  controllers: VatsimController[];
  atis: AtisEntry[];
  has_atis: boolean;
  coverage: AirportCoverage;
  coverage_score: number;
  name: string | null;
  country: string | null;
  region: string | null;
  latitude_deg: number | null;
  longitude_deg: number | null;
  fetched_at: string;
}

export interface VatsimAirportListItem {
  icao: string;
  name: string | null;
  country: string | null;
  region: string | null;
  latitude_deg: number | null;
  longitude_deg: number | null;
  controller_count: number;
  controllers: VatsimController[];
  atis: AtisEntry[];
  coverage: AirportCoverage;
  coverage_score: number;
}

export interface VatsimAirportsResponse {
  generated_at: string;
  source: string;
  sort: "coverage" | "icao";
  limit: number;
  include_empty: boolean;
  filters: Partial<AirportCoverage>;
  count: number;
  airports: VatsimAirportListItem[];
}

export interface RouteEndpointAirport {
  icao: string;
  name: string | null;
  country: string | null;
  controller_count: number;
  coverage: AirportCoverage;
}

export interface SuggestedRoute {
  departure: RouteEndpointAirport;
  arrival: RouteEndpointAirport;
  distance_nm: number;
  estimated_minutes: number;
  time_delta_minutes: number;
  route_score: number;
}

export interface VatsimRoutesResponse {
  generated_at: string;
  region: "uk" | "europe" | "us" | "south_usa";
  region_label: string;
  target_minutes: number;
  limit: number;
  count: number;
  assumptions: {
    requires_live_atc_at_both_airports: boolean;
    estimated_cruise_speed_kt: number;
    block_buffer_minutes: number;
  };
  routes: SuggestedRoute[];
}

export interface VatsimEvent {
  event_id: string;
  name: string;
  event_type: string;
  start_time_utc: string;
  end_time_utc: string;
  short_description: string;
  link_url: string;
  airports_json: string; // JSON string like "[\"EGLL\"]"
}

export interface EventsResponse {
  generated_at: string;
  count: number;
  events: VatsimEvent[];
}

export interface Booking {
  booking_id: string;
  callsign: string;
  airport_icao: string | null;
  fir_icao: string | null;
  position_type: "DEL" | "GND" | "TWR" | "APP" | "DEP" | "CTR" | "FSS";
  starts_at_utc: string;
  ends_at_utc: string;
  booking_type: "booking" | "training" | "exam";
  controller_cid: string;
}

export interface BookingsResponse {
  generated_at: string;
  count: number;
  bookings: Booking[];
}

export interface Sigmet {
  id: string;
  fir: string;
  fir_name: string;
  hazard: string;
  qualifier: string;
  base_ft: number | null;
  top_ft: number | null;
  movement_dir: number | null;
  movement_speed: number;
  valid_from: string;
  valid_to: string;
  raw_text: string;
}

export interface SigmetsResponse {
  generated_at: string;
  count: number;
  sigmets: Sigmet[];
}

export interface MapAirport {
  airport: string;
  manned: boolean;
  controller_count: number;
  inbounds: number;
  challenge_level: string;
  lat: number;
  lon: number;
  name: string;
  iata: string;
}

export interface MapAirportsResponse {
  generated_at: string;
  airports: MapAirport[];
}

export interface AirportStatus {
  airport: string;
  controller_count: number;
  has_atc: boolean;
  has_atis: boolean;
  atis_callsign: string;
  atis_frequency: string;
  overall_score: number;
  challenge_level: string;
  flight_category: string | null;
  wx_summary: string;
  raw_metar: string;
  raw_taf: string;
  controllers: VatsimController[];
}
