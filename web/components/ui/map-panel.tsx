"use client";

import "maplibre-gl/dist/maplibre-gl.css";

import { useEffect, useRef } from "react";
import { motion } from "framer-motion";
import { cn } from "@/lib/utils";
import { useHub } from "@/lib/hooks/use-hub";
import type { MapAirportsResponse } from "@/lib/api-types";
import type { Map as MapLibreMap, GeoJSONSource, Popup as MapLibrePopup } from "maplibre-gl";
import {
  AIRCRAFT_CATEGORIES,
  buildAircraftImage,
  getAircraftCategory,
} from "@/lib/aircraft-icons";

interface MapPanelProps {
  className?: string;
  network?:   string;
}

// ── Traffic tier colours ──────────────────────────────────────────────────────
function trafficTier(airport: { manned: boolean; inbounds: number; controller_count: number }) {
  if (!airport.manned) return "inactive";
  const score = airport.inbounds + airport.controller_count * 5;
  if (score >= 35) return "high";
  if (score >= 12) return "medium";
  return "low";
}
const TIER_COLOR: Record<string, string> = {
  high: "#ef4444", medium: "#f97316", low: "#60a5fa", inactive: "#52525b",
};

// ── Dead-reckoning ─────────────────────────────────────────────────────────────
function deadReckon(lat: number, lon: number, heading: number, speedKt: number, seconds: number) {
  if (speedKt < 5) return { lat, lon };
  const nm = speedKt * (seconds / 3600);
  const R  = 3440.065;
  const d  = nm / R;
  const θ  = (heading * Math.PI) / 180;
  const φ1 = (lat  * Math.PI) / 180;
  const λ1 = (lon  * Math.PI) / 180;
  const φ2 = Math.asin(Math.sin(φ1) * Math.cos(d) + Math.cos(φ1) * Math.sin(d) * Math.cos(θ));
  const λ2 = λ1 + Math.atan2(Math.sin(θ) * Math.sin(d) * Math.cos(φ1), Math.cos(d) - Math.sin(φ1) * Math.sin(φ2));
  return { lat: (φ2 * 180) / Math.PI, lon: ((λ2 * 180) / Math.PI + 540) % 360 - 180 };
}

interface SlimPilot { c: string; la: number; lo: number; h: number; a: number; s: number; t: string }
interface PilotsResponse { pilots: SlimPilot[]; ts: string }
type Airport = MapAirportsResponse["airports"][number];

// ── Build airport GeoJSON ────────────────────────────────────────────────────
function buildAirportGeoJSON(airports: Airport[]) {
  const features = airports
    .filter((a) => a.lat && a.lon)
    .map((a) => {
      const tier  = trafficTier(a);
      const color = TIER_COLOR[tier];
      const radius = a.manned ? Math.min(4 + a.controller_count, 8) : 3;
      return {
        type: "Feature" as const,
        geometry: { type: "Point" as const, coordinates: [a.lon!, a.lat!] },
        properties: {
          color,
          radius,
          manned:    a.manned ? 1 : 0,
          icao:      a.airport,
          iata:      a.iata ?? "",
          name:      a.name ?? "",
          inbounds:  a.inbounds,
          atc:       a.controller_count,
        },
      };
    });
  return { type: "FeatureCollection" as const, features };
}

export function MapPanel({ className, network = "vatsim" }: MapPanelProps) {
  const containerRef  = useRef<HTMLDivElement>(null);
  const mapRef        = useRef<MapLibreMap | null>(null);
  const mapLoadedRef  = useRef(false);
  const pilotsRef     = useRef<(SlimPilot & { fetchedAt: number })[]>([]);
  const pilotTimerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const rafRef        = useRef<number | null>(null);
  const popupRef      = useRef<MapLibrePopup | null>(null);

  const { data: mapData } = useHub<MapAirportsResponse>(
    network === "ivao" ? "ivao-ranked" : "map-airports", { limit: "30" }, { interval: 60_000 }
  );

  // ── Fetch pilots every 60 s ───────────────────────────────────────────────
  useEffect(() => {
    let cancelled = false;
    const fetch60 = async () => {
      try {
        const res  = await fetch(network === "ivao" ? "/api/hub/ivao-pilots" : "/api/hub/pilots");
        const data: PilotsResponse = await res.json();
        if (!cancelled && data.pilots)
          pilotsRef.current = data.pilots.map((p) => ({ ...p, fetchedAt: Date.now() }));
      } catch { /* silent */ }
    };
    fetch60();
    const iv = setInterval(fetch60, 60_000);
    return () => { cancelled = true; clearInterval(iv); };
  }, [network]);

  // ── Init map ───────────────────────────────────────────────────────────────
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    import("maplibre-gl").then((mgl) => {
      if (!containerRef.current) return;

      const map = new mgl.Map({
        container: containerRef.current,
        style: {
          version: 8,
          glyphs: "https://demotiles.maplibre.org/font/{fontstack}/{range}.pbf",
          sources: {
            carto: {
              type: "raster",
              tiles: ["https://basemaps.cartocdn.com/dark_all/{z}/{x}/{y}@2x.png"],
              tileSize: 256,
              attribution: "© CARTO © OpenStreetMap",
              maxzoom: 19,
            },
          },
          layers: [{
            id: "carto-layer", type: "raster", source: "carto",
            paint: { "raster-saturation": -0.4, "raster-brightness-max": 0.65, "raster-contrast": 0.1 },
          }],
        },
        center: [10, 50],
        zoom: 3.5,
        attributionControl: false,
        pitchWithRotate: false,
      });

      mapRef.current = map;

      map.on("load", async () => {
        // ── Airport GeoJSON sources & layers ────────────────────────────────
        const emptyFC = { type: "FeatureCollection" as const, features: [] };

        map.addSource("airports", { type: "geojson", data: emptyFC });

        // Pulse ring A (0 phase)
        map.addLayer({
          id: "airport-ring-a", type: "circle", source: "airports",
          filter: ["==", ["get", "manned"], 1],
          paint: {
            "circle-radius":       ["get", "radius"],
            "circle-color":        "transparent",
            "circle-stroke-width": 1.5,
            "circle-stroke-color": ["get", "color"],
            "circle-stroke-opacity": 0,
            "circle-pitch-alignment": "viewport",
          },
        });

        // Pulse ring B (offset phase)
        map.addLayer({
          id: "airport-ring-b", type: "circle", source: "airports",
          filter: ["==", ["get", "manned"], 1],
          paint: {
            "circle-radius":       ["get", "radius"],
            "circle-color":        "transparent",
            "circle-stroke-width": 1.5,
            "circle-stroke-color": ["get", "color"],
            "circle-stroke-opacity": 0,
            "circle-pitch-alignment": "viewport",
          },
        });

        // Core dot
        map.addLayer({
          id: "airport-dots", type: "circle", source: "airports",
          paint: {
            "circle-radius":       ["get", "radius"],
            "circle-color":        ["get", "color"],
            "circle-opacity":      ["case", ["==", ["get", "manned"], 1], 1, 0.3],
            "circle-stroke-width": 0,
            "circle-pitch-alignment": "viewport",
          },
        });

        // Airport popup on click
        map.on("click", "airport-dots", (e) => {
          const f = e.features?.[0];
          if (!f) return;
          const p = f.properties as Record<string, unknown>;
          const color = String(p.color);
          popupRef.current?.remove();
          popupRef.current = new mgl.Popup({ offset: 12, closeButton: false, closeOnClick: true })
            .setLngLat(e.lngLat)
            .setHTML(`
              <div style="background:#0e0e0e;border:1px solid rgba(255,255,255,0.1);padding:10px 12px;border-radius:10px;min-width:150px;font-family:monospace">
                <div style="display:flex;align-items:center;gap:6px;margin-bottom:5px">
                  <span style="font-weight:700;font-size:12px;letter-spacing:0.14em;color:#fff">${p.icao}</span>
                  ${p.iata ? `<span style="font-size:9px;color:#52525b">${p.iata}</span>` : ""}
                  ${p.manned ? `<span style="width:6px;height:6px;border-radius:50%;background:${color};box-shadow:0 0 6px ${color};flex-shrink:0;display:inline-block"></span>` : ""}
                </div>
                <p style="font-size:9px;color:#71717a;margin:0 0 8px;line-height:1.4">${p.name}</p>
                <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px">
                  <div>
                    <p style="font-size:8px;color:#3f3f46;text-transform:uppercase;letter-spacing:0.12em;margin:0">Inbound</p>
                    <p style="font-size:16px;font-weight:700;color:${color};margin:2px 0 0">${p.inbounds}</p>
                  </div>
                  <div>
                    <p style="font-size:8px;color:#3f3f46;text-transform:uppercase;letter-spacing:0.12em;margin:0">ATC</p>
                    <p style="font-size:16px;font-weight:700;color:#d4d4d8;margin:2px 0 0">${p.atc}</p>
                  </div>
                </div>
              </div>
            `)
            .addTo(map);
        });
        map.on("mouseenter", "airport-dots", () => { map.getCanvas().style.cursor = "pointer"; });
        map.on("mouseleave", "airport-dots", () => { map.getCanvas().style.cursor = ""; });

        // ── Pilot GeoJSON source & layers ────────────────────────────────────
        map.addSource("pilots", { type: "geojson", data: emptyFC });

        await Promise.all(
          AIRCRAFT_CATEGORIES.map(async (cat) => {
            try {
              const img = await buildAircraftImage(cat, "rgba(148,163,184,0.92)", 32);
              if (!map.hasImage(`ac-${cat}`)) map.addImage(`ac-${cat}`, img, { sdf: false });
            } catch { /* skip */ }
          })
        );

        const iconExpr: unknown[] = ["match", ["get", "category"]];
        for (const cat of AIRCRAFT_CATEGORIES) iconExpr.push(cat, `ac-${cat}`);
        iconExpr.push("ac-narrowbody");

        map.addLayer({
          id: "pilots-circle", type: "circle", source: "pilots", maxzoom: 5,
          paint: {
            "circle-radius": 2.5,
            "circle-color": "rgba(148,163,184,0.7)",
            "circle-stroke-width": 0.4,
            "circle-stroke-color": "rgba(0,0,0,0.4)",
          },
        });

        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        map.addLayer({ id: "pilots-icon", type: "symbol", source: "pilots", minzoom: 5, layout: { "icon-image": iconExpr as any, "icon-rotate": ["get", "heading"], "icon-rotation-alignment": "map", "icon-allow-overlap": true, "icon-ignore-placement": true, "icon-size": ["interpolate", ["linear"], ["zoom"], 5, 0.7, 8, 1.0, 11, 1.4] } } as any); // eslint-disable-line @typescript-eslint/no-explicit-any

        map.on("click", "pilots-circle", (e) => showPilotPopup(map, mgl, e, popupRef));
        map.on("click", "pilots-icon",   (e) => showPilotPopup(map, mgl, e, popupRef));
        map.on("mouseenter", "pilots-circle", () => { map.getCanvas().style.cursor = "pointer"; });
        map.on("mouseleave", "pilots-circle", () => { map.getCanvas().style.cursor = ""; });
        map.on("mouseenter", "pilots-icon",   () => { map.getCanvas().style.cursor = "pointer"; });
        map.on("mouseleave", "pilots-icon",   () => { map.getCanvas().style.cursor = ""; });

        mapLoadedRef.current = true;

        // Draw airports if data already loaded
        if (mapData?.airports?.length) {
          const src = map.getSource("airports") as GeoJSONSource;
          src.setData(buildAirportGeoJSON(mapData.airports));
        }

        // Start pilot animation + pulse ring RAF loop
        startPilotAnimation(map);
        startPulseAnimation(map);
      });
    });

    return () => {
      if (pilotTimerRef.current) clearTimeout(pilotTimerRef.current);
      if (rafRef.current) cancelAnimationFrame(rafRef.current);
      mapRef.current?.remove();
      mapRef.current    = null;
      mapLoadedRef.current = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ── Update airport GeoJSON when data refreshes ───────────────────────────
  useEffect(() => {
    if (!mapLoadedRef.current || !mapData?.airports?.length || !mapRef.current) return;
    const src = mapRef.current.getSource("airports") as GeoJSONSource | undefined;
    src?.setData(buildAirportGeoJSON(mapData.airports));
  }, [mapData]);

  // ── Pilot dead-reckoning loop (setTimeout) ──────────────────────────────
  function startPilotAnimation(map: MapLibreMap) {
    const tick = () => {
      const now    = Date.now();
      const pilots = pilotsRef.current;
      if (pilots.length > 0) {
        const features = pilots.map((p) => {
          const elapsed = (now - p.fetchedAt) / 1000;
          const { lat, lon } = deadReckon(p.la, p.lo, p.h, p.s, elapsed);
          return {
            type: "Feature" as const,
            geometry: { type: "Point" as const, coordinates: [lon, lat] },
            properties: { heading: p.h, callsign: p.c, altitude: p.a, speed: p.s, type: p.t, category: getAircraftCategory(p.t ?? "") },
          };
        });
        const src = map.getSource("pilots") as GeoJSONSource | undefined;
        src?.setData({ type: "FeatureCollection", features });
      }
      pilotTimerRef.current = setTimeout(tick, 5_000);
    };
    tick();
  }

  // ── Pulse ring RAF animation ─────────────────────────────────────────────
  // Animates circle-radius on the two ring layers to create expanding pulse.
  // Completely GPU-side — markers never drift.
  function startPulseAnimation(map: MapLibreMap) {
    const PERIOD  = 2200; // ms per pulse cycle
    const MAX_MUL = 3.8;  // ring expands to 3.8× dot radius

    const tick = (ts: number) => {
      if (!map.getLayer("airport-ring-a")) { rafRef.current = requestAnimationFrame(tick); return; }

      const tA = (ts % PERIOD) / PERIOD;                       // 0→1
      const tB = ((ts + PERIOD * 0.45) % PERIOD) / PERIOD;    // offset phase

      const rA = (data: number) => data * (1 + tA * (MAX_MUL - 1));
      const rB = (data: number) => data * (1 + tB * (MAX_MUL - 1));

      // Use a fixed base radius of 5 for the expression target
      // Actual per-feature radius varies, but for the ring we drive a global multiplier
      // by animating paint on the layer level using a data expression + multiplier trick:
      // circle-radius = ["*", ["get","radius"], multiplier]
      try {
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        map.setPaintProperty("airport-ring-a", "circle-radius",        ["*", ["get", "radius"], rA(1)] as any);
        map.setPaintProperty("airport-ring-a", "circle-stroke-opacity", Math.max(0, 0.7 * (1 - tA)));
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        map.setPaintProperty("airport-ring-b", "circle-radius",        ["*", ["get", "radius"], rB(1)] as any);
        map.setPaintProperty("airport-ring-b", "circle-stroke-opacity", Math.max(0, 0.7 * (1 - tB)));
      } catch { /* map may be removing */ }

      rafRef.current = requestAnimationFrame(tick);
    };
    rafRef.current = requestAnimationFrame(tick);
  }

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ duration: 0.6, delay: 0.2 }}
      className={cn("relative rounded-xl overflow-hidden", className)}
    >
      <div ref={containerRef} className="absolute inset-0 w-full h-full" />

      <div className="absolute inset-0 pointer-events-none rounded-xl"
        style={{ boxShadow: "inset 0 0 60px rgba(0,0,0,0.7)" }} />
      <div className="absolute inset-0 rounded-xl border border-white/[0.08] pointer-events-none" />

      {/* Label */}
      <div className="absolute top-4 left-4 z-10 pointer-events-none">
        <div className="flex items-center gap-2 px-3 py-1.5 rounded bg-black/80 backdrop-blur-sm border border-white/[0.08]">
          <span className="accent-dot w-1.5 h-1.5 rounded-full bg-red-500 animate-pulse block flex-shrink-0 transition-[background-color] duration-500" />
          <span className="text-[9px] tracking-[0.22em] uppercase text-zinc-300 font-medium">{network === "ivao" ? "IVAO" : "VATSIM"} · Live Traffic</span>
        </div>
      </div>

      {/* Legend */}
      <div className="absolute top-4 right-4 z-10 pointer-events-none">
        <div className="flex flex-col gap-1.5 px-3 py-2 rounded bg-black/80 backdrop-blur-sm border border-white/[0.08]">
          {(["high", "medium", "low", "inactive"] as const).map((tier) => (
            <div key={tier} className="flex items-center gap-2">
              <div className="w-2 h-2 rounded-full flex-shrink-0"
                style={{ background: TIER_COLOR[tier], boxShadow: `0 0 5px ${TIER_COLOR[tier]}90` }} />
              <span className="text-[8px] uppercase tracking-widest text-zinc-500">
                {tier === "high" ? "Busy" : tier === "medium" ? "Active" : tier === "low" ? "Manned" : "Unmanned"}
              </span>
            </div>
          ))}
          <div className="flex items-center gap-2 mt-1 border-t border-white/[0.06] pt-1.5">
            <div className="w-2 h-2 rounded-full flex-shrink-0 bg-slate-400/80" />
            <span className="text-[8px] uppercase tracking-widest text-zinc-500">Aircraft</span>
          </div>
        </div>
      </div>

      {/* Count */}
      {mapData && (
        <div className="absolute bottom-4 left-4 z-10 pointer-events-none">
          <div className="px-2.5 py-1 rounded bg-black/80 backdrop-blur-sm border border-white/[0.06]">
            <span className="text-[9px] text-zinc-500 tracking-widest">
              {mapData.airports.filter((a) => a.manned).length} manned ·{" "}
              {mapData.airports.length} airports ·{" "}
              {pilotsRef.current.length > 0 ? `${pilotsRef.current.length} aircraft` : "loading aircraft…"}
            </span>
          </div>
        </div>
      )}

      {/* Corner accents */}
      <div className="absolute top-0 left-0 w-5 h-5 pointer-events-none">
        <div className="map-accent-line absolute top-0 left-0 w-full h-px bg-red-600/50 transition-[background-color] duration-500" />
        <div className="map-accent-line absolute top-0 left-0 w-px h-full bg-red-600/50 transition-[background-color] duration-500" />
      </div>
      <div className="absolute bottom-0 right-0 w-5 h-5 pointer-events-none">
        <div className="map-accent-line absolute bottom-0 right-0 w-full h-px bg-red-600/50 transition-[background-color] duration-500" />
        <div className="map-accent-line absolute bottom-0 right-0 w-px h-full bg-red-600/50 transition-[background-color] duration-500" />
      </div>
    </motion.div>
  );
}

// ── Pilot popup ───────────────────────────────────────────────────────────────
function showPilotPopup(
  map: MapLibreMap,
  mgl: typeof import("maplibre-gl"),
  e: { lngLat: { lng: number; lat: number }; features?: { properties: Record<string, unknown> }[] },
  popupRef: React.MutableRefObject<MapLibrePopup | null>,
) {
  const props = e.features?.[0]?.properties;
  if (!props) return;
  const alt     = Number(props.altitude ?? 0).toLocaleString();
  const acType  = props.type     ? String(props.type)     : "";
  const cat     = props.category ? String(props.category) : "";
  popupRef.current?.remove();
  popupRef.current = new mgl.Popup({ closeButton: true, closeOnClick: true })
    .setLngLat([e.lngLat.lng, e.lngLat.lat])
    .setHTML(`
      <div style="background:#0e0e0e;border:1px solid rgba(255,255,255,0.1);padding:10px 12px;border-radius:10px;font-family:monospace;min-width:150px">
        <div style="display:flex;align-items:baseline;gap:8px;margin-bottom:6px">
          <p style="font-weight:700;font-size:12px;letter-spacing:0.14em;color:#fff;margin:0">${props.callsign ?? "—"}</p>
          ${acType ? `<span style="font-size:9px;color:#52525b">${acType}</span>` : ""}
        </div>
        ${cat ? `<p style="font-size:8px;color:#3f3f46;text-transform:uppercase;letter-spacing:0.1em;margin:0 0 8px">${cat}</p>` : ""}
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:5px">
          <div><p style="font-size:8px;color:#3f3f46;text-transform:uppercase;margin:0">Alt</p><p style="font-size:12px;font-weight:700;color:#94a3b8;margin:2px 0 0">${alt} ft</p></div>
          <div><p style="font-size:8px;color:#3f3f46;text-transform:uppercase;margin:0">GS</p><p style="font-size:12px;font-weight:700;color:#94a3b8;margin:2px 0 0">${props.speed ?? 0} kt</p></div>
          <div><p style="font-size:8px;color:#3f3f46;text-transform:uppercase;margin:0">HDG</p><p style="font-size:12px;font-weight:700;color:#94a3b8;margin:2px 0 0">${props.heading ?? 0}°</p></div>
        </div>
      </div>
    `)
    .addTo(map);
}
