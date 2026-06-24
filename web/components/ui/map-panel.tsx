"use client";

// MapLibre CSS must be imported statically — without it the map canvas is invisible
import "maplibre-gl/dist/maplibre-gl.css";

import { useEffect, useRef } from "react";
import { motion } from "framer-motion";
import { cn } from "@/lib/utils";
import { useHub } from "@/lib/hooks/use-hub";
import type { MapAirportsResponse } from "@/lib/api-types";

// maplibre-gl types — imported lazily to keep SSR safe
import type { Map as MapLibreMap, Marker as MapLibreMarker } from "maplibre-gl";

interface MapPanelProps {
  className?: string;
}

const CHALLENGE_COLOR: Record<string, string> = {
  extreme: "#ef4444",
  hard:    "#a1a1aa",
  moderate:"#71717a",
  easy:    "#3f3f46",
};

export function MapPanel({ className }: MapPanelProps) {
  const containerRef  = useRef<HTMLDivElement>(null);
  const mapRef        = useRef<MapLibreMap | null>(null);
  const mapLoadedRef  = useRef(false);
  const markersRef    = useRef<MapLibreMarker[]>([]);

  const { data: mapData } = useHub<MapAirportsResponse>(
    "map-airports",
    { limit: "30" },
    { interval: 60_000 }
  );

  // ── Initialise map once ──────────────────────────────────────────────────
  useEffect(() => {
    if (!containerRef.current || mapRef.current) return;

    let map: MapLibreMap;

    import("maplibre-gl").then((mgl) => {
      if (!containerRef.current) return;

      map = new mgl.Map({
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
          layers: [
            {
              id: "carto-layer",
              type: "raster",
              source: "carto",
              paint: {
                "raster-saturation":    -0.4,
                "raster-brightness-max": 0.65,
                "raster-contrast":       0.1,
              },
            },
          ],
        },
        center: [10, 50],
        zoom:   4,
        attributionControl: false,
        pitchWithRotate:    false,
      });

      mapRef.current = map;

      map.on("load", () => {
        mapLoadedRef.current = true;
        // If data already arrived before the map finished loading, draw now
        if (mapData?.airports?.length) {
          drawMarkers(map, mgl, mapData.airports);
        }
      });
    });

    return () => {
      markersRef.current.forEach((m) => m.remove());
      markersRef.current = [];
      mapRef.current?.remove();
      mapRef.current   = null;
      mapLoadedRef.current = false;
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, []);

  // ── Re-draw markers whenever data refreshes ──────────────────────────────
  useEffect(() => {
    if (!mapLoadedRef.current || !mapData?.airports?.length) return;

    import("maplibre-gl").then((mgl) => {
      if (!mapRef.current) return;
      drawMarkers(mapRef.current, mgl, mapData.airports);
    });
  }, [mapData]);

  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ duration: 0.6, delay: 0.2 }}
      className={cn("relative rounded-xl overflow-hidden", className)}
    >
      {/* Map canvas */}
      <div ref={containerRef} className="absolute inset-0 w-full h-full" />

      {/* Inner vignette */}
      <div className="absolute inset-0 pointer-events-none rounded-xl"
        style={{ boxShadow: "inset 0 0 60px rgba(0,0,0,0.7)" }} />

      {/* Border */}
      <div className="absolute inset-0 rounded-xl border border-white/[0.08] pointer-events-none" />

      {/* Top-left label */}
      <div className="absolute top-4 left-4 z-10 pointer-events-none">
        <div className="flex items-center gap-2 px-3 py-1.5 rounded bg-black/80 backdrop-blur-sm border border-white/[0.08]">
          <span className="live-dot w-1.5 h-1.5 rounded-full bg-red-500 block flex-shrink-0" />
          <span className="text-[9px] tracking-[0.22em] uppercase text-zinc-300 font-medium">
            VATSIM · Live Traffic
          </span>
        </div>
      </div>

      {/* Legend */}
      <div className="absolute top-4 right-4 z-10 pointer-events-none">
        <div className="flex flex-col gap-1.5 px-3 py-2 rounded bg-black/80 backdrop-blur-sm border border-white/[0.08]">
          {(["extreme", "hard", "moderate"] as const).map((lvl) => (
            <div key={lvl} className="flex items-center gap-2">
              <div
                className="w-2 h-2 rounded-full flex-shrink-0"
                style={{ background: CHALLENGE_COLOR[lvl], boxShadow: `0 0 6px ${CHALLENGE_COLOR[lvl]}80` }}
              />
              <span className="text-[8px] uppercase tracking-widest text-zinc-500 capitalize">{lvl}</span>
            </div>
          ))}
        </div>
      </div>

      {/* Manned count */}
      {mapData && (
        <div className="absolute bottom-4 left-4 z-10 pointer-events-none">
          <div className="px-2.5 py-1 rounded bg-black/80 backdrop-blur-sm border border-white/[0.06]">
            <span className="text-[9px] text-zinc-500 tracking-widest">
              {mapData.airports.filter((a) => a.manned).length} manned ·{" "}
              {mapData.airports.length} ranked
            </span>
          </div>
        </div>
      )}

      {/* Corner accents */}
      <div className="absolute top-0 left-0 w-5 h-5 pointer-events-none">
        <div className="absolute top-0 left-0 w-full h-px bg-red-600/50" />
        <div className="absolute top-0 left-0 w-px h-full bg-red-600/50" />
      </div>
      <div className="absolute bottom-0 right-0 w-5 h-5 pointer-events-none">
        <div className="absolute bottom-0 right-0 w-full h-px bg-red-600/50" />
        <div className="absolute bottom-0 right-0 w-px h-full bg-red-600/50" />
      </div>
    </motion.div>
  );
}

// ── Marker drawing (outside component to keep effect bodies clean) ──────────
type Airport = MapAirportsResponse["airports"][number];

function drawMarkers(
  map: MapLibreMap,
  mgl: typeof import("maplibre-gl"),
  airports: Airport[],
) {
  // Remove stale markers via the module-level ref — workaround: store on map instance
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  const stored: MapLibreMarker[] = (map as any).__avhub_markers ?? [];
  stored.forEach((m) => m.remove());

  const fresh: MapLibreMarker[] = [];

  airports.forEach((airport) => {
    if (!airport.lat || !airport.lon) return;

    const color  = CHALLENGE_COLOR[airport.challenge_level] ?? CHALLENGE_COLOR.easy;
    const manned = airport.manned;
    const size   = manned ? Math.min(8 + airport.controller_count * 2, 18) : 6;

    const el = document.createElement("div");
    el.style.cssText = [
      `width:${size}px`,
      `height:${size}px`,
      "border-radius:50%",
      `background:${color}`,
      `border:1.5px solid ${manned ? color : "rgba(255,255,255,0.1)"}`,
      manned ? `box-shadow:0 0 ${size}px ${color}60` : "",
      `opacity:${manned ? 1 : 0.35}`,
      "cursor:pointer",
      "transition:transform 0.15s ease,opacity 0.15s ease",
    ].join(";");

    el.addEventListener("mouseenter", () => {
      el.style.transform = "scale(1.8)";
      el.style.opacity   = "1";
    });
    el.addEventListener("mouseleave", () => {
      el.style.transform = "scale(1)";
      el.style.opacity   = manned ? "1" : "0.35";
    });

    const popup = new mgl.Popup({
      offset: 12,
      closeButton: false,
      closeOnClick: true,
    }).setHTML(`
      <div style="background:#0e0e0e;border:1px solid rgba(255,255,255,0.1);padding:10px 12px;border-radius:10px;min-width:150px;font-family:monospace">
        <div style="display:flex;align-items:center;gap:6px;margin-bottom:5px">
          <span style="font-weight:700;font-size:12px;letter-spacing:0.14em;color:#fff">${airport.airport}</span>
          ${airport.iata ? `<span style="font-size:9px;color:#52525b">${airport.iata}</span>` : ""}
          ${manned ? `<span style="width:6px;height:6px;border-radius:50%;background:${color};box-shadow:0 0 6px ${color};flex-shrink:0;display:inline-block"></span>` : ""}
        </div>
        <p style="font-size:9px;color:#71717a;margin:0 0 8px;line-height:1.4">${airport.name ?? ""}</p>
        <div style="display:grid;grid-template-columns:1fr 1fr;gap:6px">
          <div>
            <p style="font-size:8px;color:#3f3f46;text-transform:uppercase;letter-spacing:0.12em;margin:0">Inbound</p>
            <p style="font-size:16px;font-weight:700;color:${color};margin:2px 0 0">${airport.inbounds}</p>
          </div>
          <div>
            <p style="font-size:8px;color:#3f3f46;text-transform:uppercase;letter-spacing:0.12em;margin:0">ATC</p>
            <p style="font-size:16px;font-weight:700;color:#d4d4d8;margin:2px 0 0">${airport.controller_count}</p>
          </div>
        </div>
      </div>
    `);

    const marker = new mgl.Marker({ element: el })
      .setLngLat([airport.lon, airport.lat])
      .setPopup(popup)
      .addTo(map);

    fresh.push(marker);
  });

  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  (map as any).__avhub_markers = fresh;
}
