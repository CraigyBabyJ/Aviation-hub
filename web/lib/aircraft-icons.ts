/**
 * Top-down aircraft silhouette SVGs — tar1090-style overhead view.
 * All icons are 24×24 viewBox, designed to be rotated by heading on the map.
 * The nose points UP (north) at heading 0.
 */

export type AircraftCategory =
  | "heavy"      // widebody: A380, B744, B77W, A350, B789 …
  | "widebody"   // medium widebody: B763, A332, A333 …
  | "narrowbody" // single-aisle jets: B738, A320, A321 …
  | "regional"   // regional jets: E175, CRJ9, E145 …
  | "bizjet"     // business jets: C56X, GL5T, PC24 …
  | "turboprop"  // turboprops: AT72, DH8D, C208 …
  | "light"      // GA / light: C172, PA28, SR22 …
  | "helicopter" // rotorcraft: R44, EC35, H135 …
  | "military";  // fighters/military: F16, F18, B52 …

// ── SVG path data (24×24, nose pointing up) ───────────────────────────────

const SVGs: Record<AircraftCategory, string> = {

  // Heavy widebody — 4 engines, large swept wings
  heavy: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="24" height="24">
    <ellipse cx="12" cy="12" rx="2.2" ry="9.5" fill="white"/>
    <polygon points="12,7 1,15 23,15" fill="white" opacity="0.92"/>
    <polygon points="12,3 10,6 14,6" fill="white"/>
    <ellipse cx="5.2" cy="13.5" rx="1.6" ry="0.7" fill="white"/>
    <ellipse cx="8.2" cy="14.2" rx="1.6" ry="0.7" fill="white"/>
    <ellipse cx="15.8" cy="14.2" rx="1.6" ry="0.7" fill="white"/>
    <ellipse cx="18.8" cy="13.5" rx="1.6" ry="0.7" fill="white"/>
  </svg>`,

  // Medium widebody — 2 large engines, swept wings
  widebody: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="24" height="24">
    <ellipse cx="12" cy="12" rx="2" ry="9" fill="white"/>
    <polygon points="12,8 2,15 22,15" fill="white" opacity="0.92"/>
    <polygon points="12,3 10,6.5 14,6.5" fill="white"/>
    <ellipse cx="6" cy="13.8" rx="1.8" ry="0.8" fill="white"/>
    <ellipse cx="18" cy="13.8" rx="1.8" ry="0.8" fill="white"/>
  </svg>`,

  // Narrowbody — 2 underwing engines, classic single-aisle
  narrowbody: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="24" height="24">
    <ellipse cx="12" cy="12" rx="1.7" ry="8.5" fill="white"/>
    <polygon points="12,8.5 3,15 21,15" fill="white" opacity="0.92"/>
    <polygon points="12,3.5 10.2,7 13.8,7" fill="white"/>
    <ellipse cx="7" cy="14" rx="1.5" ry="0.7" fill="white"/>
    <ellipse cx="17" cy="14" rx="1.5" ry="0.7" fill="white"/>
  </svg>`,

  // Regional jet — slim, rear-mounted or small wing engines
  regional: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="24" height="24">
    <ellipse cx="12" cy="12" rx="1.4" ry="8" fill="white"/>
    <polygon points="12,9.5 4,15.5 20,15.5" fill="white" opacity="0.92"/>
    <polygon points="12,4 10.5,7.5 13.5,7.5" fill="white"/>
    <ellipse cx="9" cy="15.5" rx="1.1" ry="0.6" fill="white"/>
    <ellipse cx="15" cy="15.5" rx="1.1" ry="0.6" fill="white"/>
  </svg>`,

  // Business jet — sleek, swept wings, small
  bizjet: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="24" height="24">
    <ellipse cx="12" cy="12" rx="1.2" ry="8" fill="white"/>
    <polygon points="12,10 5,16 19,16" fill="white" opacity="0.92"/>
    <polygon points="12,4 10.5,7 13.5,7" fill="white"/>
    <ellipse cx="9.5" cy="16" rx="1" ry="0.55" fill="white"/>
    <ellipse cx="14.5" cy="16" rx="1" ry="0.55" fill="white"/>
  </svg>`,

  // Turboprop — straight/tapered wings, props shown as discs
  turboprop: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="24" height="24">
    <ellipse cx="12" cy="12" rx="1.5" ry="7.5" fill="white"/>
    <polygon points="12,10 2,14 22,14" fill="white" opacity="0.92"/>
    <polygon points="12,4.5 10.5,8 13.5,8" fill="white"/>
    <circle cx="4" cy="13" r="1.6" fill="none" stroke="white" stroke-width="0.8" opacity="0.7"/>
    <circle cx="20" cy="13" r="1.6" fill="none" stroke="white" stroke-width="0.8" opacity="0.7"/>
  </svg>`,

  // Light GA — small, high-wing or low-wing, simple
  light: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="24" height="24">
    <ellipse cx="12" cy="13" rx="1.1" ry="6" fill="white"/>
    <rect x="3" y="11.5" width="18" height="2" rx="1" fill="white" opacity="0.88"/>
    <polygon points="12,7 11,11 13,11" fill="white"/>
    <circle cx="3.5" cy="12.5" r="1.2" fill="none" stroke="white" stroke-width="0.7" opacity="0.7"/>
    <circle cx="20.5" cy="12.5" r="1.2" fill="none" stroke="white" stroke-width="0.7" opacity="0.7"/>
  </svg>`,

  // Helicopter — rotor disc + slim body
  helicopter: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="24" height="24">
    <ellipse cx="12" cy="13" rx="2.5" ry="4.5" fill="white"/>
    <ellipse cx="12" cy="10" rx="8" ry="1.2" fill="white" opacity="0.8"/>
    <line x1="12" y1="8.8" x2="12" y2="17.5" stroke="white" stroke-width="0.6"/>
    <ellipse cx="12" cy="18.5" rx="1" ry="1.8" fill="white" opacity="0.6"/>
  </svg>`,

  // Military — delta/fighter silhouette
  military: `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 24 24" width="24" height="24">
    <polygon points="12,2 15,14 12,12 9,14" fill="white"/>
    <polygon points="12,8 4,17 20,17" fill="white" opacity="0.85"/>
    <polygon points="12,15 10,20 14,20" fill="white" opacity="0.7"/>
  </svg>`,
};

// ── ICAO type → category mapping ──────────────────────────────────────────────

// Prefix/exact match lists — checked in order, first match wins
const TYPE_RULES: [RegExp, AircraftCategory][] = [
  // Helicopters
  [/^(R44|R22|B06|B407|B412|B429|EC13|EC35|EC45|H13|H47|H60|AS3|AS55|AS65|AW1|AW10|AW13|AW16|AW17|S61|S70|S76|S92|MI8|MI17|MI26|PZL|UH1|UH60|CH47|CH53|A109|A119|A139|A169|A189|BK11|BO10|EC20|EC25|EC30|EC55|EC75|EC12|BO10|BO10)/, "helicopter"],
  [/^(R[0-9]|B[0-9]0[679]|EC[0-9]|H[0-9][0-9]|AS[0-9]|AW[0-9]|S[679][0-9]|MI[0-9]|UH|CH[0-9]|HH|A10[0-9])/, "helicopter"],

  // Military
  [/^(F1[5-9]|F2[0-9]|F3[0-9]|F4|F5|B1|B2|B52|B57|C130|C17|C5|C141|P3|P8|E3|E8|U2|SR7|A10|AV8|EF2|GR4|TORM|HAWK|HAWK|VC10|MIG|SU[0-9]|YAK|IL[0-9]|TU[0-9]|AN[0-9]|HUNT|SPIF|ZERO|MOSE|G159|T38|T45|FA18|E2|V22)/, "military"],

  // Heavy widebodies (4 engine or super-heavy)
  [/^(A38|B74[0-9]|B74[A-Z]|AN12|AN22|AN7|IL76|IL96|C5A|B74|AN1[2-9]|A340|A345|A346|C17)/, "heavy"],

  // Medium widebody (twin-aisle twin engine)
  [/^(A30|A31|A32[23]XL|A33[023]|A33[0-9]|A35[09]|A359|A35K|B76[37]|B772|B773|B77[A-Z]|B787|B788|B789|B78[A-Z]|B76[0-9]|IL86|IL96|L101|DC10|MD11|C135)/, "widebody"],

  // Business jets
  [/^(C25[0-9]|C5[56][0-9X]|C68|C75|C750|CL30|CL35|CL60|CL65|F2TH|F900|FA10|FA20|FA50|FA7X|GLEX|GL5T|GL6T|G[0-9]{3}|GLF|HA4T|H25[0-9]|H26[0-9]|LJ[0-9]|MU30|P180|PC12|PC24|PIAC|SBR1|SBR2|BE40|BE90|E50P|EA50|EMB5|PC6|SF34|TBM[0-9]|WW24)/, "bizjet"],

  // Turboprops
  [/^(AT[467][0-9]|AT[0-9][0-9]|DH8|Q[0-9]|P68|C208|C212|C310|C402|C404|C411|C414|C421|C425|C441|F27|F50|F60|PC6|PA31|PA34|PA42|PA46|BN2|BE99|BE18|P3|S601|JS41|J41|MA60|SF34|C235|CN235|Y12)/, "turboprop"],

  // Light GA
  [/^(C1[0-9][0-9]|C17[0-9]|PA[0-9][0-9]|SR[0-9][0-9]|DA[0-9][0-9]|DR4|TB[0-9]|RV[0-9]|GLID|ULAC|SHIP|BALL|AS[0-9][0-9]|PIP[0-9]|T182|P28[0-9]|BE[0-9][0-9]|M20[0-9]|AA[0-9])/, "light"],

  // Regional jets
  [/^(E14|E17[0-9]|E19[0-9]|E29[0-9]|E55P|CRJ|CL[0-9]|C[0-9]00|RJ[0-9]|AR[0-9]|A[0-9]{4}|E[0-9]7[0-9]|ERJ|E[0-9]{3}|ARJ)/, "regional"],

  // Narrowbody (everything else with a standard ICAO type is likely narrowbody)
  [/^(A31[0-9]|A32[0-9]|B7[23][0-9]|B73[0-9]|B73[A-Z]|MD[0-9]|DC[0-9]|73[0-9]|72[0-9]|71[0-9]|32[0-9]|31[0-9])/, "narrowbody"],
];

export function getAircraftCategory(type: string): AircraftCategory {
  if (!type) return "narrowbody";
  const t = type.toUpperCase().trim();
  for (const [re, cat] of TYPE_RULES) {
    if (re.test(t)) return cat;
  }
  // Fallback heuristics
  if (t.startsWith("C1") || t.startsWith("PA") || t.startsWith("SR")) return "light";
  if (t.startsWith("AT") || t.startsWith("DH")) return "turboprop";
  return "narrowbody";
}

/** Render an aircraft SVG to an HTMLImageElement for MapLibre addImage() */
export function buildAircraftImage(
  category: AircraftCategory,
  color = "white",
  size = 20,
): Promise<HTMLImageElement> {
  const svgRaw = SVGs[category].replace(/fill="white"/g, `fill="${color}"`).replace(/stroke="white"/g, `stroke="${color}"`);
  const svg = svgRaw.replace('width="24" height="24"', `width="${size}" height="${size}"`);
  return new Promise((resolve, reject) => {
    const img = new Image(size, size);
    img.onload  = () => resolve(img);
    img.onerror = reject;
    img.src = `data:image/svg+xml,${encodeURIComponent(svg)}`;
  });
}

export const AIRCRAFT_CATEGORIES: AircraftCategory[] = [
  "heavy", "widebody", "narrowbody", "regional", "bizjet", "turboprop", "light", "helicopter", "military",
];

export { SVGs };
