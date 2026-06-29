import type { Metadata } from "next";

export const metadata: Metadata = {
  title: "Live Dashboard",
  description:
    "Live VATSIM and IVAO flight simulation dashboard. Track active ATC controllers, inbound aircraft, spicy airports, METAR weather, SIGMETs, events and booking data in real time.",
  alternates: {
    canonical: "https://aviation-hub.craigybabyj.com/dashboard",
  },
  openGraph: {
    url: "https://aviation-hub.craigybabyj.com/dashboard",
    title: "Live Dashboard – Aviation Hub",
    description:
      "Track VATSIM and IVAO live ATC, weather, and airport traffic in real time.",
  },
};

export default function DashboardLayout({ children }: { children: React.ReactNode }) {
  return <>{children}</>;
}
