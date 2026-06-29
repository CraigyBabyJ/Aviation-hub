import type { Metadata } from "next";
import "./globals.css";
import { Navbar } from "@/components/layout/navbar";
import { Footer } from "@/components/layout/footer";
import { Background } from "@/components/layout/background";

const BASE_URL = "https://aviation-hub.craigybabyj.com";

export const metadata: Metadata = {
  metadataBase: new URL(BASE_URL),
  title: {
    default: "Aviation Hub – Live VATSIM & IVAO Dashboard",
    template: "%s – Aviation Hub",
  },
  description:
    "Real-time VATSIM and IVAO flight simulation dashboard. Monitor live ATC, weather (METAR, TAF, ATIS, SIGMET), airport traffic, events, and bookings. Free Discord bot included.",
  keywords: [
    "VATSIM dashboard",
    "VATSIM ATC live",
    "VATSIM tracker",
    "IVAO live ATC",
    "flight simulation ATC",
    "METAR online",
    "SIGMET tracker",
    "aviation weather",
    "VATSIM Discord bot",
    "online ATC monitor",
    "flight sim weather",
    "aviation hub",
  ],
  authors: [{ name: "Craig", url: BASE_URL }],
  creator: "Craig",
  publisher: "Aviation Hub",
  robots: {
    index: true,
    follow: true,
    googleBot: { index: true, follow: true, "max-snippet": -1, "max-image-preview": "large" },
  },
  openGraph: {
    type: "website",
    locale: "en_GB",
    url: BASE_URL,
    siteName: "Aviation Hub",
    title: "Aviation Hub – Live VATSIM & IVAO Dashboard",
    description:
      "Real-time VATSIM and IVAO flight simulation dashboard. Monitor live ATC, weather, airport traffic, events and bookings.",
    images: [
      {
        url: "/og-image.png",
        width: 1200,
        height: 630,
        alt: "Aviation Hub – Live VATSIM & IVAO Dashboard",
      },
    ],
  },
  twitter: {
    card: "summary_large_image",
    title: "Aviation Hub – Live VATSIM & IVAO Dashboard",
    description:
      "Real-time VATSIM and IVAO flight simulation dashboard. Live ATC, METAR, SIGMET, events and more.",
    images: ["/og-image.png"],
    creator: "@craigybabyj",
  },
  alternates: {
    canonical: BASE_URL,
  },
  manifest: "/site.webmanifest",
};

const jsonLd = {
  "@context": "https://schema.org",
  "@type": "WebApplication",
  name: "Aviation Hub",
  url: BASE_URL,
  description:
    "Real-time VATSIM and IVAO flight simulation dashboard. Monitor live ATC, weather, airport traffic, events and bookings.",
  applicationCategory: "UtilityApplication",
  operatingSystem: "Web",
  offers: { "@type": "Offer", price: "0", priceCurrency: "GBP" },
  author: { "@type": "Person", name: "Craig", url: BASE_URL },
  inLanguage: "en-GB",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  return (
    <html lang="en" className="dark">
      <head>
        <script
          type="application/ld+json"
          dangerouslySetInnerHTML={{ __html: JSON.stringify(jsonLd) }}
        />
      </head>
      <body>
        <Background />
        <Navbar />
        <main className="relative z-10 pt-14 min-h-screen">
          {children}
        </main>
        <Footer />
      </body>
    </html>
  );
}
