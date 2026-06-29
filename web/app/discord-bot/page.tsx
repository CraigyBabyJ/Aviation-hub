import type { Metadata } from "next";
import Link from "next/link";
import { Bot, Command, Radio, ShieldCheck } from "lucide-react";

export const metadata: Metadata = {
  title: "Discord Bot",
  description:
    "Add the Aviation Hub Discord bot to your server. Get live VATSIM and IVAO ATC status, METAR, TAF, ATIS, SIGMET, runway info, satellite images and more via slash commands.",
  alternates: {
    canonical: "https://aviation-hub.craigybabyj.com/discord-bot",
  },
  openGraph: {
    url: "https://aviation-hub.craigybabyj.com/discord-bot",
    title: "Aviation Hub Discord Bot",
    description:
      "Live VATSIM & IVAO slash commands for your Discord server — ATC, METAR, SIGMET, runways and more.",
  },
};

const FEATURES = [
  {
    icon: Command,
    title: "Slash Commands",
    copy: "Airport briefs, METARs, ATIS, VATSIM lookup, routes, bookings and spicy-airport picks.",
  },
  {
    icon: Radio,
    title: "Live Network Data",
    copy: "Commands call the same Aviation Hub feed powering the dashboard.",
  },
  {
    icon: ShieldCheck,
    title: "Server Ready",
    copy: "Built for quick Discord checks without leaving your flight sim session.",
  },
];

type CommandGroup = {
  label: string;
  commands: { name: string; description: string }[];
};

const COMMAND_GROUPS: CommandGroup[] = [
  {
    label: "Weather",
    commands: [
      { name: "/atis", description: "Live VATSIM ATIS for an airport" },
      { name: "/metar", description: "Raw METAR text from Aviation Hub DB" },
      { name: "/pirep", description: "Recent PIREPs near an airport (last 6 h, 200 nm radius)" },
      { name: "/sigmet", description: "Active international SIGMETs" },
      { name: "/taf", description: "Terminal Aerodrome Forecast for an airport" },
      { name: "/weather", description: "METAR + summary for an airport" },
      { name: "/winds", description: "Winds aloft forecast for a US airport (FL060–FL390)" },
    ],
  },
  {
    label: "Airport",
    commands: [
      { name: "/airport", description: "Weather, spicy score, VATSIM, bookings & inbounds" },
      { name: "/charts", description: "Chart & reference links (SkyVector, FlightAware, FAA, AIP)" },
      { name: "/nearby", description: "Airports within a radius (default 50 nm, max 200 nm)" },
      { name: "/runway", description: "Runway details: length, surface, headings" },
      { name: "/sat", description: "Satellite aerial image (ESRI, cached locally)" },
      { name: "/spicy", description: "Current spicy airports widget" },
      { name: "/summary", description: "Light airport row: ATC, weather flags, upcoming" },
      { name: "/xwind", description: "Crosswind & headwind components per runway (live METAR)" },
    ],
  },
  {
    label: "VATSIM & IVAO",
    commands: [
      { name: "/vatsim", description: "Flight, ATC callsign, or airport ICAO lookup" },
      { name: "/bookings", description: "VATSIM ATC bookings (advisory)" },
      { name: "/events", description: "VATSIM events in the next N days" },
      { name: "/inbounds", description: "Pilots filed to land at an ICAO (VATSIM snapshot)" },
      { name: "/ivao", description: "IVAO online ATC for an airport" },
      { name: "/ranked", description: "Best airports now: live ATC, traffic & weather" },
      { name: "/stats", description: "VATSIM member stats: rating, pilot & ATC hours" },
      { name: "/upcoming", description: "Upcoming bookings/events by airport (next N hours)" },
      { name: "/vatsimcount", description: "Total pilots & controllers currently online on VATSIM" },
    ],
  },
  {
    label: "SimBrief & Planning",
    commands: [
      { name: "/myplan", description: "Fetch and DM your latest SimBrief flight plan" },
      { name: "/postflight", description: "Post a SimBrief flight plan in this channel" },
      { name: "/airlines", description: "DM the full airline ICAO code & callsign list" },
    ],
  },
  {
    label: "Utilities",
    commands: [
      { name: "/convert", description: "Aviation unit converter: pressure, altitude, distance, speed, temperature" },
      { name: "/distance", description: "Great-circle distance & bearing between two airports" },
    ],
  },
  {
    label: "Bot & Server",
    commands: [
      { name: "/gnd-twr-alerts", description: "Enable/disable GND + TWR online alerts for this server" },
      { name: "/help", description: "Show every slash command and its description" },
      { name: "/info", description: "About Aviation Hub — bot invite & support server links" },
      { name: "/ping", description: "Check gateway latency to the Discord server" },
    ],
  },
];

export default function DiscordBotPage() {
  return (
    <div className="mx-auto max-w-[1200px] px-4 py-8 sm:px-6">
      {/* Hero */}
      <section className="hub-panel corner-accent w-full overflow-hidden p-8 sm:p-10">
        <div className="mx-auto max-w-2xl text-center">
          <div className="mx-auto mb-6 flex h-14 w-14 items-center justify-center rounded-full border border-red-500/30 bg-red-500/10 text-red-200">
            <Bot size={26} strokeWidth={1.5} />
          </div>
          <p className="mb-3 text-[9px] uppercase tracking-[0.34em] text-zinc-700">
            Aviation Hub Discord
          </p>
          <h1 className="text-3xl font-bold uppercase tracking-[0.18em] text-white sm:text-5xl">
            Add Discord Bot
          </h1>
          <p className="mx-auto mt-5 max-w-xl text-sm leading-6 text-zinc-500">
            Add the Aviation Hub bot to your Discord server for instant access to live airport,
            weather, ATC and route data — all powered by the same feed as the dashboard.
          </p>

          <div className="mt-8 flex flex-col items-center justify-center gap-3 sm:flex-row">
            <a
              href="https://discord.com/oauth2/authorize?client_id=1483159726475710704"
              target="_blank"
              rel="noreferrer"
              className="rounded border border-indigo-500/40 bg-indigo-500/[0.08] px-5 py-2 text-[10px] font-bold uppercase tracking-[0.24em] text-indigo-200/90 transition-colors hover:border-indigo-400/60 hover:bg-indigo-500/15 hover:text-white"
            >
              Invite Bot
            </a>
            <a
              href="https://discord.craigybabyj.com"
              target="_blank"
              rel="noreferrer"
              className="rounded border border-white/[0.08] bg-white/[0.02] px-5 py-2 text-[10px] font-bold uppercase tracking-[0.24em] text-zinc-400 transition-colors hover:border-white/20 hover:text-zinc-200"
            >
              Support Server
            </a>
            <Link
              href="/"
              className="rounded border border-red-500/25 bg-red-500/[0.04] px-5 py-2 text-[10px] font-bold uppercase tracking-[0.24em] text-red-100/90 transition-colors hover:border-red-400/45 hover:bg-red-500/10 hover:text-white"
            >
              Back To Dashboard
            </Link>
          </div>
        </div>

        {/* Feature cards */}
        <div className="mt-10 grid gap-3 md:grid-cols-3">
          {FEATURES.map(({ icon: Icon, title, copy }) => (
            <div key={title} className="rounded border border-white/[0.06] bg-white/[0.02] p-4">
              <Icon className="mb-4 text-red-300/80" size={18} strokeWidth={1.5} />
              <h2 className="mb-2 text-[11px] font-bold uppercase tracking-[0.2em] text-white">
                {title}
              </h2>
              <p className="text-xs leading-5 text-zinc-600">{copy}</p>
            </div>
          ))}
        </div>
      </section>

      {/* Commands */}
      <section className="mt-6">
        <h2 className="mb-4 text-[11px] font-bold uppercase tracking-[0.28em] text-zinc-600">
          Slash Commands
        </h2>
        <div className="grid gap-4 sm:grid-cols-2">
          {COMMAND_GROUPS.map((group) => (
            <div
              key={group.label}
              className="rounded border border-white/[0.06] bg-white/[0.02] p-5"
            >
              <p className="mb-3 text-[9px] font-bold uppercase tracking-[0.3em] text-red-500/70">
                {group.label}
              </p>
              <ul className="space-y-2">
                {group.commands.map((cmd) => (
                  <li key={cmd.name} className="flex gap-3">
                    <span className="w-36 shrink-0 font-mono text-[11px] text-indigo-300/80">
                      {cmd.name}
                    </span>
                    <span className="text-[11px] leading-4 text-zinc-500">{cmd.description}</span>
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>
      </section>
    </div>
  );
}
