"use client";

import { motion } from "framer-motion";
import { Activity, Radio, Globe, Calendar, Settings, Map } from "lucide-react";
import Link from "next/link";
import { usePathname } from "next/navigation";
import { cn } from "@/lib/utils";
import { AirportSearch } from "@/components/dashboard/airport-search";

const NAV_ITEMS = [
  { href: "/dashboard", label: "OVERVIEW", icon: Globe },
  { href: "/dashboard/routes", label: "ROUTES", icon: Map },
  { href: "/dashboard/traffic", label: "TRAFFIC", icon: Activity },
  { href: "/dashboard/atc", label: "ATC", icon: Radio },
  { href: "/dashboard/events", label: "EVENTS", icon: Calendar },
];

export function Navbar() {
  const pathname = usePathname();

  return (
    <motion.header
      initial={{ opacity: 0, y: -8 }}
      animate={{ opacity: 1, y: 0 }}
      transition={{ duration: 0.4, ease: "easeOut" }}
      className="fixed top-0 left-0 right-0 z-50 h-14"
    >
      {/* Glass bar */}
      <div className="h-full bg-[rgba(8,8,8,0.85)] backdrop-blur-xl border-b border-white/[0.06]">
        <div className="max-w-[1600px] mx-auto h-full flex items-center justify-between px-6">

          {/* Logo */}
          <Link href="/dashboard" className="flex items-center gap-3 group">
            <div className="relative flex items-center justify-center w-7 h-7">
              <div className="absolute inset-0 rounded-full border transition-[border-color] duration-500"
                style={{ borderColor: "rgba(var(--accent), 0.6)" }} />
              <div className="w-2 h-2 rounded-full transition-[background-color] duration-500"
                style={{ backgroundColor: "var(--accent-hex)" }} />
              <div className="absolute inset-0 rounded-full border animate-ping"
                style={{ borderColor: "rgba(var(--accent), 0.3)", animationDuration: "3s" }} />
            </div>
            <div className="flex flex-col leading-none">
              <span className="text-white font-bold text-sm tracking-[0.2em] uppercase">Aviation</span>
              <span className="text-[10px] tracking-[0.35em] uppercase font-medium transition-[color] duration-500"
                style={{ color: "var(--accent-text)" }}>Hub</span>
            </div>
          </Link>

          {/* Nav links */}
          <nav className="flex items-center gap-1">
            {NAV_ITEMS.map(({ href, label, icon: Icon }) => {
              const active = pathname === href || pathname?.startsWith(href + "/");
              return (
                <Link
                  key={href}
                  href={href}
                  className={cn(
                    "relative flex items-center gap-2 px-4 py-1.5 text-[11px] tracking-[0.18em] uppercase font-medium transition-colors duration-200 rounded",
                    active
                      ? "text-white"
                      : "text-zinc-500 hover:text-zinc-300"
                  )}
                >
                  <Icon size={12} strokeWidth={1.5} />
                  {label}
                  {active && (
                    <motion.div
                      layoutId="nav-indicator"
                      className="absolute bottom-0 left-2 right-2 h-px transition-[background-color] duration-500"
                      style={{ backgroundColor: "var(--accent-hex)" }}
                      transition={{ type: "spring", stiffness: 380, damping: 30 }}
                    />
                  )}
                </Link>
              );
            })}
          </nav>

          {/* Right: search + status + settings */}
          <div className="flex items-center gap-3">
            <AirportSearch />

            {/* Live indicator */}
            <div className="hidden sm:flex items-center gap-2">
              <span className="live-dot w-1.5 h-1.5 rounded-full block transition-[background-color] duration-500"
                style={{ backgroundColor: "var(--accent-hex)" }} />
              <span className="text-[10px] tracking-widest uppercase text-zinc-600">Live</span>
            </div>

            {/* Settings icon */}
            <button className="text-zinc-600 hover:text-zinc-400 transition-colors">
              <Settings size={14} strokeWidth={1.5} />
            </button>
          </div>
        </div>
      </div>

      {/* Bottom glow line */}
      <div className="h-px transition-[background] duration-500"
        style={{ background: "linear-gradient(to right, transparent, rgba(var(--accent), 0.25), transparent)" }} />
    </motion.header>
  );
}
