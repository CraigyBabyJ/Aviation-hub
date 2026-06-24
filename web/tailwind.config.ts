import type { Config } from "tailwindcss";

const config: Config = {
  content: [
    "./app/**/*.{ts,tsx}",
    "./components/**/*.{ts,tsx}",
    "./lib/**/*.{ts,tsx}",
  ],
  theme: {
    extend: {
      colors: {
        hub: {
          bg: "#050505",
          panel: "rgba(20,20,20,0.7)",
          solid: "#111111",
          line: "rgba(255,255,255,0.08)",
          glow: "rgba(255,255,255,0.25)",
          muted: "#9ca3af",
          red: "#dc2626",
          "red-dim": "rgba(220,38,38,0.15)",
          "red-glow": "rgba(220,38,38,0.4)",
        },
      },
      fontFamily: {
        mono: ["'JetBrains Mono'", "'Fira Code'", "ui-monospace", "monospace"],
      },
      letterSpacing: {
        widest: "0.25em",
        "extra-wide": "0.15em",
      },
      animation: {
        pulse: "pulse 2s cubic-bezier(0.4, 0, 0.6, 1) infinite",
        "spin-slow": "spin 8s linear infinite",
        scan: "scan 3s ease-in-out infinite",
      },
      keyframes: {
        scan: {
          "0%, 100%": { opacity: "0.3" },
          "50%": { opacity: "1" },
        },
      },
      backgroundImage: {
        "radial-dark":
          "radial-gradient(ellipse 80% 50% at 50% 100%, rgba(255,255,255,0.04) 0%, transparent 70%)",
        "panel-gradient":
          "linear-gradient(to bottom, rgba(255,255,255,0.04) 0%, transparent 100%)",
        "red-glow":
          "radial-gradient(ellipse at center, rgba(220,38,38,0.2) 0%, transparent 70%)",
      },
      boxShadow: {
        panel: "0 0 0 1px rgba(255,255,255,0.06), inset 0 1px 0 rgba(255,255,255,0.06)",
        "panel-hover": "0 0 24px rgba(255,255,255,0.06), 0 0 0 1px rgba(255,255,255,0.12)",
        "red-glow": "0 0 20px rgba(220,38,38,0.3)",
        "map-inner": "inset 0 0 60px rgba(0,0,0,0.8), inset 0 0 0 1px rgba(255,255,255,0.06)",
      },
    },
  },
  plugins: [],
};

export default config;
