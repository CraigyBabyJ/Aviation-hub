import { ImageResponse } from "next/og";

export const alt = "Aviation Hub – Live VATSIM & IVAO Dashboard";
export const size = { width: 1200, height: 630 };
export const contentType = "image/png";

export default function OGImage() {
  return new ImageResponse(
    (
      <div
        style={{
          background: "#050505",
          width: "100%",
          height: "100%",
          display: "flex",
          flexDirection: "column",
          alignItems: "flex-start",
          justifyContent: "center",
          padding: "80px",
          fontFamily: "system-ui, sans-serif",
          position: "relative",
          overflow: "hidden",
        }}
      >
        {/* Grid lines */}
        <div
          style={{
            position: "absolute",
            inset: 0,
            backgroundImage:
              "linear-gradient(rgba(255,255,255,0.03) 1px, transparent 1px), linear-gradient(90deg, rgba(255,255,255,0.03) 1px, transparent 1px)",
            backgroundSize: "40px 40px",
          }}
        />

        {/* Red top-left glow */}
        <div
          style={{
            position: "absolute",
            top: -120,
            left: -120,
            width: 700,
            height: 700,
            borderRadius: "50%",
            background: "radial-gradient(circle, rgba(220,38,38,0.18) 0%, transparent 65%)",
          }}
        />

        {/* Logo */}
        <div style={{ display: "flex", alignItems: "center", marginBottom: 48 }}>
          <div
            style={{
              width: 60,
              height: 60,
              borderRadius: "50%",
              border: "2px solid rgba(220,38,38,0.65)",
              display: "flex",
              alignItems: "center",
              justifyContent: "center",
              marginRight: 20,
            }}
          >
            <div
              style={{
                width: 18,
                height: 18,
                borderRadius: "50%",
                background: "#dc2626",
              }}
            />
          </div>
          <div style={{ display: "flex", flexDirection: "column" }}>
            <span
              style={{
                color: "white",
                fontSize: 22,
                fontWeight: 700,
                letterSpacing: "0.22em",
                textTransform: "uppercase",
              }}
            >
              Aviation
            </span>
            <span
              style={{
                color: "rgba(220,38,38,0.8)",
                fontSize: 13,
                letterSpacing: "0.38em",
                textTransform: "uppercase",
              }}
            >
              Hub
            </span>
          </div>
        </div>

        {/* Headline */}
        <div
          style={{
            color: "white",
            fontSize: 68,
            fontWeight: 800,
            lineHeight: 1.1,
            marginBottom: 24,
            maxWidth: 820,
            display: "flex",
          }}
        >
          Live VATSIM &amp; IVAO Dashboard
        </div>

        {/* Subtitle */}
        <div
          style={{
            color: "rgba(161,161,170,0.85)",
            fontSize: 28,
            marginBottom: 52,
            display: "flex",
          }}
        >
          Real-time ATC · Weather · Airport Traffic · Events
        </div>

        {/* Feature pills */}
        <div style={{ display: "flex", gap: 14 }}>
          {["Live ATC", "METAR & TAF", "SIGMET", "SimBrief", "Discord Bot"].map((label) => (
            <div
              key={label}
              style={{
                background: "rgba(255,255,255,0.04)",
                border: "1px solid rgba(255,255,255,0.08)",
                borderRadius: 8,
                padding: "10px 22px",
                color: "rgba(161,161,170,0.9)",
                fontSize: 18,
                display: "flex",
              }}
            >
              {label}
            </div>
          ))}
        </div>

        {/* Domain */}
        <div
          style={{
            position: "absolute",
            bottom: 56,
            right: 80,
            color: "rgba(113,113,122,0.55)",
            fontSize: 20,
            letterSpacing: "0.04em",
            display: "flex",
          }}
        >
          aviation-hub.craigybabyj.com
        </div>

        {/* Bottom red accent line */}
        <div
          style={{
            position: "absolute",
            bottom: 0,
            left: 0,
            right: 0,
            height: 3,
            background: "linear-gradient(90deg, transparent, #dc2626 40%, #dc2626 60%, transparent)",
          }}
        />
      </div>
    ),
    { ...size }
  );
}
