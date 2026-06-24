"use client";

import { useEffect } from "react";

// global-error replaces the root layout when it catches, so it must include
// its own <html> and <body> tags.
export default function GlobalError({
  error,
  reset,
}: {
  error: Error & { digest?: string };
  reset: () => void;
}) {
  useEffect(() => {
    console.error("[AvHub global error]", error);
  }, [error]);

  return (
    <html lang="en">
      <body
        style={{
          background: "#050505",
          color: "#fff",
          fontFamily: "monospace",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          minHeight: "100vh",
          margin: 0,
        }}
      >
        <div style={{ textAlign: "center", maxWidth: 400 }}>
          <p
            style={{
              fontSize: 10,
              letterSpacing: "0.2em",
              textTransform: "uppercase",
              color: "#ef4444",
              marginBottom: 12,
            }}
          >
            Critical Error
          </p>
          <p style={{ fontSize: 12, color: "#71717a", marginBottom: 24 }}>
            {error?.message ?? "Aviation Hub encountered a fatal error."}
          </p>
          <button
            onClick={reset}
            style={{
              padding: "8px 20px",
              border: "1px solid rgba(255,255,255,0.1)",
              background: "rgba(255,255,255,0.04)",
              color: "#a1a1aa",
              fontSize: 10,
              letterSpacing: "0.15em",
              textTransform: "uppercase",
              borderRadius: 8,
              cursor: "pointer",
            }}
          >
            Reload
          </button>
        </div>
      </body>
    </html>
  );
}
