"use client";

export function Background() {
  return (
    <div className="fixed inset-0 pointer-events-none z-0" aria-hidden>
      {/* Base dark fill */}
      <div className="absolute inset-0 bg-[#050505]" />

      {/* Bottom radial glow */}
      <div className="absolute inset-0 bg-radial-dark" />

      {/* Top-left accent bleed — follows network theme via CSS var */}
      <div className="absolute -top-40 -left-40 w-[600px] h-[600px] rounded-full blur-[120px] transition-[background-color] duration-700"
        style={{ backgroundColor: "rgba(var(--accent), 0.07)" }} />

      {/* Bottom-right accent */}
      <div className="absolute -bottom-40 -right-40 w-[500px] h-[500px] rounded-full blur-[100px] transition-[background-color] duration-700"
        style={{ backgroundColor: "rgba(var(--accent), 0.05)" }} />

      {/* Diagonal grid lines */}
      <div className="absolute inset-0 bg-grid-lines opacity-100" />

      {/* Horizontal rule at top */}
      <div className="absolute top-0 left-0 right-0 h-px bg-gradient-to-r from-transparent via-white/10 to-transparent" />
    </div>
  );
}
