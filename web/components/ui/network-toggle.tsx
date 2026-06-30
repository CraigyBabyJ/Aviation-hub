"use client";

import { useNetwork, type Network } from "@/lib/network-context";

export function NetworkToggle() {
  const { network, setNetwork } = useNetwork();
  const isIvao = network === "ivao";

  return (
    <div className="flex items-center gap-2.5">
      <span className={`text-[10px] font-bold tracking-widest uppercase transition-colors ${!isIvao ? "text-red-400" : "text-zinc-600"}`}>
        VATSIM
      </span>

      <button
        onClick={() => setNetwork(isIvao ? "vatsim" : "ivao")}
        role="switch"
        aria-checked={isIvao}
        className="relative w-10 h-5 rounded-full border transition-colors duration-200 focus:outline-none"
        style={{
          background:   isIvao ? "rgba(59,130,246,0.25)" : "rgba(239,68,68,0.15)",
          borderColor:  isIvao ? "rgba(59,130,246,0.5)"  : "rgba(239,68,68,0.4)",
        }}
      >
        <span
          className="absolute top-0.5 w-4 h-4 rounded-full transition-all duration-200 shadow-sm"
          style={{
            left:       isIvao ? "calc(100% - 18px)" : "2px",
            background: isIvao ? "#3b82f6" : "#ef4444",
            boxShadow:  isIvao ? "0 0 8px rgba(59,130,246,0.6)" : "0 0 8px rgba(239,68,68,0.6)",
          }}
        />
      </button>

      <span className={`text-[10px] font-bold tracking-widest uppercase transition-colors ${isIvao ? "text-blue-400" : "text-zinc-600"}`}>
        IVAO
      </span>
    </div>
  );
}
