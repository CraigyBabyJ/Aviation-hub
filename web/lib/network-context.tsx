"use client";

import { createContext, useContext, useState, useEffect, type ReactNode } from "react";

export type Network = "vatsim" | "ivao";

interface NetworkContextValue {
  network:    Network;
  setNetwork: (n: Network) => void;
}

const NetworkContext = createContext<NetworkContextValue>({
  network:    "vatsim",
  setNetwork: () => {},
});

export function NetworkProvider({ children }: { children: ReactNode }) {
  const [network, setNetworkState] = useState<Network>("vatsim");

  useEffect(() => {
    const stored = localStorage.getItem("avhub-network") as Network | null;
    const initial = (stored === "vatsim" || stored === "ivao") ? stored : "vatsim";
    setNetworkState(initial);
    document.documentElement.setAttribute("data-network", initial);
  }, []);

  function setNetwork(n: Network) {
    setNetworkState(n);
    localStorage.setItem("avhub-network", n);
    document.documentElement.setAttribute("data-network", n);
  }

  return (
    <NetworkContext.Provider value={{ network, setNetwork }}>
      {children}
    </NetworkContext.Provider>
  );
}

export function useNetwork() {
  return useContext(NetworkContext);
}
