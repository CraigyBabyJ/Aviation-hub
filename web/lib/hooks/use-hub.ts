"use client";

import { useEffect, useRef, useState, useCallback } from "react";

interface UseHubOptions {
  /** Polling interval in milliseconds. Default 30 000 (30s). Pass 0 to disable. */
  interval?: number;
  /** Don't fetch on mount — wait for manual refresh. */
  manual?: boolean;
}

interface UseHubResult<T extends object> {
  data: T | null;
  loading: boolean;
  error: string | null;
  lastUpdated: Date | null;
  refresh: () => void;
}

/**
 * Polls /api/hub/<path> at the given interval and returns typed data.
 *
 * @example
 * const { data, loading } = useHub<RankedResponse>("airports/ranked", { limit: "20" });
 */
export function useHub<T extends object>(
  path: string,
  queryParams?: Record<string, string>,
  options: UseHubOptions = {}
): UseHubResult<T> {
  const { interval = 30_000, manual = false } = options;
  const [data, setData] = useState<T | null>(null);
  const [loading, setLoading] = useState(!manual);
  const [error, setError] = useState<string | null>(null);
  const [lastUpdated, setLastUpdated] = useState<Date | null>(null);
  const timerRef = useRef<ReturnType<typeof setInterval> | null>(null);
  const abortRef = useRef<AbortController | null>(null);

  const buildUrl = useCallback(() => {
    const url = new URL(`/api/hub/${path}`, window.location.origin);
    if (queryParams) {
      Object.entries(queryParams).forEach(([k, v]) => url.searchParams.set(k, v));
    }
    return url.toString();
  }, [path, JSON.stringify(queryParams)]); // eslint-disable-line react-hooks/exhaustive-deps

  const fetchData = useCallback(async () => {
    abortRef.current?.abort();
    abortRef.current = new AbortController();
    setLoading(true);
    try {
      const res = await fetch(buildUrl(), { signal: abortRef.current.signal });
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const json = await res.json();
      setData(json);
      setError(null);
      setLastUpdated(new Date());
    } catch (err: unknown) {
      if (err instanceof Error && err.name === "AbortError") return;
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setLoading(false);
    }
  }, [buildUrl]);

  useEffect(() => {
    if (!manual) fetchData();

    if (interval > 0) {
      timerRef.current = setInterval(fetchData, interval);
    }

    return () => {
      if (timerRef.current) clearInterval(timerRef.current);
      abortRef.current?.abort();
    };
  }, [fetchData, interval, manual]);

  return { data, loading, error, lastUpdated, refresh: fetchData };
}
