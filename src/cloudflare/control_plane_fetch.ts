/** Bounded entry failover. Ambiguous writes are returned to the caller without replay. */
export async function controlPlaneFetch(endpoints: string, route: string, init: RequestInit = {}): Promise<Response> {
  const bases = endpoints.split(",").map(value => value.trim()).filter(Boolean);
  if (!bases.length || bases.length > 4) throw new Error("INVALID_CONTROL_ENDPOINTS");
  for (const base of bases) { const url = new URL(base); if (!["http:", "https:"].includes(url.protocol) || url.username || url.password || url.search || url.hash) throw new Error("INVALID_CONTROL_ENDPOINT"); }
  const reads = !init.method || init.method === "GET";
  if (!reads && bases.length > 1) {
    for (const base of bases) {
      init.signal?.throwIfAborted();
      let healthy = false;
      try { const probe = await fetch(new URL("/healthz", base), { redirect: "error", signal: init.signal ? AbortSignal.any([init.signal, AbortSignal.timeout(1500)]) : AbortSignal.timeout(1500) }); healthy = probe.ok; await probe.body?.cancel(); } catch { /* choose another entry before issuing the write */ }
      if (healthy) return controlPlaneFetch(base, route, init);
    }
    throw new Error("CONTROL_UNAVAILABLE");
  }
  let last: unknown;
  for (const base of reads ? bases : bases.slice(0, 1)) {
    init.signal?.throwIfAborted();
    try {
      const response = await fetch(new URL(route, base), { ...init, redirect: "error", signal: init.signal ? AbortSignal.any([init.signal, AbortSignal.timeout(3000)]) : AbortSignal.timeout(3000) });
      if (!reads || ![502, 503, 504].includes(response.status)) return response;
      last = new Error(`CONTROL_STATUS_${response.status}`); await response.body?.cancel();
    } catch (error) { last = error; }
  }
  throw last ?? new Error("CONTROL_UNAVAILABLE");
}
