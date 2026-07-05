const BASE_URL = import.meta.env.VITE_API_BASE_URL || "http://localhost:8090";

async function getJSON(path) {
  const res = await fetch(`${BASE_URL}${path}`);
  if (!res.ok) {
    const body = await res.json().catch(() => ({}));
    throw new Error(body.error || `Request failed with status ${res.status}`);
  }
  return res.json();
}

export function fetchWorstOffenders(window) {
  return getJSON(`/api/v1/routes/worst-offenders?window=${encodeURIComponent(window)}`);
}

export function fetchHotspots(window, routeId) {
  const params = new URLSearchParams({ window });
  if (routeId) params.set("route_id", routeId);
  return getJSON(`/api/v1/hotspots?${params}`);
}

export function fetchRouteTimeseries(routeId, window) {
  return getJSON(`/api/v1/routes/${encodeURIComponent(routeId)}/timeseries?window=${encodeURIComponent(window)}`);
}
