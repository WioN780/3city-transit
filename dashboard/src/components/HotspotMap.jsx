import { useState } from "react";
import { MapContainer, TileLayer, CircleMarker, Popup } from "react-leaflet";
import "leaflet/dist/leaflet.css";
import { useApi } from "../useApi";
import { fetchHotspots } from "../api";
import WindowSelector from "./WindowSelector";

const GDANSK_CENTER = [54.352, 18.6466];

function severityColor(avgDelaySeconds) {
  if (avgDelaySeconds < 60) return "#2e7d32";
  if (avgDelaySeconds < 180) return "#f9a825";
  return "#c62828";
}

export default function HotspotMap({ routeId }) {
  const [window, setWindow] = useState("7d");
  const { data, loading, error } = useApi(() => fetchHotspots(window, routeId), [window, routeId]);

  return (
    <section className="panel">
      <div className="panel-header">
        <h2>Delay hotspots{routeId ? ` — route ${routeId}` : ""}</h2>
        <WindowSelector value={window} onChange={setWindow} />
      </div>
      {loading && <p className="status">Loading…</p>}
      {error && <p className="status error">Failed to load hotspots: {error.message}</p>}
      <div className="map-wrap">
        <MapContainer center={GDANSK_CENTER} zoom={12} style={{ height: "100%", width: "100%" }}>
          <TileLayer
            attribution="&copy; OpenStreetMap contributors"
            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
          />
          {data?.hotspots.map((h, i) => (
            <CircleMarker
              key={i}
              center={[h.latitude, h.longitude]}
              radius={6 + Math.min(h.incident_count / 5, 12)}
              pathOptions={{ color: severityColor(h.avg_delay_seconds), fillOpacity: 0.6 }}
            >
              <Popup>
                Avg delay: {Math.round(h.avg_delay_seconds)}s
                <br />
                Incidents: {h.incident_count}
              </Popup>
            </CircleMarker>
          ))}
        </MapContainer>
      </div>
    </section>
  );
}
