import { useState } from "react";
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer } from "recharts";
import { useApi } from "../useApi";
import { fetchRouteTimeseries } from "../api";
import WindowSelector from "./WindowSelector";

export default function TimeseriesChart({ routeId }) {
  const [window, setWindow] = useState("30d");
  const { data, loading, error } = useApi(
    () => (routeId ? fetchRouteTimeseries(routeId, window) : Promise.resolve(null)),
    [routeId, window]
  );

  return (
    <section className="panel">
      <div className="panel-header">
        <h2>On-time % over time{routeId ? ` — route ${routeId}` : ""}</h2>
        <WindowSelector value={window} onChange={setWindow} />
      </div>
      {!routeId && <p className="status">Select a route from the leaderboard to see its trend.</p>}
      {loading && <p className="status">Loading…</p>}
      {error && <p className="status error">Failed to load time series: {error.message}</p>}
      {data && data.points.length === 0 && <p className="status">No data for this window.</p>}
      {data && data.points.length > 0 && (
        <ResponsiveContainer width="100%" height={280}>
          <LineChart data={data.points}>
            <CartesianGrid strokeDasharray="3 3" />
            <XAxis dataKey="service_date" />
            <YAxis domain={[0, 100]} unit="%" />
            <Tooltip formatter={(value) => `${value.toFixed(1)}%`} />
            <Line type="monotone" dataKey="on_time_pct" stroke="#1565c0" dot={false} />
          </LineChart>
        </ResponsiveContainer>
      )}
    </section>
  );
}
