import { useState } from "react";
import { useApi } from "../useApi";
import { fetchWorstOffenders } from "../api";
import WindowSelector from "./WindowSelector";

export default function Leaderboard({ onSelectRoute, selectedRouteId }) {
  const [window, setWindow] = useState("7d");
  const { data, loading, error } = useApi(() => fetchWorstOffenders(window), [window]);

  return (
    <section className="panel">
      <div className="panel-header">
        <h2>Worst-performing routes</h2>
        <WindowSelector value={window} onChange={setWindow} />
      </div>
      {loading && <p className="status">Loading…</p>}
      {error && <p className="status error">Failed to load leaderboard: {error.message}</p>}
      {data && (
        <table className="leaderboard">
          <thead>
            <tr>
              <th>#</th>
              <th>Route</th>
              <th>On-time %</th>
              <th>Avg delay</th>
            </tr>
          </thead>
          <tbody>
            {data.routes.map((r) => (
              <tr
                key={r.route_id}
                className={r.route_id === selectedRouteId ? "selected" : ""}
                onClick={() => onSelectRoute(r.route_id)}
              >
                <td>{r.rank}</td>
                <td>{r.route_name || r.route_id}</td>
                <td>{r.on_time_pct.toFixed(1)}%</td>
                <td>{Math.round(r.avg_delay_seconds)}s</td>
              </tr>
            ))}
            {data.routes.length === 0 && (
              <tr>
                <td colSpan={4} className="status">
                  No data for this window.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      )}
    </section>
  );
}
