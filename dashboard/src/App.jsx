import { useState } from "react";
import Leaderboard from "./components/Leaderboard";
import HotspotMap from "./components/HotspotMap";
import TimeseriesChart from "./components/TimeseriesChart";
import "./App.css";

export default function App() {
  const [selectedRouteId, setSelectedRouteId] = useState(null);

  return (
    <div className="app">
      <header className="app-header">
        <h1>3City Transit — Delay Dashboard</h1>
      </header>
      <main className="app-grid">
        <Leaderboard onSelectRoute={setSelectedRouteId} selectedRouteId={selectedRouteId} />
        <HotspotMap routeId={selectedRouteId} />
        <TimeseriesChart routeId={selectedRouteId} />
      </main>
    </div>
  );
}
