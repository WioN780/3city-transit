# dashboard

React (Vite) dashboard consuming the Go API's `/api/v1/routes/worst-offenders`,
`/api/v1/routes/{route_id}/timeseries`, and `/api/v1/hotspots` endpoints.

- **Leaderboard** -- worst-performing routes for a selectable window (24h/7d/30d). Click a row to select a route.
- **Map** -- delay hotspots colored by severity (react-leaflet + OpenStreetMap tiles), filterable by the selected route.
- **Time series** -- on-time % over time for the selected route (recharts).

## Local dev

```
cd dashboard
npm install
cp .env.example .env   # set VITE_API_BASE_URL if the API isn't at localhost:8090
npm run dev
```

## Docker

Built and served via `dashboard/Dockerfile` (Vite build -> nginx) as the `dashboard`
service in the root `docker-compose.yml`, on host port 5173 -- matching the API's
default `DASHBOARD_ORIGIN` CORS allowance.
