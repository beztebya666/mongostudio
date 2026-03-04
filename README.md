# ⚡ MongoStudio v2.0.0

Blazing-fast MongoDB GUI with dark/light themes, execution safety, and full CRUD.

## What's New in v2.0

### Core Features
- **Advanced Connection Config** — TLS, authSource, replicaSet, SRV, directConnection, readPreference
- **Execution Safety** — Safe/Power mode, maxTimeMS, query limits, rate limiting, $where blocking
- **Explain Integration** — Query plan visualization, index usage, collection scan detection
- **Query History** — Last 50 queries with timing, replay, slow query detection
- **Cancel Query** — Abort running queries with live execution timer
- **Schema Preview** — Sample-based field analyzer with type distribution
- **Connection Profiles** — Save/load connection configurations
- **Production Warning Banner** — Auto-detected, always visible
- **Error Classification** — Auth, network, TLS, timeout, permission errors with friendly messages

### UI Enhancements
- **Dark/Light Theme** — Toggle with full CSS variable system
- **Table View** — Switch between JSON and table document views
- **Export Data** — JSON and CSV export with filters
- **Sidebar CRUD** — Create/drop databases and collections directly
- **Index Manager** — Create with options (unique, sparse, compound) / drop indexes
- **Settings Modal** — Execution config, server info, health metrics
- **Result Size Indicator** — KB size of query results
- **Health/Cluster Info Panel** — Server status, uptime, connections, opcounters

### Backend
- `POST /api/databases` — Create database
- `DELETE /api/databases/:db` — Drop database
- `POST /api/databases/:db/collections` — Create collection
- `DELETE /api/databases/:db/collections/:col` — Drop collection
- `GET /api/.../schema` — Schema analysis (sample-based)
- `POST /api/.../explain` — Query explain with summary
- `POST /api/.../export` — JSON/CSV export
- `GET/PUT /api/execution-config` — Safe/Power mode config
- `GET /api/health` — Health check
- `GET /api/metrics` — Server metrics
- `GET /api/audit` — Audit log (per session)
- `GET /api/status` — Full server status

## Quick Start

```bash
npm install
npm run dev
# → http://localhost:5173
```

## Stack
React 18 · Vite 5 · Express 4 · MongoDB Driver 5 · Tailwind CSS 3

## Compatibility
MongoDB 3.6 → 8.x · Atlas & Self-hosted · Auto-detect version with capability matrix
