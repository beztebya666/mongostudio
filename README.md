<div align="center">

# ⚡MongoStudio

**A blazing-fast, beautiful MongoDB UI**

Modern • Minimal • Open Source

![License](https://img.shields.io/badge/license-MIT-00ed64)
![Docker](https://img.shields.io/badge/docker-ready-blue)
![MongoDB](https://img.shields.io/badge/MongoDB-3.6_→_8.x-green)

</div>

---

## Why MongoStudio?

MongoStudio is a lightweight, stunning MongoDB GUI that runs in a single Docker container. Connect with just a connection string and start browsing your data instantly.

- **⚡ Blazing Fast** — Sub-100ms query rendering, zero bloat
- **🎨 Beautiful UI** — Dark, minimal design inspired by Linear & Vercel
- **🐳 Single Docker Container** — One command to deploy, runs anywhere
- **🔄 MongoDB 3.6 → 8.x** — Auto-detects version, adapts API calls automatically
- **🧭 Operate Center** — Insert/Delete/Update/Bulk with method-level control
- **🖥️ Console Modes** — ConsoleUI + optional real `mongo` / `mongosh` with auto-version routing
- **🛠️ Server Management** — mongodump, mongorestore, mongoexport, mongoimport, mongostat, mongotop, mongofiles with legacy/modern auto-fallback
- **🔑 Audit & Logs** — action/source/method/scope filters + JSON/CSV export
- **🔐 Admin Access Control** — Optional `ADMIN_ACCESS_KEY` to gate Console and Server Management
- **👤 Connection Profiles** — Save, edit, export/import profiles with optional password and admin key
- **🧪 Try Demo** — mock mode synced with real UI/API flow (except real shells and server tools)
- **🔒 Secure** — Helmet.js, non-root container, admin-gated sensitive tools, no MongoDB document data stored server-side
- **📦 Lightweight** — ~50MB Docker image, minimal dependencies

## Quick Start

Demo: https://beztebya666.github.io/mongostudio/

If you pull from a registry, use the same fully qualified image name in `docker run`.
Examples below use `mongostudio` for a locally built image or a local tag alias.

```bash
# Pull from ghcr
docker pull ghcr.io/beztebya666/mongostudio:latest
```

```bash
# Run ghcr image
docker run -d -p 3141:3141 --name mongostudio ghcr.io/beztebya666/mongostudio:latest
```

```bash
# Pull from docker.hub
docker pull beztebya666/mongostudio:latest
```

```bash
# Run docker.hub image
docker run -d -p 3141:3141 --name mongostudio beztebya666/mongostudio:latest
```

Open **http://localhost:3141** and paste your connection string.

Optional local alias if you prefer shorter commands:

```bash
docker tag ghcr.io/beztebya666/mongostudio:latest mongostudio
# or
docker tag beztebya666/mongostudio:latest mongostudio
```

```bash
# Build
docker build -t mongostudio .

# Run
docker run -d -p 3141:3141 --name mongostudio mongostudio
```

Open **http://localhost:3141** and paste your connection string.

### Connecting to localhost MongoDB

If MongoDB runs on your host machine or in another Docker container:

```bash
# Linux opt 1 (recommended if MongoDB is in Docker too, same custom network)
docker run -d --name mongostudio --network mongo-net -p 3141:3141 mongostudio
# Then connect with container DNS name, for example: mongodb://mongo:27017

# Linux opt 2 (if MongoDB runs on Linux host machine)
docker run -d -p 3141:3141 --add-host=host.docker.internal:host-gateway mongostudio
# Then connect with: mongodb://host.docker.internal:27017

# macOS / Windows - works automatically
docker run -d -p 3141:3141 mongostudio
# Connect with: mongodb://host.docker.internal:27017
```

### Custom port

```bash
docker run -d -p 8080:3141 mongostudio
# Open http://localhost:8080
```

### Production recommended flags

```bash
docker run -d \
  -p 3141:3141 \
  --name mongostudio \
  --restart unless-stopped \
  --memory 512m \
  --cpus 1 \
  mongostudio
```

### Admin access key

Set `ADMIN_ACCESS_KEY` to require a key for Console and Server Management features:

```bash
docker run -d -p 3141:3141 \
  -e ADMIN_ACCESS_KEY="your-secret-key" \
  --name mongostudio mongostudio
```

When set, users must enter the key in Settings before accessing Console or Server Management. When not set, all features are accessible without a key.

## MongoDB Version Compatibility

MongoStudio auto-detects your MongoDB server version on connect and adapts its behavior:

| MongoDB Version | Support Level | Notes |
|-----------------|---------------|-------|
| **8.x** | ✅ Full | All features |
| **7.x** | ✅ Full | All features |
| **6.x** | ✅ Full | All features |
| **5.x** | ✅ Full | All features |
| **4.4** | ✅ Full | All features |
| **4.2** | ✅ Full | `$unionWith` unavailable |
| **4.0** | ✅ Full | `$merge`, `$unionWith` unavailable |
| **3.6** | ⚡ Good | Uses legacy `count()`, no transactions |
| **< 3.6** | ❌ | Upgrade required (driver limitation) |

The UI shows a version badge and warnings banner when connected to older versions.

### How version detection works

1. On connect, MongoStudio runs `buildInfo` to get the exact version
2. If that fails (restricted permissions), it falls back to wire protocol version detection via `hello` / `isMaster`
3. A capabilities matrix is built and stored for the session
4. Every API call uses version-appropriate methods (e.g., `countDocuments()` on 3.6+, legacy `count()` on older)
5. Aggregation pipeline stages are pre-checked against version before execution

### Console auto-version routing

MongoStudio ships two shell binaries in Docker:
- **mongosh 2.x** — for MongoDB 4.2+
- **mongosh 1.x** (as `mongosh1`) — for MongoDB 3.6–4.0

Regardless of which Console tab the user selects, the server automatically routes to the correct binary based on the connected MongoDB version. Old servers always get mongosh 1.x, new servers always get mongosh 2.x.

### Server Management tool auto-fallback

Server tools (mongodump, mongoexport, etc.) ship in two variants:
- **Modern** — current `mongodb-database-tools` (MongoDB 4.2+)
- **Legacy** — MongoDB 4.0.x line tools (`*_legacy` binaries)

The server probes both and automatically falls back to legacy if the modern binary reports wire version incompatibility.

## Features

- **Database Explorer**: Browse databases and collections in a resizable sidebar with instant search.
- **Document Browser**: Paginated documents with expandable JSON rows, syntax highlighting, inline edit/delete, and MongoDB filter bar.
- **Query Console**: Shell-style syntax (`db.collection.find({})`, `.aggregate([])`, `.distinct()`), templates, and execution time tracking.
- **Operate Modal**: Scoped write operations with Insert/Delete/Update/Bulk tabs, method presets, and safety preflight for risky actions.
- **Console (UI + Real Shell)**: ConsoleUI for guided commands plus optional real-shell modes (`mongo` legacy / `mongosh`) when binaries are installed.
- **Server Management**: Run mongodump, mongorestore, mongoexport, mongoimport, mongostat, mongotop, mongofiles directly from the UI with form-based configuration and live output.
- **Index Manager**: View, create, and drop indexes with unique/sparse/TTL support and wildcard-version validation.
- **Document Editor**: Full JSON editor with line numbers, tab indentation, keyboard shortcuts (`Ctrl/Cmd+Enter` to save, `Esc` to cancel), and syntax validation.
- **Schema Analyzer**: Auto-detect field types and frequency across sampled documents.
- **Audit & Logs**: Track connect/query/write/admin events with source/method/scope filters and one-click JSON/CSV export.
- **Connection Profiles**: Save, edit, delete connection profiles with optional password and admin key storage. Export/import profiles as JSON.
- **Try Demo Mode**: Built-in demo dataset with reset support and mock API flow aligned with the real UI.

## Architecture

```
HTML: index.html (SPA, #root)
CSS: Tailwind CSS + custom index.css (CSS variables, dark/light)
JS/Frontend: React 18 (JSX), no TypeScript
Build: Vite + PostCSS + Autoprefixer
API client: fetch (custom src/utils/api.js)
Backend: Node.js + Express
MongoDB: official MongoDB driver (no ORM)
Security/API middleware: helmet, cors, compression, express-rate-limit
Deployment: Docker (multi-stage, node:20-bookworm-slim)
Shell binaries: mongosh 2.x, mongosh 1.x, mongodb-database-tools, legacy mongo tools (all via tarballs)
```

## Local Development

```bash
npm install
npm run dev
# Frontend: http://localhost:5173
# Backend:  http://localhost:3141
```

### Console shell check (Node.js)

Use this command to verify that both shell binaries are available for real Console mode:

```bash
npm run check:shells
```

Optional JSON output:

```bash
node server/check-shells.mjs --json
```

Readiness endpoint (no `X-Connection-Id` required):

```bash
curl http://localhost:3141/api/health
# { status: "ok", ready: true, ... }
```

Status endpoint requires active connection and supports shell-probe refresh:

```bash
curl http://localhost:3141/api/status \
  -H "X-Connection-Id: <connectionId>"

curl "http://localhost:3141/api/status?refresh=1" \
  -H "X-Connection-Id: <connectionId>"
```

MongoStudio does not silently remap `mongo` to `mongosh`.
Each mode is checked independently and reported as available/unavailable.

### Security/Ops Notes (v2.6)

- `/api/service-config` is connection-gated; `PUT` requires execution mode `power`.
- Invalid JSON in `filter` / `sort` / `projection` now returns `400 validation` (no silent `{}` fallback on critical routes).
- Real shell auth uses URI auth path (minimal strip): credentials are not injected to process argv.
- Audit coverage includes preflight and exact-total lifecycle (`exact_total_start`, `exact_total_done|timeout|error`).
- Docker healthcheck uses `/api/health`, so container readiness is independent from active Mongo session.
- Admin access key endpoints (`/api/admin-access/*`) gate Console and Server Management when `ADMIN_ACCESS_KEY` is set.
- Server Management endpoints are protected by admin key when configured.
- Service-wide rate-limit / governor / cache settings persist in `server/service-config.json`.
- All MongoDB binaries in Docker are installed via direct tarballs (no apt repo dependency).

### Release QA (Manual)

- Manual parity + smoke checklist: [docs/QA-MANUAL.md](docs/QA-MANUAL.md)
- Current release scope is a stable OSS/manual-QA release. Remaining strict production-readiness gaps are tracked in [docs/release-v-3-0-0-tracker.md](docs/release-v-3-0-0-tracker.md).

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `3141` | Server port |
| `NODE_ENV` | `development` | Set to `production` in Docker |
| `MONGO_BIN` | `mongo` | Legacy shell binary used by Console legacy mode |
| `MONGOSH_BIN` | `mongosh` | Modern shell binary used by Console mongosh mode |
| `ADMIN_ACCESS_KEY` | *(none)* | When set, requires this key to access Console and Server Management |
| `STATUS_SHELL_RUNTIME_TTL_MS` | `15000` | TTL cache for runtime shell probe in `/api/status` |
| `MONGOSTAT_BIN` | `mongostat` | Path to mongostat binary |
| `MONGOTOP_BIN` | `mongotop` | Path to mongotop binary |
| `MONGODUMP_BIN` | `mongodump` | Path to mongodump binary |
| `MONGORESTORE_BIN` | `mongorestore` | Path to mongorestore binary |
| `MONGOEXPORT_BIN` | `mongoexport` | Path to mongoexport binary |
| `MONGOIMPORT_BIN` | `mongoimport` | Path to mongoimport binary |
| `MONGOFILES_BIN` | `mongofiles` | Path to mongofiles binary |
| `MONGOSTAT_LEGACY_BIN` | `mongostat_legacy` | Legacy mongostat for MongoDB < 4.2 |
| `MONGOTOP_LEGACY_BIN` | `mongotop_legacy` | Legacy mongotop for MongoDB < 4.2 |
| `MONGODUMP_LEGACY_BIN` | `mongodump_legacy` | Legacy mongodump for MongoDB < 4.2 |
| `MONGORESTORE_LEGACY_BIN` | `mongorestore_legacy` | Legacy mongorestore for MongoDB < 4.2 |
| `MONGOEXPORT_LEGACY_BIN` | `mongoexport_legacy` | Legacy mongoexport for MongoDB < 4.2 |
| `MONGOIMPORT_LEGACY_BIN` | `mongoimport_legacy` | Legacy mongoimport for MongoDB < 4.2 |
| `MONGOFILES_LEGACY_BIN` | `mongofiles_legacy` | Legacy mongofiles for MongoDB < 4.2 |

## License

MIT — use it however you want.

---

<div align="center">
  <sub>Built with ⚡ for the open source community</sub>
</div>
