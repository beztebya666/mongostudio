<div align="center">

# ⚡ MongoStudio

**A blazing-fast, beautiful MongoDB UI**

Modern • Minimal • Open Source

![License](https://img.shields.io/badge/license-MIT-00ed64)
![Docker](https://img.shields.io/badge/docker-ready-blue)
![MongoDB](https://img.shields.io/badge/MongoDB-2.6_→_8.x-green)

</div>

---

## Why MongoStudio?

MongoStudio is a lightweight, stunning MongoDB GUI that runs in a single Docker container. Connect with just a connection string and start browsing your data instantly.

- **⚡ Blazing Fast** — Sub-100ms query rendering, zero bloat
- **🎨 Beautiful UI** — Dark, minimal design inspired by Linear & Vercel
- **🐳 Single Docker Container** — One command to deploy, runs anywhere
- **🔄 MongoDB 3.6 → 8.x** — Auto-detects version, adapts API calls automatically
- **🔒 Secure** — Helmet.js, non-root container, no data stored server-side
- **📦 Lightweight** — ~50MB Docker image, minimal dependencies

## 🚀 Quick Start

#### 🔹 Option 1 — Pull from Docker Hub (Recommended)

```bash
# Pull image
docker pull beztebya666/mongostudio:latest

# Run container
docker run -d -p 3141:3141 --name mongostudio beztebya666/mongostudio:latest
```

Open **http://localhost:3141** and paste your connection string.

#### 🔹 Option 2 — Pull from GitHub Container Registry (GHCR)

```bash
# Pull image
docker pull ghcr.io/beztebya666/mongostudio:latest

# Run container
docker run -d -p 3141:3141 --name mongostudio ghcr.io/beztebya666/mongostudio:latest
```

Open **http://localhost:3141** and paste your connection string.

#### 🔹 Option 3 — Build Locally

```bash
# Clone repository
git clone https://github.com/beztebya666/MongoStudio.git
cd MongoStudio

# Build image
docker build -t mongostudio .

# Run container
docker run -d -p 3141:3141 --name mongostudio mongostudio
```

Open **http://localhost:3141** and paste your connection string.

### Connecting to localhost MongoDB

If MongoDB runs on your host machine:

```bash
# Linux
docker run -d -p 3141:3141 --add-host=host.docker.internal:host-gateway mongostudio
# Then connect with: mongodb://host.docker.internal:27017

# macOS / Windows — works automatically
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

## Features

### Database Explorer
Browse all databases and collections in a resizable sidebar with instant search filtering.

### Document Browser
Paginated documents with expandable JSON rows, syntax highlighting, inline edit/delete, MongoDB query filter bar.

### Query Console
Shell-style syntax (`db.collection.find({})`, `.aggregate([])`, `.distinct()`), query templates, execution time tracking, aggregation pipeline support with version-aware warnings.

### Index Manager
View, create, and drop indexes. Shows unique/sparse/TTL properties. Validates wildcard indexes against server version.

### Document Editor
Full JSON editor with line numbers, tab indentation, keyboard shortcuts (⌘+Enter to save, Esc to cancel), syntax validation.

## Architecture

```
mongostudio/
├── server/index.js      # Express API + MongoDB compat layer
├── src/                  # React 18 frontend
│   ├── components/       # UI components
│   ├── utils/            # API client, formatters
│   └── index.css         # Tailwind + custom styles
├── Dockerfile            # Multi-stage production build
└── package.json
```

**Backend**: Express.js + native MongoDB driver. Version-aware compat layer handles API differences across MongoDB 2.6–8.x.

**Frontend**: React 18 + Vite + Tailwind CSS. Instant builds, tree-shaken, <200KB gzipped.

## Local Development

```bash
npm install
npm run dev
# Frontend: http://localhost:5173
# Backend:  http://localhost:3141
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `3141` | Server port |
| `NODE_ENV` | `development` | Set to `production` in Docker |

## License

MIT — use it however you want.

---

<div align="center">
  <sub>Built with ⚡ for the open source community</sub>
</div>
