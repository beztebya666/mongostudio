# ══════════════════════════════════════════════════════════════════════
#  ⚡ MongoStudio — Blazing-fast MongoDB UI
#
#  Build & run:
#    docker build -t mongostudio .
#    docker run -d -p 3141:3141 --name mongostudio mongostudio
#
#  Then open http://localhost:3141
#
#  Options:
#    -p 8080:3141          — map to custom port
#    --restart unless-stopped — auto-restart
#    --memory 512m         — memory limit
#    --cpus 1              — CPU limit
#    --network host        — use host network (for localhost MongoDB)
#
#  Connect to a local MongoDB running on the host:
#    Linux:   docker run -d -p 3141:3141 --add-host=host.docker.internal:host-gateway mongostudio
#             then connect with: mongodb://host.docker.internal:27017
#    macOS/Windows: host.docker.internal works out of the box
#
#  Connect to a remote MongoDB:
#    Just paste the connection string in the UI:
#    mongodb+srv://user:pass@cluster.mongodb.net/mydb
#
# ══════════════════════════════════════════════════════════════════════

# ─── Stage 1: Build Frontend ────────────────────────────────────────
FROM node:20-alpine AS builder

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci --prefer-offline 2>/dev/null || npm install

COPY . .
RUN npm run build

# ─── Stage 2: Production ────────────────────────────────────────────
FROM node:20-alpine

LABEL org.opencontainers.image.title="MongoStudio"
LABEL org.opencontainers.image.description="Blazing-fast MongoDB UI — supports MongoDB 2.6 through 8.x"
LABEL org.opencontainers.image.version="1.0.0"
LABEL org.opencontainers.image.licenses="MIT"

WORKDIR /app

# Production deps only
COPY package.json package-lock.json* ./
RUN npm ci --omit=dev --prefer-offline 2>/dev/null || npm install --omit=dev \
    && npm cache clean --force \
    && rm -rf /tmp/* /root/.npm

# Server code
COPY server/ ./server/

# Built frontend from stage 1
COPY --from=builder /app/dist ./dist

# Non-root user
RUN addgroup -g 1001 -S app && adduser -S app -u 1001
USER app

# Health check — works with version detection endpoint
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://localhost:3141/api/status >/dev/null 2>&1 || exit 1

ENV NODE_ENV=production
ENV PORT=3141

EXPOSE 3141

CMD ["node", "server/index.js"]
