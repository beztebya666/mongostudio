# MongoStudio Docker image (multi-stage)
#
# What this image does:
# - Stage 1: builds frontend with Vite
# - Stage 2: runs Node.js backend + built frontend
# - Ships full app features: Query, Operate, Console, Audit, Try Demo
# - Installs Console shell binaries:
#   - mongosh 2.x  (MongoDB 4.2+) via apt
#   - mongosh1 1.x (MongoDB 3.6+) via GitHub tarball — used as MONGO_BIN
#   - mongodb-database-tools: mongodump, mongoexport, etc.
#
# Build:
#   docker build -t mongostudio .
#
# Run:
#   docker run -d -p 3141:3141 --name mongostudio mongostudio
#
# Open:
#   http://localhost:3141
#
# Optional run flags:
#   --restart unless-stopped
#   --memory 512m
#   --cpus 1
#   --network host   (Linux only, for host MongoDB access)
#
# Linux options:
#   Opt 1 (recommended if MongoDB is in Docker too, same custom network):
#     docker run -d --name mongostudio --network mongo-net -p 3141:3141 mongostudio
#     URI example: mongodb://mongo:27017
#   Opt 2 (if MongoDB runs on Linux host machine):
#     docker run -d -p 3141:3141 --add-host=host.docker.internal:host-gateway mongostudio
#     URI: mongodb://host.docker.internal:27017
#   macOS/Windows:
#     host.docker.internal works by default
#
# Connect to remote MongoDB:
#   Paste connection URI in UI, for example:
#   mongodb+srv://user:pass@cluster.mongodb.net/mydb

# Stage 1: build frontend
FROM node:20-bookworm-slim AS builder

WORKDIR /app

COPY package.json package-lock.json* ./
RUN npm ci --prefer-offline 2>/dev/null || npm install

COPY . .
RUN npm run build

# Stage 2: production runtime
FROM node:20-bookworm-slim

LABEL org.opencontainers.image.title="MongoStudio"
LABEL org.opencontainers.image.description="Blazing-fast MongoDB UI with Query, Operate, Console, Audit, and Try Demo (MongoDB 3.6 through 8.x)"
LABEL org.opencontainers.image.version="2.6.0"
LABEL org.opencontainers.image.licenses="MIT"

WORKDIR /app

# Install shell binaries for Console real-shell mode.
# All binaries installed via direct tarball downloads (no apt repo dependency).
# mongosh 2.x (MongoDB 4.2+) via GitHub tarball.
# mongosh 1.x (MongoDB 3.6+) as mongosh1 via GitHub tarball — used as MONGO_BIN.
# mongodb-database-tools (new): mongodump, mongoexport, mongoimport, mongorestore, mongostat, mongotop, mongofiles.
# legacy mongo tools (MongoDB 4.0 line): *_legacy binaries for MongoDB < 4.2.
RUN set -eux; \
    apt-get update; \
    apt-get install -y --no-install-recommends ca-certificates curl wget; \
    # mongosh 2.x
    curl -fsSL https://github.com/mongodb-js/mongosh/releases/download/v2.5.0/mongosh-2.5.0-linux-x64.tgz -o /tmp/mongosh2.tgz; \
    tar -xzf /tmp/mongosh2.tgz -C /tmp; \
    cp -a /tmp/mongosh-2.5.0-linux-x64/bin/* /usr/local/bin/; \
    chmod +x /usr/local/bin/mongosh; \
    # mongosh 1.x (legacy compat for MongoDB 3.6+)
    curl -fsSL https://github.com/mongodb-js/mongosh/releases/download/v1.10.0/mongosh-1.10.0-linux-x64.tgz -o /tmp/mongosh1.tgz; \
    tar -xzf /tmp/mongosh1.tgz -C /tmp; \
    cp -a /tmp/mongosh-1.10.0-linux-x64/bin/* /usr/local/bin/; \
    mv /usr/local/bin/mongosh /usr/local/bin/mongosh1; \
    chmod +x /usr/local/bin/mongosh1; \
    # restore mongosh 2.x (overwritten by mongosh 1.x copy)
    cp -a /tmp/mongosh-2.5.0-linux-x64/bin/mongosh /usr/local/bin/mongosh; \
    chmod +x /usr/local/bin/mongosh; \
    # mongodb-database-tools (modern)
    curl -fsSL https://fastdl.mongodb.org/tools/db/mongodb-database-tools-debian12-x86_64-100.12.0.tgz -o /tmp/db-tools.tgz; \
    tar -xzf /tmp/db-tools.tgz -C /tmp; \
    DB_TOOLS_DIR="$(find /tmp -maxdepth 1 -type d -name 'mongodb-database-tools-*' | head -n 1)"; \
    test -n "$DB_TOOLS_DIR"; \
    for tool in mongostat mongotop mongodump mongorestore mongoexport mongoimport mongofiles; do \
      cp "$DB_TOOLS_DIR/bin/$tool" "/usr/local/bin/$tool"; \
      chmod +x "/usr/local/bin/$tool"; \
    done; \
    # legacy mongo tools (MongoDB 4.0 line)
    curl -fsSL https://fastdl.mongodb.org/linux/mongodb-linux-x86_64-4.0.28.tgz -o /tmp/mongo-tools-legacy.tgz; \
    tar -xzf /tmp/mongo-tools-legacy.tgz -C /tmp; \
    LEGACY_DIR="$(find /tmp -maxdepth 1 -type d -name 'mongodb-linux-*4.0.28' | head -n 1)"; \
    test -n "$LEGACY_DIR"; \
    for tool in mongostat mongotop mongodump mongorestore mongoexport mongoimport mongofiles; do \
      cp "$LEGACY_DIR/bin/$tool" "/usr/local/bin/${tool}_legacy"; \
      chmod +x "/usr/local/bin/${tool}_legacy"; \
    done; \
    # cleanup tarballs only — keep curl deps (libgssapi-krb5-2 etc) needed by MongoDB binaries
    rm -rf /tmp/mongosh2.tgz /tmp/mongosh-2.5.0-linux-x64 \
           /tmp/mongosh1.tgz /tmp/mongosh-1.10.0-linux-x64 \
           /tmp/db-tools.tgz "$DB_TOOLS_DIR" \
           /tmp/mongo-tools-legacy.tgz "$LEGACY_DIR"; \
    dpkg --remove --force-depends curl; \
    rm -rf /var/lib/apt/lists/*; \
    # Verify all binaries work
    echo "=== Verifying binaries ==="; \
    mongosh --version; \
    mongosh1 --version; \
    mongodump --version; \
    mongodump_legacy --version; \
    echo "=== All OK ==="

# Production dependencies only
COPY package.json package-lock.json* ./
RUN npm ci --omit=dev --prefer-offline 2>/dev/null || npm install --omit=dev \
    && npm cache clean --force \
    && rm -rf /tmp/* /root/.npm

# Backend + built frontend
COPY server/ ./server/
COPY --from=builder /app/dist ./dist

# Non-root user
RUN groupadd -g 1001 app \
    && useradd -m -u 1001 -g app app \
    && chown -R app:app /app
USER app

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
  CMD wget -qO- http://localhost:3141/api/health >/dev/null 2>&1 || exit 1

ENV NODE_ENV=production
ENV PORT=3141
ENV MONGOSH_BIN=mongosh
ENV MONGO_BIN=mongosh1
ENV MONGOSTAT_LEGACY_BIN=mongostat_legacy
ENV MONGOTOP_LEGACY_BIN=mongotop_legacy
ENV MONGODUMP_LEGACY_BIN=mongodump_legacy
ENV MONGORESTORE_LEGACY_BIN=mongorestore_legacy
ENV MONGOEXPORT_LEGACY_BIN=mongoexport_legacy
ENV MONGOIMPORT_LEGACY_BIN=mongoimport_legacy
ENV MONGOFILES_LEGACY_BIN=mongofiles_legacy

EXPOSE 3141

CMD ["node", "server/index.js"]
