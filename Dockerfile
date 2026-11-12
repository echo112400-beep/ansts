FROM node:20-bookworm-slim

WORKDIR /app

# Copy package manifests, pre-built dist/, and full TypeScript sources
COPY package.json tsconfig.json wrangler.json wrangler.toml Makefile ./
COPY types ./types
COPY specs ./specs
COPY proto ./proto
COPY migrations ./migrations
COPY scripts ./scripts
COPY src ./src
COPY tests ./tests

# dist/ is not tracked in git any more; it is always rebuilt from src/ + tests/ here
RUN npm install --no-audit --no-fund 2>/dev/null && npm run build

ENV NODE_ENV=production \
    ANSTS_HOST=0.0.0.0 \
    ANSTS_PORT=8080 \
    ANSTS_DASHBOARD_PORT=8090 \
    ANSTS_WORKER_NODES=3 \
    ANSTS_TICK_INTERVAL_MS=1000

EXPOSE 8080 8090

HEALTHCHECK --interval=5s --timeout=3s --start-period=3s --retries=3 \
  CMD node -e "fetch('http://127.0.0.1:' + (process.env.ANSTS_PORT || 8080) + '/healthz').then(r => process.exit(r.ok ? 0 : 1)).catch(() => process.exit(1))"

CMD ["node", "dist/src/server_main.js"]
