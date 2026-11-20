FROM node:24-bookworm-slim
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY deploy/open-orange/package.json deploy/open-orange/package-lock.json /opt/orange/
RUN npm ci --prefix /opt/orange --omit=dev --no-audit --no-fund && ln -s /opt/orange/node_modules/.bin/orange /usr/local/bin/orange
COPY src ./src
COPY migrations ./migrations
COPY deploy/celld ./deploy/celld
COPY scripts/check_open_orange.mjs scripts/init_open_orange_pool.mjs ./scripts/
ENTRYPOINT ["node", "scripts/check_open_orange.mjs", "--serve"]
