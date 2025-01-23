# syntax = docker/dockerfile:1

FROM node:23.6.1-slim AS base
LABEL fly_launch_runtime="nodejs"
WORKDIR /app
ENV NODE_ENV=production

# Throw-away build stage to reduce size of final image
FROM base AS build

COPY --link package-lock.json package.json ./
# We cannot use a wildcard until `COPY --parents` is stabilised
# See https://docs.docker.com/reference/dockerfile/#copy---parents
COPY --link api/package.json ./api/
COPY --link indexer/package.json ./indexer/
COPY --link repository/package.json ./repository/
RUN npm ci --workspaces
COPY --link . .

# Final stage for app image
FROM base
COPY --from=build /app /app
# This argument controls the value used by npm to choose which workspace (subdir) to start
ENV NPM_CONFIG_WORKSPACE=""
CMD [ "npm", "start" ]
