FROM node:20-alpine AS deps

WORKDIR /app
RUN corepack enable
COPY package.json pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile

FROM node:20-alpine AS builder

WORKDIR /app
RUN corepack enable
ENV NEXT_TELEMETRY_DISABLED=1
ARG NEXT_PUBLIC_API_BASE_URL=http://localhost:8000/api
ENV NEXT_PUBLIC_API_BASE_URL=$NEXT_PUBLIC_API_BASE_URL

COPY --from=deps /app/node_modules ./node_modules
COPY . .
RUN pnpm run build

FROM node:20-alpine AS runner

WORKDIR /app
RUN corepack enable
ENV NODE_ENV=production
ENV NEXT_TELEMETRY_DISABLED=1
ARG NEXT_PUBLIC_API_BASE_URL=http://localhost:8000/api
ENV NEXT_PUBLIC_API_BASE_URL=$NEXT_PUBLIC_API_BASE_URL

COPY --from=builder /app/package.json ./package.json
COPY --from=builder /app/pnpm-lock.yaml ./pnpm-lock.yaml
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/.next ./.next
COPY --from=builder /app/public ./public

EXPOSE 3000

CMD ["pnpm", "run", "start", "--", "--hostname", "0.0.0.0", "--port", "3000"]
