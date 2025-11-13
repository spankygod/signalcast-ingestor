# SignalCast Ingestor – AI Rules

## Project Identity
- This is a backend ingestion worker that orchestrates multiple long-lived workers, not a web server or UI project. `src/index.ts` just wires worker lifecycles and signal handling—keep it that way.
- The service’s mission is to ingest Polymarket data, filter it to the politics niche, queue normalized jobs, and persist them. Do not introduce REST endpoints, client bundles, or other app scaffolding.

## Runtime & Commands
- Use the existing npm scripts: `npm run dev` (ts-node entrypoint), `npm run build` (tsc), `npm start` (node dist), `npm run lint` (type-check). Tests are intentionally absent.
- Production is managed by PM2 via `pm2.config.js` running `dist/index.js`. Keep deployment changes compatible with that single-process worker model.

## Worker Graph
- Polling workers (`events-poller`, `markets-poller`, `outcomes-poller`) are interval timers that push normalized jobs into the Redis updates queue; new pollers must follow the same pattern and respect `settings.*PollIntervalMs`.
- `market-channel` is the only websocket consumer; reuse its reconnect/backoff logic for any streaming changes instead of opening extra sockets.
- `db-writer` is the single queue consumer. Extend `UpdateKind`, `pushUpdate`, and the `handleJob` switch when introducing new payloads rather than writing to Postgres from pollers.
- `heartbeat` monitors queue depth and worker liveness. New loops must report with `heartbeatMonitor.beat/markIdle` so alerts stay consistent.

## Data & Integrations
- We do **not** use Drizzle. The canonical schema lives in `db/db_schema.sql`, and runtime DB access should use the `postgres` client plus the helpers in `src/lib/db.ts` (to be kept simple/raw). Never import or scaffold `drizzle-orm`.
- Any schema change must be applied both to `db/db_schema.sql` and whatever raw SQL layer replaces/extends `src/lib/db.ts`.
- Redis connectivity already has a no-op fallback—call the exported helpers (`enqueue`, `dequeue`, `setCache`, etc.) and check `redisAvailable` before assuming success.
- Normalize external data through `normalizeEvent`, `normalizeMarket`, `normalizeOutcome`, and `normalizeTick` so downstream jobs all share the same shape/relevance math.
- Respect the existing politics-only filtering in `politicsFilter`; modify it there if you need to expand/contract coverage rather than bypassing the filter inside workers.

## Coding Standards
- Stick to the TypeScript settings in `tsconfig.json` (CommonJS, `strict`, Node libs). Don’t flip module systems or loosen types without project-wide coordination.
- Use the shared Winston logger for all operational logging so messages hit `logs/ingestor.log` and `logs/error.log`. Avoid stray `console.*` except in bootstrap/tests.
- Reuse the `retryWithBackoff` + `sleep` helpers whenever you need throttled retries.

## Operational Safety
- Environment variables (`DATABASE_URL`, Redis, Polymarket keys, etc.) drive everything. Fail fast with clear errors when they’re missing, and never log actual secret values.
- Queue depth logging already happens inside heartbeat; keep Redis write volume low and batch where possible.
- `.env` contains real credentials—treat them as sensitive (no echoing, no check-ins).

