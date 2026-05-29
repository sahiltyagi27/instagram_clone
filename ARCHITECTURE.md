# Architecture

A layer-by-layer walkthrough of the service. For the high-level diagrams,
API reference, and setup, see [`README.md`](./README.md).

## What is this project?

A backend service that replicates the core mechanics of Instagram — uploads,
stories, a social graph (follows), likes/comments, and a fan-out feed. There is
no frontend; it is a pure REST API. Structured data lives in **Postgres**, the
feed and rate-limit counters live in **Redis**, media bytes live in **S3/MinIO**,
and asynchronous work flows over **Kafka**.

---

## Project Structure

```
cmd/server/main.go          ← entry point, wires everything together
internal/
  handler/                  ← HTTP layer (routes, request parsing, responses)
  service/                  ← business logic, validation, error translation
  store/                    ← data access: Postgres + Redis, pg error-code mapping
  model/                    ← data types shared across layers
  kafka/                    ← event producer and consumer (retry + DLQ)
  middleware/               ← JWT auth, per-user rate limiting
  telemetry/                ← metrics, tracing, Kafka header carrier
  db/                       ← Postgres pool + Redis client constructors
  migrations/               ← golang-migrate SQL, applied on startup
```

The dependency direction is strictly **handler → service → store**. Handlers
never touch the database directly; stores never contain business rules.

---

## Layers

### 1. Entry Point — `main.go`

Everything is constructed and connected here. On startup it:

- Initialises the structured logger (`slog`, JSON output)
- Connects to Jaeger for distributed tracing (OTel) — warns and continues if unavailable
- Opens the Postgres pool and runs pending migrations
- Connects to Redis
- Creates all stores (`User`, `Media`, `Story`, `Feed`, `Follow`, `Like`, `Comment`)
- Creates the S3/MinIO `Storage` client
- Creates all services and the Kafka producer/consumer
- Starts the Kafka consumer and the pending-cleanup worker as goroutines
- Registers HTTP routes (grouped by rate-limit budget) on a chi router
- Listens on `:8080`

On `SIGTERM`/`Ctrl+C` it shuts the HTTP server down gracefully within 10 seconds.

---

### 2. Auth — `service/auth.go` + `handler/auth.go`

**Endpoints:** `POST /auth/signup` and `POST /auth/login` (both unauthenticated)

- **Signup:** hashes the password with **bcrypt**, generates a random user ID,
  persists the user in Postgres, and issues a **JWT** (HS256).
- **Login:** looks up the user by email, verifies the bcrypt hash, issues a new JWT.

The JWT carries only the `sub` (user ID) claim and expires in 24 hours. The
`PasswordHash` is stripped from every response.

---

### 3. JWT Middleware — `middleware/auth.go`

All routes except `/auth/*` and `/metrics` are protected. The middleware reads
`Authorization: Bearer <token>`, validates signature and expiry, extracts the
user ID from the `sub` claim, and stores it in the request context. Handlers
read it via `userIDFromRequest(r)` — they never trust a `user_id` in the body.

---

### 4. Rate Limiting — `middleware/rate_limit.go`

Per-user GCRA limiting backed by Redis, keyed `ratelimit:{scope}:{userID}`:

- `RateLimit(scope, rate)` — fixed scope, used for the write (20/min) and read
  (60/min) groups.
- `RateLimitByMethod(writeRate, readRate)` — charges `GET`/`HEAD` to the read
  budget and mutating methods to the write budget. Used for the social routes,
  which mix reads and writes under the same path prefixes and so can't be split
  across groups by mounting.

If Redis errors, the limiter **fails open** (allows the request) so a cache
outage doesn't take the API down. Over-limit requests get `429` with
`Retry-After` and `X-RateLimit-*` headers.

---

### 5. Media Upload — `service/storage.go` + `handler/upload.go`

A **two-step upload flow** — the client uploads directly to S3/MinIO, never
through the app server.

**Step 1 — `POST /presigned-url`**
- Generates a random `media_id` and an S3 key `users/{user_id}/{media_id}/{filename}`
- Calls the AWS SDK's `PresignPutObject` for a 15-minute signed PUT URL
- Persists a `media` row with `status = pending`
- Returns `media_id` + `upload_url`

**Step 2 — `POST /media/confirm`**
- Verifies the media belongs to the authenticated user
- Marks it `uploaded` and stamps `uploaded_at` — **idempotent**: re-confirming
  matches an already-uploaded row and preserves `uploaded_at` via `COALESCE`
- Publishes a `media-uploaded` Kafka event
- Returns the updated media

`GET /media/{id}` fetches a record by ID. The presign client uses the public
endpoint (`localhost:9000`) while the internal SDK client uses the Docker
endpoint (`minio:9000`), kept separate so generated URLs work from the host
without breaking the signature.

---

### 6. Kafka — `kafka/producer.go` + `kafka/consumer.go`

**Topics:**

| Topic | Published by | Consumed by | Dead-letter |
|---|---|---|---|
| `media-uploaded` | `POST /media/confirm` | `consumeMedia` goroutine | `media-uploaded-dlq` |
| `story-uploaded` | `POST /stories/confirm` | `consumeStories` goroutine | `story-uploaded-dlq` |

**Producer:** serialises the event as JSON, injects the current OTel trace
context into the message headers, and writes to the topic.

**Consumer:** one goroutine per topic, each an infinite `FetchMessage` loop with
**manual offset commits** (commit only after success → at-least-once delivery).
Each message starts a child span from the header trace context. The
`media-uploaded` handler runs `MediaProcessor.Process()` then
`FeedService.FanoutFeedItem()` as a single retryable unit.

**Failure handling:**
- Transient failures retry with exponential backoff (1s, 2s, 4s; 3 attempts).
- Exhausted or malformed messages are routed to `<topic>-dlq`, and the source
  offset is committed **only after** the DLQ write succeeds — because a Kafka
  commit advances the partition high-water mark, committing past an
  un-dead-lettered message would silently lose it. The DLQ write is retried
  until it succeeds (or shutdown).
- On shutdown (context cancelled mid-process) the offset is left uncommitted so
  the still-valid message replays on restart, rather than being dead-lettered.

---

### 7. Media Processor — `service/processor.go`

Called by the consumer after a photo upload is confirmed.

**For photos:**
1. Downloads the original from S3
2. Creates a **150×150 thumbnail** (center-cropped) with the `imaging` library
3. Creates a **640×640 medium** version (fit within bounds)
4. Uploads all three back to S3 at `{key}/thumb`, `{key}/medium`, `{key}/original`

**For videos:** logs "transcoding queued" — not yet implemented.

Every step is wrapped in an OTel span, so the pipeline is visible in Jaeger.

---

### 8. Stories — `service/story.go` + `handler/story.go`

Same two-step presign flow as media, with a **24-hour TTL**.

| Endpoint | Description |
|---|---|
| `POST /stories/presigned-url` | Presigned URL under `stories/{user_id}/{story_id}/` |
| `POST /stories/confirm` | Marks active, sets `expires_at = now + 24h`, publishes event |
| `GET /stories/{id}` | Returns the story only if active (confirmed, not expired) |
| `GET /stories/user/{user_id}` | All active stories for a user |

A background cleanup worker periodically deletes pending uploads that were never
confirmed within the TTL window and physically removes expired stories.

---

### 9. Social Graph — follows, likes, comments

**Follows** (`store/follow_store.go`, `service/follow.go`, `handler/follow.go`):
follow/unfollow and followers/following listings. The `follows` table has a
composite PK and a `no_self_follow` check constraint; follow is idempotent via
`ON CONFLICT DO NOTHING`. `GetFollowers` drives feed fan-out.

**Likes** (`*_store.go` / `like.go`): idempotent like/unlike on media, plus a
status query returning `{ count, liked }`. FK violations map to "media not found".

**Comments** (`comment.go`): create (body trimmed, validated ≤ 2200 chars),
list newest-first, and delete. Delete is scoped by `media_id` **and** owner, so
a user can only remove their own comment and only under the correct media.

Postgres error codes are translated centrally in `store/errors.go`
(`23505` unique → 409, `23503` FK → 404, `23514` check → 400).

---

### 10. Feed — `store/feed_store.go` + `service/feed.go` + `handler/feed.go`

The feed is a per-user Redis sorted set `feed:{userID}`, scored by upload time
(`CreatedAt.UnixMilli()` with a CRC32 tie-breaker suffix). It is built
**fan-out-on-write**: when media is processed, `FanoutFeedItem` writes the item
into the author's feed and every follower's feed (capped at 1000 items each).
This happens inside the Kafka consumer, so it inherits the retry/DLQ guarantees.

`GET /feed/{user_id}` returns items newest-first using a **compound cursor**
(`score + media_id`) instead of an offset — paging through it never skips or
duplicates items, even when two uploads share a millisecond. The response
carries a `next_cursor` (empty when exhausted). Only the authenticated user can
read their own feed.

A `nil` follower-lister disables fan-out (author-only feed), which keeps the
feed service unit-testable without Postgres.

---

### 11. Observability

**Structured logging** — `slog` with JSON output throughout.

**Distributed tracing** — OTel trace context flows through the full lifecycle:

```
HTTP request → handler span
  → Kafka publish span (headers carry W3C trace context)
    → Kafka consume span
      → MediaProcessor span
        → processPhoto span
```

Visible in Jaeger at <http://localhost:16686>.

**Metrics** — Prometheus counters/histograms at `/metrics` (including
`kafka_messages_consumed_total` with `ok`/`error`/`dlq_error`/`commit_error`
statuses), scraped every 15s and visualised in Grafana.

---

## Infrastructure (Docker Compose)

| Service | Port | Purpose |
|---|---|---|
| App | 8080 | The Go API |
| Postgres | 5432 | Structured data |
| Redis | 6379 | Feed + rate-limit counters |
| MinIO | 9000 / 9001 | Local S3 (API / Browser UI) |
| Kafka | 9092 | Event bus |
| Kafka UI | 8090 | Browse topics and messages |
| Jaeger | 16686 / 4318 | Trace viewer / OTLP receiver |
| Prometheus | 9090 | Metrics store |
| Grafana | 3000 | Dashboards |

---

## Persistence

All durable state lives in Postgres (users, media, stories, follows, likes,
comments) and Redis (feed sorted sets, rate-limit counters); media bytes live in
S3/MinIO. The app is stateless and can be restarted or scaled horizontally
without data loss. Schema changes are versioned in `internal/migrations/` and
applied automatically on startup via golang-migrate.
