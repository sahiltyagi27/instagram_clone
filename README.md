# Instagram Clone — Backend Service

A production-shaped Go backend that replicates the core mechanics of Instagram: JWT auth, direct-to-S3 media uploads, asynchronous image processing, 24-hour stories, a **social graph** (follow/unfollow), **likes & comments**, and a **fan-out-on-write feed** — all wired together with Kafka, Postgres, Redis, and end-to-end observability (OpenTelemetry, Prometheus, Grafana).

There is no frontend; this is a pure REST API.

---

## Table of Contents

- [Highlights](#highlights)
- [Tech Stack](#tech-stack)
- [Architecture](#architecture)
  - [System overview](#system-overview)
  - [Code layout & layering](#code-layout--layering)
  - [Photo upload & processing (sequence)](#photo-upload--processing-sequence)
  - [Feed fan-out-on-write](#feed-fan-out-on-write)
  - [Reliability: retries & dead-letter queue](#reliability-retries--dead-letter-queue)
  - [Data model](#data-model)
  - [Distributed tracing](#distributed-tracing)
- [Quick Start](#quick-start)
- [API Reference](#api-reference)
- [Rate Limiting](#rate-limiting)
- [Environment Variables](#environment-variables)
- [Running Tests](#running-tests)
- [Observability Endpoints](#observability-endpoints)

---

## Highlights

- **Direct-to-S3 uploads** — clients PUT bytes straight to object storage via presigned URLs; the API server never proxies file data.
- **Asynchronous processing** — `POST /media/confirm` returns immediately and publishes a Kafka event; a background consumer transcodes thumbnails and fans the item out to followers' feeds.
- **Real social feed** — fan-out-on-write to a per-user Redis sorted set, with compound-cursor pagination (no gaps or duplicates, even for same-millisecond uploads).
- **Social graph** — follow/unfollow, followers/following listings, idempotent likes, and ownership-scoped comments.
- **At-least-once Kafka** — manual offset commits, retry-with-backoff, and a dead-letter queue that never advances past an un-dead-lettered message.
- **Per-user rate limiting** — independent read/write budgets backed by Redis (GCRA), failing open on cache outage.
- **Full observability** — W3C trace context propagated through HTTP → Kafka → processor, Prometheus metrics, and Grafana dashboards.

---

## Tech Stack

| Concern | Technology | Port |
|---|---|---|
| HTTP service | Go + [chi](https://github.com/go-chi/chi) router | `8080` |
| Auth | JWT (HS256) | — |
| Database | Postgres 16 (`pgx`/`pgxpool`) | `5432` |
| Feed / rate-limit cache | Redis 7 (`go-redis`) | `6379` |
| Object storage | MinIO (S3-compatible) | `9000` / `9001` (UI) |
| Event bus | Kafka + Zookeeper (`segmentio/kafka-go`) | `9092` |
| Tracing | OpenTelemetry → Jaeger | `16686` |
| Metrics | Prometheus | `9090` |
| Dashboards | Grafana | `3000` |
| Kafka browser | Kafka UI | `8090` |

---

## Architecture

### System overview

```mermaid
flowchart LR
    Client([Client])

    subgraph App["API Server (Go, :8080)"]
        direction TB
        MW["JWT auth · rate limit · OTel/Prom middleware"]
        H["HTTP handlers"]
        SVC["Services (business logic)"]
        ST["Stores (data access)"]
        MW --> H --> SVC --> ST
        CONS["Kafka consumer<br/>(2 goroutines)"]
        PROC["Media processor"]
        CONS --> PROC
    end

    Client -- "REST + Bearer JWT" --> MW
    Client -- "PUT bytes (presigned)" --> MinIO[("MinIO / S3")]

    ST -->|users, media, stories,<br/>follows, likes, comments| PG[("Postgres")]
    ST -->|feed sorted sets,<br/>rate-limit counters| Redis[("Redis")]
    SVC -->|publish events| Kafka{{"Kafka<br/>media-uploaded · story-uploaded"}}
    Kafka --> CONS
    PROC -->|download + upload variants| MinIO
    CONS -->|fan out feed items| Redis

    App -.->|traces| Jaeger[(Jaeger)]
    App -.->|metrics| Prom[(Prometheus)]
    Prom --> Grafana[(Grafana)]
```

The API server is a clean **handler → service → store** stack. Anything that
can be done off the request path (image processing, feed fan-out) is handed to
Kafka and performed by a background consumer, so write requests stay fast.

### Code layout & layering

```
cmd/server/main.go        ← entry point: builds stores, services, handlers, Kafka, routes
internal/
  handler/                ← HTTP layer: routing, request parsing, status mapping
  service/                ← business logic, validation, error translation
  store/                  ← data access (Postgres + Redis), pg error-code mapping
  model/                  ← shared data types and request/response shapes
  kafka/                  ← event producer + consumer (retry, DLQ)
  middleware/             ← JWT auth, per-user rate limiting
  telemetry/              ← OTel tracing, Prometheus metrics, Kafka header carrier
  db/                     ← Postgres pool + Redis client constructors
  migrations/             ← golang-migrate SQL (applied automatically on startup)
```

Each layer only knows about the one below it:

```mermaid
flowchart TD
    H[handler] --> S[service]
    S --> ST[store]
    ST --> PG[(Postgres)]
    ST --> R[(Redis)]
    S --> K[kafka producer]
    H -.uses.-> MW[middleware]
```

### Photo upload & processing (sequence)

A two-step presigned flow keeps file bytes off the API server, and all
processing happens asynchronously after the client confirms.

```mermaid
sequenceDiagram
    autonumber
    participant C as Client
    participant API as API Server
    participant S3 as MinIO/S3
    participant K as Kafka
    participant W as Consumer + Processor
    participant R as Redis

    C->>API: POST /presigned-url
    API->>API: create media (status: pending)
    API-->>C: media_id + presigned PUT url
    C->>S3: PUT file bytes (direct)
    C->>API: POST /media/confirm
    API->>API: status = uploaded, uploaded_at = now (idempotent)
    API->>K: publish media-uploaded
    API-->>C: 200 OK (returns immediately)

    Note over K,R: asynchronous, off the request path
    K->>W: consume media-uploaded
    W->>S3: download original
    W->>S3: upload thumb (150²) + medium (640²) + original
    W->>R: fan out feed item to author + followers
```

**Idempotent confirm:** re-confirming the same `media_id` is a no-op, not an
error — the upload's `uploaded_at` is preserved. **Idempotent processing:**
because Kafka is at-least-once, a redelivered event simply regenerates the same
variants and re-adds the same feed entry.

### Feed fan-out-on-write

The feed is a per-user Redis sorted set, `feed:{userID}`, scored by upload time.
When media is processed, the consumer writes the item into the author's feed
**and every follower's feed** — so reads are a single cheap range query.

```mermaid
flowchart TD
    EV["media-uploaded event<br/>(author = U)"] --> FO{FanoutFeedItem}
    FO -->|GetFollowers U| DB[(Postgres follows)]
    DB --> R1["ZADD feed:U"]
    DB --> R2["ZADD feed:follower_1"]
    DB --> R3["ZADD feed:follower_n"]
    R1 & R2 & R3 --> CAP["trim to newest 1000 items per feed"]
```

Reading `GET /feed/{user_id}` returns items newest-first using a **compound
cursor** (`score + media_id`) rather than an offset. This pages without gaps or
duplicates even when two uploads land in the same millisecond, and the response
carries a `next_cursor` (empty when there are no more pages). Each feed is
capped at the most recent 1000 items.

### Reliability: retries & dead-letter queue

The consumer commits offsets **manually**, only after a message is fully
processed, giving at-least-once delivery. Failures are retried with exponential
backoff; permanent failures are routed to a `<topic>-dlq` topic before the
offset advances.

```mermaid
flowchart TD
    M[FetchMessage] --> P{"process:<br/>transcode + fan out"}
    P -->|success| CM[commit offset]
    P -->|transient failure| RB["retry w/ backoff (3x)"]
    RB --> P
    P -->|exhausted / malformed| DLQ{"write to topic-dlq"}
    DLQ -->|success| CM
    DLQ -->|failure| BLOCK["block & retry DLQ write<br/>(offset NOT advanced)"]
    BLOCK --> DLQ
    P -->|shutdown ctx cancelled| LEAVE["leave offset uncommitted<br/>(replay on restart)"]
```

The key invariant: a Kafka commit advances the partition high-water mark, so the
consumer **never commits past a message it could not either process or
dead-letter**. On shutdown, in-flight work is left uncommitted and replayed.

### Data model

```mermaid
erDiagram
    users ||--o{ media     : uploads
    users ||--o{ stories   : posts
    users ||--o{ comments  : writes
    users ||--o{ likes     : gives
    users ||--o{ follows   : "follower / followee"
    media ||--o{ likes     : receives
    media ||--o{ comments  : receives

    users {
        varchar id PK
        varchar username
        varchar email UK
        varchar password_hash
        timestamptz created_at
    }
    media {
        varchar id PK
        varchar user_id FK
        varchar type "photo or video"
        varchar status "pending or uploaded"
        varchar s3_key
        timestamptz uploaded_at
    }
    stories {
        varchar id PK
        varchar user_id FK
        varchar s3_key
        timestamptz expires_at "now + 24h"
    }
    follows {
        varchar follower_id PK,FK
        varchar followee_id PK,FK
        timestamptz created_at
    }
    likes {
        varchar media_id PK,FK
        varchar user_id PK,FK
        timestamptz created_at
    }
    comments {
        varchar id PK
        varchar media_id FK
        varchar user_id FK
        varchar body "<= 2200 chars"
        timestamptz created_at
    }
```

Migrations live in `internal/migrations/` and are applied automatically on
startup. `follows` enforces a `no_self_follow` check constraint; `likes` and
`follows` use composite primary keys so re-inserts are idempotent via
`ON CONFLICT DO NOTHING`. Feed items live only in Redis, not Postgres.

### Distributed tracing

A single trace follows a request across process and transport boundaries — the
producer injects W3C trace context into Kafka headers, and the consumer
continues the same trace:

```
HTTP request span
└─ Kafka publish span        (headers carry W3C trace context)
   └─ Kafka consume span
      └─ MediaProcessor span
         └─ processPhoto span
```

View it all in Jaeger at <http://localhost:16686>.

---

## Quick Start

```sh
docker compose up --build
```

On first startup the stack self-initialises:

- `createbuckets` creates the `instagram-media` bucket in MinIO
- Kafka creates the `media-uploaded` / `story-uploaded` topics (plus their `-dlq` siblings)
- The app runs all pending Postgres migrations

Then walk through the API:

```sh
# 1. Sign up and capture a token
TOKEN=$(curl -s -X POST http://localhost:8080/auth/signup \
  -H "Content-Type: application/json" \
  -d '{"username":"sahil","email":"sahil@example.com","password":"secret123"}' \
  | jq -r .token)

# 2. Get a presigned URL
curl -s -X POST http://localhost:8080/presigned-url \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"file_name":"sunset.jpg","content_type":"image/jpeg","media_type":"photo"}'

# 3. PUT the file to the returned upload_url, then confirm
curl -s -X POST http://localhost:8080/media/confirm \
  -H "Authorization: Bearer $TOKEN" -H "Content-Type: application/json" \
  -d '{"media_id":"<media_id>"}'
```

---

## API Reference

All routes except `POST /auth/signup` and `POST /auth/login` require an
`Authorization: Bearer <token>` header. The authenticated user ID is taken from
the JWT `sub` claim — never from the request body. JSON errors always use the
shape `{ "error": "message" }`.

### Auth

| Method | Path | Description |
|---|---|---|
| `POST` | `/auth/signup` | Create a user, return user + JWT |
| `POST` | `/auth/login` | Verify credentials, return user + JWT |

### Media

| Method | Path | Description |
|---|---|---|
| `POST` | `/presigned-url` | Generate a presigned PUT URL (15 min TTL) |
| `POST` | `/media/confirm` | Mark uploaded, publish `media-uploaded` (idempotent) |
| `GET` | `/media/{id}` | Fetch media metadata |

### Stories (24-hour TTL)

| Method | Path | Description |
|---|---|---|
| `POST` | `/stories/presigned-url` | Generate a presigned URL for a story |
| `POST` | `/stories/confirm` | Mark active, set `expires_at = now + 24h` |
| `GET` | `/stories/{id}` | Fetch a story (only if active) |
| `GET` | `/stories/user/{user_id}` | List a user's active stories |

### Feed

| Method | Path | Description |
|---|---|---|
| `GET` | `/feed/{user_id}?limit=20&cursor=` | Cursor-paginated feed, newest first |

```sh
curl -s "http://localhost:8080/feed/$USER_ID?limit=20" -H "Authorization: Bearer $TOKEN"
# response: { "items": [...], "limit": 20, "next_cursor": "eyJ..." }
# pass next_cursor back as ?cursor= to fetch the next page
```

### Follows

| Method | Path | Description |
|---|---|---|
| `POST` | `/follows/{user_id}` | Follow a user (idempotent; self-follow → 400) |
| `DELETE` | `/follows/{user_id}` | Unfollow a user (no-op if not following) |
| `GET` | `/follows/{user_id}/followers` | List who follows this user |
| `GET` | `/follows/{user_id}/following` | List who this user follows |

### Likes

| Method | Path | Description |
|---|---|---|
| `PUT` | `/likes/{media_id}` | Like media (idempotent) |
| `DELETE` | `/likes/{media_id}` | Unlike media (no-op if not liked) |
| `GET` | `/likes/{media_id}` | `{ media_id, count, liked }` for the caller |

### Comments

| Method | Path | Description |
|---|---|---|
| `POST` | `/comments/{media_id}` | Add a comment (`{ "body": "..." }`, ≤ 2200 chars) |
| `GET` | `/comments/{media_id}?limit=50` | List comments, newest first |
| `DELETE` | `/comments/{media_id}/{comment_id}` | Delete own comment (scoped by media + owner) |

> Full machine-readable spec: [`openapi.yaml`](./openapi.yaml) — paste into
> [editor.swagger.io](https://editor.swagger.io) for interactive docs.

---

## Rate Limiting

Per-user GCRA rate limiting (backed by Redis) protects the API. There are two
independent budgets, keyed `ratelimit:{scope}:{userID}`:

| Scope | Limit | Applies to |
|---|---|---|
| `write` | 20 req/min | uploads, stories, and **mutating** social calls (POST/PUT/DELETE) |
| `read` | 60 req/min | feed reads and **safe** social calls (GET/HEAD) |

Social routes mix reads and writes on the same path prefixes, so they use a
method-aware limiter (`RateLimitByMethod`) that charges each request to the
matching budget. If Redis is unavailable the limiter **fails open** so a cache
outage never takes the whole API down. Exceeded limits return `429` with
`Retry-After` and `X-RateLimit-*` headers.

---

## Environment Variables

```text
# Postgres
DATABASE_URL=postgres://postgres:postgres@postgres:5432/instagram_clone

# Redis
REDIS_ADDR=redis:6379

# S3 / MinIO
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
S3_BUCKET=instagram-media
S3_ENDPOINT=http://minio:9000             # internal Docker endpoint (SDK client)
S3_PUBLIC_ENDPOINT=http://localhost:9000  # baked into presigned URLs (host-reachable)

# Auth
JWT_SECRET=dev-secret-do-not-use-in-prod  # required outside dev/local/test
APP_ENV=dev

# Kafka
KAFKA_BROKER=kafka:9092

# Tracing
OTEL_EXPORTER_OTLP_ENDPOINT=jaeger:4318
```

`S3_ENDPOINT` and `S3_PUBLIC_ENDPOINT` are deliberately distinct: the SDK talks
to MinIO over the Docker network, while presigned URLs must be reachable from
the host — keeping them separate avoids breaking the URL signature.

`JWT_SECRET` falls back to a development secret only when `APP_ENV` is empty,
`dev`, `local`, or `test`; it is required everywhere else.

---

## Running Tests

Integration tests need Postgres and Redis. They **skip automatically** when the
services are unavailable, so `go test ./...` is always safe to run.

```sh
# Bring up just the data stores
docker compose up -d postgres redis

# Run the full suite
go test ./...
```

Tests scope their own fixtures and clean up only the rows they own, so the suite
is safe to run in parallel across packages.

---

## Observability Endpoints

| Tool | URL | Notes |
|---|---|---|
| Jaeger | <http://localhost:16686> | Traces across HTTP → Kafka → processor |
| Grafana | <http://localhost:3000> | Dashboards (`admin` / `admin`) |
| Prometheus | <http://localhost:9090> | Raw metrics (scraped from `/metrics`) |
| Kafka UI | <http://localhost:8090> | Browse topics, messages, and DLQs |
| MinIO Console | <http://localhost:9001> | Browse buckets (`minioadmin` / `minioadmin`) |

---

## Notes & Current Limitations

- Photo processing generates thumbnail + medium + original variants. **Video
  transcoding is currently a stub** (the consumer logs and acks).
- The `story-uploaded` consumer currently just logs — story fan-out
  (notifications, analytics) is a future extension.
- For a deeper, layer-by-layer walkthrough see [`ARCHITECTURE.md`](./ARCHITECTURE.md).
