# Architecture

## What is this project?

A backend service that replicates the core upload/feed/stories mechanics of Instagram. There is no frontend — it is a pure REST API. A user can register, upload photos/videos, post stories, and get a feed of their own uploaded content.

---

## Project Structure

```
cmd/server/main.go          ← entry point, wires everything together
internal/
  handler/                  ← HTTP layer (routes, request parsing, responses)
  service/                  ← business logic
  model/                    ← data types shared across layers
  kafka/                    ← event producer and consumer
  middleware/               ← JWT auth middleware
  telemetry/                ← metrics, tracing, Kafka header carrier
```

---

## Layers

### 1. Entry Point — `main.go`

Everything gets created and connected here. On startup it:

- Initialises the structured logger (`slog`)
- Connects to Jaeger for distributed tracing (OTel)
- Creates a `Storage` (S3/MinIO client)
- Creates all services: `AuthService`, `StoryService`, `FeedService`, `MediaProcessor`
- Creates a Kafka producer and consumer
- Starts the story expiry background worker
- Starts the Kafka consumer in a goroutine
- Registers all HTTP routes on a Chi router
- Listens on `:8080`

When a `SIGTERM` or `Ctrl+C` arrives, it shuts the HTTP server down gracefully within 10 seconds.

---

### 2. Auth — `service/auth.go` + `handler/auth.go`

**Endpoints:** `POST /auth/signup` and `POST /auth/login` (both unauthenticated)

- **Signup:** takes username, email, password → hashes the password with **bcrypt** → generates a random user ID → issues a **JWT** signed with HS256 → stores the user in an in-memory map (keyed by ID and by email) → returns the user + token
- **Login:** looks up the user by email → verifies the bcrypt hash → issues a new JWT → returns user + token

The JWT contains only the `sub` (user ID) claim and expires in 24 hours. The `PasswordHash` is never returned in responses — `publicUser()` strips it before sending.

---

### 3. JWT Middleware — `middleware/auth.go`

All routes except `/auth/*` and `/metrics` are protected by this middleware.

It reads the `Authorization: Bearer <token>` header, validates the JWT signature and expiry, extracts the user ID from the `sub` claim, and stores it in the request context. Any subsequent handler can call `userIDFromRequest(r)` to get the authenticated user ID — no need to trust a `user_id` field in the request body.

---

### 4. Media Upload — `service/storage.go` + `handler/upload.go`

A **two-step upload flow** — the client never uploads through the app server, it goes directly to S3/MinIO.

**Step 1 — `POST /presigned-url`**

- Generates a random `media_id`
- Builds an S3 key: `users/{user_id}/{media_id}/{filename}`
- Calls the AWS SDK's `PresignPutObject` to generate a time-limited signed PUT URL (15 minutes)
- Stores a `Media` record in memory with `status: pending`
- Returns the `media_id` and the `upload_url` to the client

**Step 2 — `POST /media/confirm`**

- Client sends the `media_id` after it has PUT the file to S3
- Finds the media record in memory, checks it belongs to the authenticated user
- Sets `status: uploaded` and `uploaded_at`
- Publishes a `media-uploaded` event to Kafka
- Returns the updated media object

There is also `GET /media/{id}` to fetch a media record by ID at any time.

The presign client uses `localhost:9000` (public endpoint) while the internal S3 client uses `minio:9000` (Docker network) — these are kept separate so generated URLs are reachable from outside Docker without breaking the signature.

---

### 5. Kafka — `kafka/producer.go` + `kafka/consumer.go`

**Two topics:**

| Topic | Published by | Consumed by |
|---|---|---|
| `media-uploaded` | `POST /media/confirm` | `consumeMedia` goroutine |
| `story-uploaded` | `POST /stories/confirm` | `consumeStories` goroutine |

**Producer:**
- Serialises the event as JSON
- Injects the current OTel trace context into the Kafka message headers so consumers can link their spans to the same trace
- Writes to the appropriate topic

**Consumer:**
- Two goroutines, one per topic, running infinite read loops
- On each message, extracts the trace context from headers and starts a child span
- `media-uploaded` handler: calls `MediaProcessor.Process()` to generate thumbnails, then calls `FeedService.AddFeedItem()` to add it to the user's feed
- `story-uploaded` handler: currently just logs — story fan-out (notifications, analytics) is a TODO
- On read error (e.g. Kafka unavailable at startup), waits 2 seconds before retrying — context-aware so it shuts down cleanly on `SIGTERM`

---

### 6. Media Processor — `service/processor.go`

Called by the Kafka consumer after a photo upload is confirmed.

**For photos:**

1. Downloads the original file from S3
2. Creates a **150×150 thumbnail** (center-cropped) using the `imaging` library
3. Creates a **640×640 medium** version (fit within bounds, no crop)
4. Uploads all three back to S3 at:
   - `{original_key}/thumb`
   - `{original_key}/medium`
   - `{original_key}/original`

**For videos:** logs "transcoding queued" — actual transcoding is not yet implemented.

Every step is wrapped in an OTel span so the full processing pipeline is visible in Jaeger.

---

### 7. Stories — `service/story.go` + `handler/story.go`

Same two-step presign flow as media, but stories have a **24-hour TTL**.

| Endpoint | Description |
|---|---|
| `POST /stories/presigned-url` | Generates S3 key under `stories/{user_id}/{story_id}/`, returns presigned URL |
| `POST /stories/confirm` | Marks story active, sets `expires_at = now + 24h`, publishes Kafka event |
| `GET /stories/{id}` | Returns story only if active (confirmed and not expired) |
| `GET /stories/user/{user_id}` | Returns all active stories for a user |

A **background worker** (`StartExpiryWorker`) ticks every 5 minutes and deletes expired stories from memory. Pending stories (presigned but never confirmed) are auto-expired after 30 minutes.

---

### 8. Feed — `service/feed.go` + `handler/feed.go`

The feed is populated **asynchronously** by the Kafka consumer — not inline during the HTTP confirm request. When `POST /media/confirm` is called, the HTTP response returns immediately. The Kafka message is consumed in the background and `AddFeedItem` is called separately.

`GET /feed/{user_id}` returns paginated feed items sorted by `created_at` descending (newest first). Supports `?limit=` and `?offset=` query params. Only the authenticated user can fetch their own feed.

---

### 9. Observability

**Structured logging** — `slog` with JSON output. No `fmt.Println` or bare `log.Printf`.

**Distributed tracing** — OTel traces flow through the full request lifecycle:

```
HTTP request → handler span
  → Kafka publish span (headers carry W3C trace context)
    → Kafka consume span
      → MediaProcessor span
        → processPhoto span
```

Visible in Jaeger at `http://localhost:16686`.

**Metrics** — Prometheus counters and histograms exposed at `/metrics`, scraped every 15s, visualised in Grafana at `http://localhost:3000`.

---

## Infrastructure (Docker Compose)

| Service | Port | Purpose |
|---|---|---|
| App | 8080 | The Go API |
| MinIO | 9000 / 9001 | Local S3 (API / Browser UI) |
| Kafka | 9092 | Event bus |
| Kafka UI | 8090 | Browse topics and messages |
| Jaeger | 16686 / 4318 | Trace viewer / OTLP receiver |
| Prometheus | 9090 | Metrics store |
| Grafana | 3000 | Dashboards |

---

## Request Lifecycle — Photo Upload

```
Client
  │
  ├─ POST /presigned-url ──► handler ──► Storage.GeneratePresignedURL
  │                                         ├─ media stored in memory (status: pending)
  │                                         └─ returns upload_url (localhost:9000)
  │
  ├─ PUT localhost:9000/... ──► MinIO directly (app server not involved)
  │
  ├─ POST /media/confirm ──► handler ──► Storage.ConfirmMediaUploaded
  │                                         ├─ status = uploaded, uploaded_at = now
  │                                         └─ KafkaProducer.PublishMediaUploaded
  │                                               └─ media-uploaded topic
  │
  └─ (async, in background)
       KafkaConsumer reads media-uploaded
         ├─ MediaProcessor.Process
         │     ├─ download original from MinIO
         │     ├─ generate 150×150 thumb
         │     ├─ generate 640×640 medium
         │     └─ upload 3 variants back to MinIO
         └─ FeedService.AddFeedItem
               └─ media appears in GET /feed/{user_id}
```

---

## Current Limitations

Everything is stored **in-memory** — users, media, stories, and feed items. Restarting the app wipes all data. In a production service these would be backed by a database (Postgres for structured data, Redis for the feed cache).
