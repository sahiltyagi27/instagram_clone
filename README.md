# Instagram Clone Upload Service

A Go microservice for Instagram-style media uploads. Supports JWT auth, S3 presigned uploads, Kafka events, 24-hour stories, image variant processing, a Redis-backed feed, and full observability via OpenTelemetry, Prometheus, and Grafana.

## Stack

| Component | Technology |
|---|---|
| HTTP service | Go + Chi router, port `8080` |
| Auth | JWT HS256 |
| Object storage | MinIO (S3-compatible), port `9000` |
| Database | Postgres 16, port `5432` |
| Cache / Feed | Redis 7, port `6379` |
| Events | Kafka + Zookeeper |
| Tracing | OpenTelemetry → Jaeger, port `16686` |
| Metrics | Prometheus, port `9090` |
| Dashboards | Grafana, port `3000` |
| Kafka UI | Kafka UI, port `8090` |
| MinIO UI | MinIO Console, port `9001` |

## Setup

```sh
docker compose up --build
```

On first startup:
- `createbuckets` creates the `instagram-media` S3 bucket in MinIO
- Kafka creates the `media-uploaded` and `story-uploaded` topics
- The app runs all pending Postgres migrations automatically

## Observability

| Tool | URL | Purpose |
|---|---|---|
| Jaeger | http://localhost:16686 | Distributed traces across HTTP → Kafka → processor |
| Grafana | http://localhost:3000 | Dashboards (admin / admin) |
| Prometheus | http://localhost:9090 | Raw metrics |
| Kafka UI | http://localhost:8090 | Browse topics and messages |
| MinIO UI | http://localhost:9001 | Browse S3 buckets (minioadmin / minioadmin) |

## Auth

All routes except `POST /auth/signup` and `POST /auth/login` require a Bearer token.

### Signup

```sh
curl -s -X POST http://localhost:8080/auth/signup \
  -H "Content-Type: application/json" \
  -d '{
    "username": "sahil",
    "email": "sahil@example.com",
    "password": "secret123"
  }'
```

Save the token:

```sh
TOKEN="paste-token-here"
```

### Login

```sh
curl -s -X POST http://localhost:8080/auth/login \
  -H "Content-Type: application/json" \
  -d '{
    "email": "sahil@example.com",
    "password": "secret123"
  }'
```

## Media Uploads

### 1. Generate a Presigned Upload URL

```sh
curl -s -X POST http://localhost:8080/presigned-url \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "file_name": "sunset.jpg",
    "content_type": "image/jpeg",
    "media_type": "photo"
  }'
```

Example response:

```json
{
  "media_id": "1f6f5e8c1c2d4e0f9a8b7c6d5e4f3012",
  "upload_url": "http://localhost:9000/instagram-media/users/...",
  "s3_bucket": "instagram-media",
  "s3_key": "users/user_id/1f6f5e8c1c2d4e0f9a8b7c6d5e4f3012/sunset.jpg",
  "expires_in": 900
}
```

### 2. Upload the File Directly to S3

Use the `upload_url` from the previous response:

```sh
curl -X PUT "$UPLOAD_URL" \
  -H "Content-Type: image/jpeg" \
  --data-binary @sunset.jpg
```

### 3. Confirm the Upload

```sh
curl -s -X POST http://localhost:8080/media/confirm \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "media_id": "1f6f5e8c1c2d4e0f9a8b7c6d5e4f3012"
  }'
```

This publishes a `media-uploaded` Kafka event. The consumer then:
- Downloads the original from S3
- Generates a `150×150` thumbnail and a `640×640` medium variant
- Uploads all three back to S3 (`/thumb`, `/medium`, `/original`)
- Adds the item to the user's Redis feed

### Fetch Media Metadata

```sh
curl -s http://localhost:8080/media/1f6f5e8c1c2d4e0f9a8b7c6d5e4f3012 \
  -H "Authorization: Bearer $TOKEN"
```

## Stories

Stories expire 24 hours after confirmation. Same two-step presign flow as media.

### 1. Generate a Story Presigned URL

```sh
curl -s -X POST http://localhost:8080/stories/presigned-url \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "file_name": "story.jpg",
    "content_type": "image/jpeg"
  }'
```

### 2. Confirm the Story Upload

```sh
curl -s -X POST http://localhost:8080/stories/confirm \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "story_id": "story-id-from-presigned-response"
  }'
```

### Fetch a Story

```sh
curl -s http://localhost:8080/stories/{story_id} \
  -H "Authorization: Bearer $TOKEN"
```

### Fetch Active Stories by User

```sh
curl -s http://localhost:8080/stories/user/{user_id} \
  -H "Authorization: Bearer $TOKEN"
```

## Feed

The feed is populated asynchronously after `POST /media/confirm` via Kafka → Redis. Supports `limit` and `offset` pagination, capped at 1000 items per user.

```sh
curl -s "http://localhost:8080/feed/{user_id}?limit=20&offset=0" \
  -H "Authorization: Bearer $TOKEN"
```

## Environment

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
S3_ENDPOINT=http://minio:9000          # internal Docker endpoint
S3_PUBLIC_ENDPOINT=http://localhost:9000  # used for presigned URLs (reachable from host)

# Auth
JWT_SECRET=dev-secret-do-not-use-in-prod
APP_ENV=dev

# Kafka
KAFKA_BROKER=kafka:9092

# Tracing
OTEL_EXPORTER_OTLP_ENDPOINT=jaeger:4318
```

## Running Tests

Integration tests require Postgres and Redis to be running. They skip automatically if unavailable.

```sh
# Run all tests (skips DB-dependent tests if stack is not up)
go test ./...

# Run with the full stack up
docker compose up -d postgres redis
go test ./...
```

## API Reference

See [`openapi.yaml`](./openapi.yaml) at the project root. Paste it into [editor.swagger.io](https://editor.swagger.io) for interactive docs.

## Notes

- `JWT_SECRET` falls back to a development secret when `APP_ENV` is empty, `dev`, `local`, or `test`. Set it explicitly in all other environments.
- The Kafka consumer processes photos (thumbnail + medium + original). Video transcoding is currently a stub.
- JSON errors always use the shape `{ "error": "message" }`.
