# Instagram Clone Upload Service

A Go microservice for Instagram-style media uploads. It supports JWT auth, S3 presigned uploads, media confirmation events through Kafka, 24-hour stories, image variant processing, and an in-memory feed.

## Stack

- Go HTTP service on port `8080`
- Chi router
- JWT auth with HS256
- AWS SDK for Go v2
- MinIO S3 at `http://minio:9000` (browser UI at `http://localhost:9001`)
- S3 bucket: `instagram-media`
- Kafka and Zookeeper through Docker Compose
- In-memory stores for users, media, stories, and feeds

## Setup

```sh
docker compose up --build
```

The `createbuckets` container creates the `instagram-media` bucket on startup. Kafka creates the `media-uploaded` and `story-uploaded` topics on startup.

### S3 Browser UI

Open **http://localhost:9001** and log in with `minioadmin` / `minioadmin` to browse buckets and objects visually.

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

### Generate a Presigned Upload URL

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
  "upload_url": "http://minio:9000/instagram-media/users/user_123/...",
  "s3_bucket": "instagram-media",
  "s3_key": "users/user_123/1f6f5e8c1c2d4e0f9a8b7c6d5e4f3012/sunset.jpg",
  "expires_in": 900
}
```

### Upload the File to S3

Use the `upload_url` returned by the first endpoint:

```sh
curl --resolve minio:9000:127.0.0.1 -X PUT "$UPLOAD_URL" \
  -H "Content-Type: image/jpeg" \
  --data-binary @sunset.jpg
```

The `--resolve` flag lets curl use the Docker-internal `minio` hostname from the signed URL while sending traffic to your local machine.

### Confirm Media Upload

```sh
curl -s -X POST http://localhost:8080/media/confirm \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "media_id": "1f6f5e8c1c2d4e0f9a8b7c6d5e4f3012"
  }'
```

Confirmation publishes a `MediaUploadedEvent` to Kafka topic `media-uploaded`. The consumer processes photos into `/thumb`, `/medium`, and `/original` S3 objects. Video processing is currently a log-only transcoding stub.

### Fetch Media Metadata

```sh
curl -s http://localhost:8080/media/1f6f5e8c1c2d4e0f9a8b7c6d5e4f3012 \
  -H "Authorization: Bearer $TOKEN"
```

## Stories

Stories are uploaded to the same S3 bucket under the `stories/` prefix and expire after 24 hours once confirmed.

### Generate a Story Presigned URL

```sh
curl -s -X POST http://localhost:8080/stories/presigned-url \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "file_name": "story.jpg",
    "content_type": "image/jpeg"
  }'
```

### Confirm Story Upload

```sh
curl -s -X POST http://localhost:8080/stories/confirm \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "story_id": "story-id-from-presigned-response"
  }'
```

Confirmation publishes a `StoryUploadedEvent` to Kafka topic `story-uploaded`.

### Fetch One Story

```sh
curl -s http://localhost:8080/stories/story-id-from-presigned-response \
  -H "Authorization: Bearer $TOKEN"
```

### Fetch Active Stories by User

```sh
curl -s http://localhost:8080/stories/user/user_id_from_auth_response \
  -H "Authorization: Bearer $TOKEN"
```

## Feed

The Kafka consumer adds media upload events into an in-memory feed after media processing succeeds.
Feed items include `thumbnail_key` for the processed S3 object key. They do not expose a public URL yet.

```sh
curl -s "http://localhost:8080/feed/user_id_from_auth_response?limit=20&offset=0" \
  -H "Authorization: Bearer $TOKEN"
```

## Environment

```text
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=minioadmin
AWS_SECRET_ACCESS_KEY=minioadmin
S3_BUCKET=instagram-media
S3_ENDPOINT=http://minio:9000
JWT_SECRET=dev-secret-do-not-use-in-prod
KAFKA_BROKER=kafka:9092
APP_ENV=dev
```

## Notes

- Metadata is stored in memory, so restarting the Go service clears users, media records, stories, and feeds.
- The service uses `user_id` from the JWT for protected upload, media confirm, story write routes, and user-scoped feed/story reads.
- `JWT_SECRET` falls back to a development secret only when `APP_ENV` is empty, `dev`, `local`, or `test`.
- S3 credentials are set via environment variables. Docker Compose uses the MinIO root credentials.
- JSON errors use this shape:

```json
{
  "error": "media not found"
}
```
