# Instagram Clone Upload Service

A small Go microservice for generating S3 presigned upload URLs, confirming completed uploads, and fetching in-memory media metadata.

## Stack

- Go HTTP service on port `8080`
- Chi router
- AWS SDK for Go v2
- LocalStack S3 at `http://localstack:4566`
- S3 bucket: `instagram-media`

## Project Structure

```text
cmd/server/main.go
internal/model/media.go
internal/service/storage.go
internal/handler/upload.go
docker-compose.yml
Dockerfile
init-scripts/create-bucket.sh
```

## Setup

```sh
docker compose up --build
```

The LocalStack container creates the `instagram-media` bucket on startup.

## Endpoints

### Generate a Presigned Upload URL

```sh
curl -s -X POST http://localhost:8080/presigned-url \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user_123",
    "file_name": "sunset.jpg",
    "content_type": "image/jpeg",
    "media_type": "photo"
  }'
```

Example response:

```json
{
  "media_id": "1f6f5e8c1c2d4e0f9a8b7c6d5e4f3012",
  "upload_url": "http://localstack:4566/instagram-media/users/user_123/...",
  "s3_bucket": "instagram-media",
  "s3_key": "users/user_123/1f6f5e8c1c2d4e0f9a8b7c6d5e4f3012/sunset.jpg",
  "expires_in": 900
}
```

### Upload the File to S3

Use the `upload_url` returned by the first endpoint:

```sh
curl --resolve localstack:4566:127.0.0.1 -X PUT "$UPLOAD_URL" \
  -H "Content-Type: image/jpeg" \
  --data-binary @sunset.jpg
```

The `--resolve` flag lets curl use the Docker-internal `localstack` hostname from the signed URL while sending traffic to your local machine.

The URL expires after 15 minutes.

### Confirm Media Upload

```sh
curl -s -X POST http://localhost:8080/media/confirm \
  -H "Content-Type: application/json" \
  -d '{
    "user_id": "user_123",
    "media_id": "1f6f5e8c1c2d4e0f9a8b7c6d5e4f3012"
  }'
```

### Fetch Media Metadata

```sh
curl -s http://localhost:8080/media/1f6f5e8c1c2d4e0f9a8b7c6d5e4f3012
```

## Notes

- Auth is intentionally skipped. Send `user_id` in request bodies for now.
- Metadata is stored in memory, so restarting the Go service clears media records.
- JSON errors use this shape:

```json
{
  "error": "media not found"
}
```
