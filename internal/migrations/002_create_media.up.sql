CREATE TABLE IF NOT EXISTS media (
    id           VARCHAR(32)  PRIMARY KEY,
    user_id      VARCHAR(32)  NOT NULL REFERENCES users(id),
    type         VARCHAR(10)  NOT NULL,
    status       VARCHAR(10)  NOT NULL DEFAULT 'pending',
    file_name    VARCHAR(255) NOT NULL,
    content_type VARCHAR(100) NOT NULL,
    s3_bucket    VARCHAR(255) NOT NULL,
    s3_key       VARCHAR(500) NOT NULL,
    created_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    uploaded_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_media_user_id ON media(user_id);
