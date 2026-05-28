CREATE TABLE IF NOT EXISTS stories (
    id         VARCHAR(32)  PRIMARY KEY,
    user_id    VARCHAR(32)  NOT NULL REFERENCES users(id),
    s3_key     VARCHAR(500) NOT NULL,
    url        VARCHAR(500) NOT NULL,
    created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    expires_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_stories_user_id  ON stories(user_id);
CREATE INDEX IF NOT EXISTS idx_stories_expires_at ON stories(expires_at);
