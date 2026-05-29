CREATE TABLE IF NOT EXISTS follows (
    follower_id VARCHAR(32) NOT NULL REFERENCES users(id),
    followee_id VARCHAR(32) NOT NULL REFERENCES users(id),
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (follower_id, followee_id),
    CONSTRAINT no_self_follow CHECK (follower_id <> followee_id)
);

-- Lookup of "who follows user X" drives feed fan-out on write.
CREATE INDEX IF NOT EXISTS idx_follows_followee ON follows(followee_id);
