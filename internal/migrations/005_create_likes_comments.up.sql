CREATE TABLE IF NOT EXISTS likes (
    media_id   VARCHAR(32) NOT NULL REFERENCES media(id) ON DELETE CASCADE,
    user_id    VARCHAR(32) NOT NULL REFERENCES users(id),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (media_id, user_id)
);

CREATE INDEX IF NOT EXISTS idx_likes_media ON likes(media_id);

CREATE TABLE IF NOT EXISTS comments (
    id         VARCHAR(32)   PRIMARY KEY,
    media_id   VARCHAR(32)   NOT NULL REFERENCES media(id) ON DELETE CASCADE,
    user_id    VARCHAR(32)   NOT NULL REFERENCES users(id),
    body       VARCHAR(2200) NOT NULL,
    created_at TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- Comments are listed newest-first per media item.
CREATE INDEX IF NOT EXISTS idx_comments_media ON comments(media_id, created_at DESC);
