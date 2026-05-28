package handler

import (
	"context"
	"net/http"
	"testing"

	"instagram_clone/internal/model"
	"instagram_clone/internal/service"
	"instagram_clone/internal/store"

	"github.com/jackc/pgx/v5/pgxpool"
)

// newSocialPGPool connects to Postgres and seeds two users (user_123, the
// authenticated caller, and other_user) plus one media row owned by user_123.
// It cleans up every social table on completion so tests stay isolated.
func newSocialPGPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	const dsn = "postgres://postgres:postgres@localhost:5432/instagram_clone"
	pool, err := pgxpool.New(context.Background(), dsn)
	if err != nil || pool.Ping(context.Background()) != nil {
		if pool != nil {
			pool.Close()
		}
		t.Skip("postgres unavailable, skipping")
	}

	ctx := context.Background()
	pool.Exec(ctx, `
		INSERT INTO users (id, username, email, password_hash) VALUES
		('user_123', 'caller', 'caller@example.com', 'x'),
		('other_user', 'other', 'other@example.com', 'x')
		ON CONFLICT DO NOTHING`)
	pool.Exec(ctx, `
		INSERT INTO media (id, user_id, type, status, file_name, content_type, s3_bucket, s3_key) VALUES
		('media_1', 'user_123', 'photo', 'ready', 'p.jpg', 'image/jpeg', 'b', 'k')
		ON CONFLICT DO NOTHING`)

	t.Cleanup(func() {
		pool.Exec(ctx, "DELETE FROM comments")
		pool.Exec(ctx, "DELETE FROM likes")
		pool.Exec(ctx, "DELETE FROM follows")
		pool.Exec(ctx, "DELETE FROM media")
		pool.Exec(ctx, "DELETE FROM users")
		pool.Close()
	})
	return pool
}

// ── follow tests ───────────────────────────────────────────────────────────────

func TestFollowHandlerFollowAndList(t *testing.T) {
	pool := newSocialPGPool(t)
	router := NewFollowHandler(service.NewFollowService(store.NewFollowStore(pool))).Router()

	rec := performRequest(router, http.MethodPost, "/other_user", "")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("follow status = %d, want %d; body: %s", rec.Code, http.StatusNoContent, rec.Body.String())
	}

	// Idempotent: following again is still a no-op success.
	rec = performRequest(router, http.MethodPost, "/other_user", "")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("repeat follow status = %d, want %d", rec.Code, http.StatusNoContent)
	}

	// user_123 should now list other_user in its "following" set.
	rec = performRequest(router, http.MethodGet, "/user_123/following", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("following status = %d, want %d", rec.Code, http.StatusOK)
	}
	var following model.FollowListResponse
	decodeResponse(t, rec, &following)
	if following.Count != 1 || following.UserIDs[0] != "other_user" {
		t.Fatalf("following = %#v, want [other_user]", following)
	}

	// other_user should list user_123 as a follower.
	rec = performRequest(router, http.MethodGet, "/other_user/followers", "")
	var followers model.FollowListResponse
	decodeResponse(t, rec, &followers)
	if followers.Count != 1 || followers.UserIDs[0] != "user_123" {
		t.Fatalf("followers = %#v, want [user_123]", followers)
	}

	// Unfollow and confirm the edge is gone.
	rec = performRequest(router, http.MethodDelete, "/other_user", "")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("unfollow status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	rec = performRequest(router, http.MethodGet, "/user_123/following", "")
	decodeResponse(t, rec, &following)
	if following.Count != 0 {
		t.Fatalf("following after unfollow = %#v, want empty", following)
	}
}

func TestFollowHandlerRejectsSelfFollow(t *testing.T) {
	pool := newSocialPGPool(t)
	router := NewFollowHandler(service.NewFollowService(store.NewFollowStore(pool))).Router()

	rec := performRequest(router, http.MethodPost, "/user_123", "")
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("self-follow status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	assertErrorResponse(t, rec, "cannot follow yourself")
}

func TestFollowHandlerUnknownUser(t *testing.T) {
	pool := newSocialPGPool(t)
	router := NewFollowHandler(service.NewFollowService(store.NewFollowStore(pool))).Router()

	rec := performRequest(router, http.MethodPost, "/ghost", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("follow-unknown status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	assertErrorResponse(t, rec, "user not found")
}

// ── like tests ─────────────────────────────────────────────────────────────────

func TestLikeHandlerLikeStatusUnlike(t *testing.T) {
	pool := newSocialPGPool(t)
	router := NewLikeHandler(service.NewLikeService(store.NewLikeStore(pool))).Router()

	rec := performRequest(router, http.MethodPut, "/media_1", "")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("like status = %d, want %d; body: %s", rec.Code, http.StatusNoContent, rec.Body.String())
	}

	// Idempotent: liking again is still a no-op success.
	rec = performRequest(router, http.MethodPut, "/media_1", "")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("repeat like status = %d, want %d", rec.Code, http.StatusNoContent)
	}

	rec = performRequest(router, http.MethodGet, "/media_1", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("status code = %d, want %d", rec.Code, http.StatusOK)
	}
	var status model.LikeStatusResponse
	decodeResponse(t, rec, &status)
	if status.Count != 1 || !status.Liked {
		t.Fatalf("like status = %#v, want count 1 liked true", status)
	}

	rec = performRequest(router, http.MethodDelete, "/media_1", "")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("unlike status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	rec = performRequest(router, http.MethodGet, "/media_1", "")
	decodeResponse(t, rec, &status)
	if status.Count != 0 || status.Liked {
		t.Fatalf("like status after unlike = %#v, want count 0 liked false", status)
	}
}

func TestLikeHandlerUnknownMedia(t *testing.T) {
	pool := newSocialPGPool(t)
	router := NewLikeHandler(service.NewLikeService(store.NewLikeStore(pool))).Router()

	rec := performRequest(router, http.MethodPut, "/ghost", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("like-unknown status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	assertErrorResponse(t, rec, "media not found")
}

// ── comment tests ────────────────────────────────────────────────────────────────

func TestCommentHandlerAddListDelete(t *testing.T) {
	pool := newSocialPGPool(t)
	router := NewCommentHandler(service.NewCommentService(store.NewCommentStore(pool))).Router()

	rec := performRequest(router, http.MethodPost, "/media_1", `{"body":"nice shot"}`)
	if rec.Code != http.StatusCreated {
		t.Fatalf("add status = %d, want %d; body: %s", rec.Code, http.StatusCreated, rec.Body.String())
	}
	var created model.Comment
	decodeResponse(t, rec, &created)
	if created.ID == "" || created.Body != "nice shot" {
		t.Fatalf("created comment = %#v, want body 'nice shot' with id", created)
	}

	rec = performRequest(router, http.MethodGet, "/media_1", "")
	if rec.Code != http.StatusOK {
		t.Fatalf("list status = %d, want %d", rec.Code, http.StatusOK)
	}
	var list model.CommentListResponse
	decodeResponse(t, rec, &list)
	if len(list.Comments) != 1 || list.Comments[0].ID != created.ID {
		t.Fatalf("list = %#v, want one comment %s", list, created.ID)
	}

	rec = performRequest(router, http.MethodDelete, "/media_1/"+created.ID, "")
	if rec.Code != http.StatusNoContent {
		t.Fatalf("delete status = %d, want %d", rec.Code, http.StatusNoContent)
	}

	rec = performRequest(router, http.MethodGet, "/media_1", "")
	decodeResponse(t, rec, &list)
	if len(list.Comments) != 0 {
		t.Fatalf("list after delete = %#v, want empty", list)
	}
}

func TestCommentHandlerRejectsEmptyBody(t *testing.T) {
	pool := newSocialPGPool(t)
	router := NewCommentHandler(service.NewCommentService(store.NewCommentStore(pool))).Router()

	rec := performRequest(router, http.MethodPost, "/media_1", `{"body":"   "}`)
	if rec.Code != http.StatusBadRequest {
		t.Fatalf("empty-body status = %d, want %d", rec.Code, http.StatusBadRequest)
	}
	assertErrorResponse(t, rec, "comment body must not be empty")
}

func TestCommentHandlerDeleteOtherUsersCommentForbidden(t *testing.T) {
	pool := newSocialPGPool(t)
	router := NewCommentHandler(service.NewCommentService(store.NewCommentStore(pool))).Router()

	// Seed a comment owned by other_user directly, then have user_123 try to delete it.
	pool.Exec(context.Background(), `
		INSERT INTO comments (id, media_id, user_id, body) VALUES
		('comment_x', 'media_1', 'other_user', 'theirs')`)

	rec := performRequest(router, http.MethodDelete, "/media_1/comment_x", "")
	if rec.Code != http.StatusNotFound {
		t.Fatalf("delete-foreign status = %d, want %d", rec.Code, http.StatusNotFound)
	}
	assertErrorResponse(t, rec, "comment not found")
}
