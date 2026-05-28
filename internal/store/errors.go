package store

import (
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

const pgErrUniqueViolation = "23505"

// PendingUploadTTL is the window in which a pending upload must be confirmed.
// It matches the presigned URL expiry so a row can never be confirmed after
// the upload URL itself has expired.
const PendingUploadTTL = 15 * time.Minute

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == pgErrUniqueViolation
}
