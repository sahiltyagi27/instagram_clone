package store

import (
	"errors"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
)

const (
	pgErrUniqueViolation     = "23505"
	pgErrForeignKeyViolation = "23503"
	pgErrCheckViolation      = "23514"
)

// PendingUploadTTL is the window in which a pending upload must be confirmed.
// It matches the presigned URL expiry so a row can never be confirmed after
// the upload URL itself has expired.
const PendingUploadTTL = 15 * time.Minute

func isUniqueViolation(err error) bool {
	return hasPgErrCode(err, pgErrUniqueViolation)
}

func isForeignKeyViolation(err error) bool {
	return hasPgErrCode(err, pgErrForeignKeyViolation)
}

func isCheckViolation(err error) bool {
	return hasPgErrCode(err, pgErrCheckViolation)
}

func hasPgErrCode(err error, code string) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == code
}
