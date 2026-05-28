package db

import (
	"context"
	"fmt"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	_ "github.com/golang-migrate/migrate/v4/source/file"
	"github.com/jackc/pgx/v5/pgxpool"
)

// NewPostgresPool opens a connection pool and verifies connectivity.
func NewPostgresPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, fmt.Errorf("open postgres pool: %w", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}
	return pool, nil
}

// RunMigrations applies all pending up migrations from the given directory.
// The DSN must use a postgres:// or postgresql:// scheme; an empty or
// unrecognised scheme returns an error rather than panicking.
func RunMigrations(dsn, migrationsPath string) error {
	if dsn == "" {
		return fmt.Errorf("DATABASE_URL must not be empty")
	}

	// golang-migrate's pgx5 driver requires a pgx5:// scheme.
	// Strip the standard postgres(ql):// prefix before reattaching pgx5://.
	rest, ok := strings.CutPrefix(dsn, "postgres://")
	if !ok {
		rest, ok = strings.CutPrefix(dsn, "postgresql://")
	}
	if !ok {
		return fmt.Errorf("unsupported DATABASE_URL scheme (want postgres:// or postgresql://): %q", dsn)
	}

	m, err := migrate.New("file://"+migrationsPath, "pgx5://"+rest)
	if err != nil {
		return fmt.Errorf("create migrator: %w", err)
	}
	defer m.Close()

	if err := m.Up(); err != nil && err != migrate.ErrNoChange {
		return fmt.Errorf("run migrations: %w", err)
	}
	return nil
}
