package postgres

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"os"
)

type DB struct{ Pool *pgxpool.Pool }

func Open(ctx context.Context, url string) (*DB, error) {
	cfg, err := pgxpool.ParseConfig(url)
	if err != nil {
		return nil, err
	}
	cfg.MaxConns = 12
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return nil, err
	}
	return &DB{Pool: pool}, nil
}
func (d *DB) Close() { d.Pool.Close() }

func Migrate(ctx context.Context, d *DB) error {
	return migrateWithObserver(ctx, d, nil)
}

func EnvURL() string {
	if v := os.Getenv("DATABASE_URL"); v != "" {
		return v
	}
	return "postgres://airbridge:airbridge@localhost:55433/airbridge?sslmode=disable"
}
