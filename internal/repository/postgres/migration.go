package postgres

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/jackc/pgx/v5/pgxpool"
)

type migration struct {
	version string
	path    string
	body    []byte
}

type migrationObserver func(version string)

type migrationRunner struct {
	db       *pgxpool.Pool
	root     string
	observer migrationObserver
}

func migrateWithObserver(ctx context.Context, database *DB, observer migrationObserver) error {
	runner := migrationRunner{
		db:       database.Pool,
		root:     migrationRoot(),
		observer: observer,
	}
	return runner.run(ctx)
}

func (runner migrationRunner) run(ctx context.Context) error {
	if err := runner.ensureVersionTable(ctx); err != nil {
		return err
	}
	migrations, err := runner.discover()
	if err != nil {
		return err
	}
	for _, item := range migrations {
		applied, err := runner.isApplied(ctx, item.version)
		if err != nil {
			return fmt.Errorf("inspect migration %s: %w", item.version, err)
		}
		if applied {
			continue
		}
		if runner.observer != nil {
			runner.observer(item.version)
		}
		if err := runner.apply(ctx, item); err != nil {
			return err
		}
	}
	return nil
}

func (runner migrationRunner) ensureVersionTable(ctx context.Context) error {
	_, err := runner.db.Exec(ctx, `CREATE TABLE IF NOT EXISTS schema_migrations(version text primary key, applied_at timestamptz not null default now())`)
	if err != nil {
		return fmt.Errorf("prepare migration history: %w", err)
	}
	return nil
}

func (runner migrationRunner) discover() ([]migration, error) {
	entries, err := os.ReadDir(runner.root)
	if err != nil {
		return nil, fmt.Errorf("read migrations: %w", err)
	}
	items := make([]migration, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != ".sql" {
			continue
		}
		path := filepath.Join(runner.root, entry.Name())
		body, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("read migration %s: %w", entry.Name(), err)
		}
		items = append(items, migration{version: entry.Name(), path: path, body: body})
	}
	sort.Slice(items, func(i, j int) bool { return items[i].version < items[j].version })
	return items, nil
}

func (runner migrationRunner) isApplied(ctx context.Context, version string) (bool, error) {
	var applied bool
	err := runner.db.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version=$1)`, version).Scan(&applied)
	return applied, err
}

func (runner migrationRunner) apply(ctx context.Context, item migration) error {
	tx, err := runner.db.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin migration %s: %w", item.version, err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, string(item.body)); err != nil {
		return fmt.Errorf("execute migration %s: %w", item.version, err)
	}
	if _, err := tx.Exec(ctx, `INSERT INTO schema_migrations(version) VALUES($1)`, item.version); err != nil {
		return fmt.Errorf("record migration %s: %w", item.version, err)
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit migration %s: %w", item.version, err)
	}
	return nil
}

func migrationRoot() string {
	for _, candidate := range []string{"migrations", "../../../../migrations", "../../../migrations", "../../migrations"} {
		if entries, err := os.ReadDir(candidate); err == nil && len(entries) > 0 {
			return candidate
		}
	}
	return "migrations"
}
