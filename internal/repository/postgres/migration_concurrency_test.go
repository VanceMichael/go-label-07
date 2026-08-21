package postgres

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

const migrationWorkerEnv = "AIRBRIDGE_MIGRATION_TEST_WORKER"

func TestConcurrentStartupSerializesMigrationOwnership(t *testing.T) {
	if worker := os.Getenv(migrationWorkerEnv); worker != "" {
		runMigrationWorker(t, worker)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	database, err := Open(ctx, EnvURL())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	if err := Migrate(ctx, database); err != nil {
		t.Fatalf("prepare database: %v", err)
	}
	if _, err := database.Pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS migration_startup_probe(
			worker text PRIMARY KEY,
			arrived_at timestamptz NOT NULL DEFAULT now()
		);
		TRUNCATE migration_startup_probe;
		DELETE FROM schema_migrations WHERE version='002_seed.sql';
	`); err != nil {
		t.Fatalf("reset migration state: %v", err)
	}
	t.Cleanup(func() {
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cleanupCancel()
		_, _ = database.Pool.Exec(cleanupCtx, `DROP TABLE IF EXISTS migration_startup_probe`)
		_ = Migrate(cleanupCtx, database)
	})

	start := make(chan struct{})
	results := make(chan error, 2)
	for _, worker := range []string{"blue", "green"} {
		go func(worker string) {
			<-start
			command := exec.CommandContext(ctx, os.Args[0], "-test.run=^TestConcurrentStartupSerializesMigrationOwnership$", "-test.v")
			command.Env = append(os.Environ(), migrationWorkerEnv+"="+worker)
			output, err := command.CombinedOutput()
			if err != nil {
				results <- fmt.Errorf("%s startup: %w\n%s", worker, err, output)
				return
			}
			results <- nil
		}(worker)
	}
	close(start)

	for attempt := 0; attempt < 2; attempt++ {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}
	var arrivals, applied int
	if err := database.Pool.QueryRow(ctx, `SELECT count(*) FROM migration_startup_probe`).Scan(&arrivals); err != nil {
		t.Fatal(err)
	}
	if err := database.Pool.QueryRow(ctx, `SELECT count(*) FROM schema_migrations WHERE version='002_seed.sql'`).Scan(&applied); err != nil {
		t.Fatal(err)
	}
	if arrivals != 1 {
		t.Fatalf("migration body ownership was entered by %d startup processes", arrivals)
	}
	if applied != 1 {
		t.Fatalf("seed migration history rows = %d", applied)
	}
}

func runMigrationWorker(t *testing.T, worker string) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	database, err := Open(ctx, EnvURL())
	if err != nil {
		t.Fatal(err)
	}
	defer database.Close()
	observer := func(version string) {
		if version != "002_seed.sql" {
			return
		}
		if _, err := database.Pool.Exec(ctx, `INSERT INTO migration_startup_probe(worker) VALUES($1)`, worker); err != nil {
			t.Errorf("record startup arrival: %v", err)
			return
		}
		deadline := time.Now().Add(250 * time.Millisecond)
		for time.Now().Before(deadline) {
			var arrivals int
			if err := database.Pool.QueryRow(ctx, `SELECT count(*) FROM migration_startup_probe`).Scan(&arrivals); err != nil {
				t.Errorf("read startup arrivals: %v", err)
				return
			}
			if arrivals >= 2 {
				return
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	if err := migrateWithObserver(ctx, database, observer); err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(worker) == "" {
		t.Fatal("worker identity was lost")
	}
}
