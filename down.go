package goose

import (
	"database/sql"
	"fmt"
	"time"
)

// Down rolls back a single migration from the current version.
func Down(db *sql.DB, dir string) error {
	log.Printf("goose: try lock %d", time.Now())
	if err := GetDialect().lock(db); err != nil {
		return err
	}
	log.Printf("goose: get lock %d", time.Now())
	defer func() {
		log.Printf("goose: release lock %d", time.Now())
		_ = GetDialect().unlock(db)
	}()

	currentVersion, err := GetDBVersion(db)
	if err != nil {
		return err
	}

	migrations, err := CollectMigrations(dir, minVersion, maxVersion)
	if err != nil {
		return err
	}

	current, err := migrations.Current(currentVersion)
	if err != nil {
		return fmt.Errorf("no migration %v", currentVersion)
	}

	return current.Down(db)
}

// DownTo rolls back migrations to a specific version.
func DownTo(db *sql.DB, dir string, version int64) error {
	migrations, err := CollectMigrations(dir, minVersion, maxVersion)
	if err != nil {
		return err
	}

	loop := func() error {
		log.Printf("goose: try lock %d", time.Now())
		if err := GetDialect().lock(db); err != nil {
			return err
		}
		log.Printf("goose: get lock %d", time.Now())
		defer func() {
			log.Printf("goose: release lock %d", time.Now())
			_ = GetDialect().unlock(db)
		}()

		currentVersion, err := GetDBVersion(db)
		if err != nil {
			return err
		}

		current, err := migrations.Current(currentVersion)
		if err != nil {
			log.Printf("goose: no migrations to run. current version: %d\n", currentVersion)
			return ErrNoCurrentVersion
		}

		if current.Version <= version {
			log.Printf("goose: no migrations to run. current version: %d\n", currentVersion)
			return ErrNoNextVersion
		}

		if err = current.Down(db); err != nil {
			return err
		}

		return nil
	}

	for {
		if err := loop(); err != nil {
			if err == ErrNoNextVersion || err == ErrNoCurrentVersion {
				return nil
			}
			return err
		}
	}
}
