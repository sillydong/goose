package goose

import (
	"database/sql"
	"time"
)

// Redo rolls back the most recently applied migration, then runs it again.
func Redo(db *sql.DB, dir string) error {
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
		return err
	}

	if err := current.Down(db); err != nil {
		return err
	}

	if err := current.Up(db); err != nil {
		return err
	}

	return nil
}
