package goose

import (
	"database/sql"
	"time"
)

// Redo rolls back the most recently applied migration, then runs it again.
func Redo(db *sql.DB, dir string) error {
	log.Printf("goose: try lock %d", time.Now().Unix())
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	if err := GetDialect().lock(tx); err != nil {
		return err
	}
	log.Printf("goose: got lock %d", time.Now().Unix())
	defer func() {
		log.Printf("goose: release lock %d", time.Now().Unix())
		if err := GetDialect().unlock(tx); err != nil {
			log.Printf("goose: release lock error: %v", err)
		}
		if err := tx.Commit(); err != nil {
			log.Printf("goose: release lock tx commit error: %v", err)
		}
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
