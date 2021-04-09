package goose

import (
	"database/sql"
	"time"
)

// UpTo migrates up to a specific version.
func UpTo(db *sql.DB, dir string, version int64) error {
	migrations, err := CollectMigrations(dir, minVersion, version)
	if err != nil {
		return err
	}

	loop := func() error {
		current, err := GetDBVersion(db)
		if err != nil {
			return err
		}

		next, err := migrations.Next(current)
		if err != nil {
			if err == ErrNoNextVersion {
				log.Printf("goose: no migrations to run. current version: %d\n", current)
			}
			return err
		}

		if err = next.Up(db); err != nil {
			return err
		}

		return nil
	}

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
		_ = GetDialect().unlock(tx)
		if err := tx.Commit(); err != nil {
			log.Printf("goose: release lock error: %v", err)
		}
	}()

	for {
		if err := loop(); err != nil {
			if err == ErrNoNextVersion {
				return nil
			}
			return err
		}
	}
}

// Up applies all available migrations.
func Up(db *sql.DB, dir string) error {
	return UpTo(db, dir, maxVersion)
}

// UpByOne migrates up by a single version.
func UpByOne(db *sql.DB, dir string) error {
	migrations, err := CollectMigrations(dir, minVersion, maxVersion)
	if err != nil {
		return err
	}

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
		_ = GetDialect().unlock(tx)
		if err := tx.Commit(); err != nil {
			log.Printf("goose: release lock error: %v", err)
		}
	}()

	currentVersion, err := GetDBVersion(db)
	if err != nil {
		return err
	}

	next, err := migrations.Next(currentVersion)
	if err != nil {
		if err == ErrNoNextVersion {
			log.Printf("goose: no migrations to run. current version: %d\n", currentVersion)
		}
		return err
	}

	if err = next.Up(db); err != nil {
		return err
	}

	return nil
}
