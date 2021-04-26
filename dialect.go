package goose

import (
	"context"
	"database/sql"
	"fmt"
)

// SQLDialect abstracts the details of specific SQL dialects
// for goose's few SQL specific statements
type SQLDialect interface {
	createVersionTableSQL() string // sql string to create the db version table
	insertVersionSQL() string      // sql string to insert the initial version table row
	deleteVersionSQL() string      // sql string to delete version
	migrationSQL() string          // sql string to retrieve migrations
	dbVersionQuery(db *sql.DB) (*sql.Rows, error)
	lock(db *sql.Tx) error
	unlock(db *sql.Tx) error
}

var dialect SQLDialect = &PostgresDialect{}

// GetDialect gets the SQLDialect
func GetDialect() SQLDialect {
	return dialect
}

// SetDialect sets the SQLDialect
func SetDialect(d string) error {
	switch d {
	case "postgres":
		dialect = &PostgresDialect{}
	case "mysql":
		dialect = &MySQLDialect{}
	case "tidb":
		dialect = &TiDBDialect{}
	default:
		return fmt.Errorf("%q: unknown dialect", d)
	}

	return nil
}

////////////////////////////
// Postgres
////////////////////////////

// PostgresDialect struct.
type PostgresDialect struct{}

func (pg PostgresDialect) createVersionTableSQL() string {
	return fmt.Sprintf(`CREATE TABLE %s (
            	id serial NOT NULL,
                version_id bigint NOT NULL,
                is_applied boolean NOT NULL,
                tstamp timestamp NULL default now(),
                PRIMARY KEY(id)
            );`, TableName())
}

func (pg PostgresDialect) insertVersionSQL() string {
	return fmt.Sprintf("INSERT INTO %s (version_id, is_applied) VALUES ($1, $2);", TableName())
}

func (pg PostgresDialect) dbVersionQuery(db *sql.DB) (*sql.Rows, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT version_id, is_applied from %s ORDER BY id DESC", TableName()))
	if err != nil {
		return nil, err
	}

	return rows, err
}

func (m PostgresDialect) migrationSQL() string {
	return fmt.Sprintf("SELECT tstamp, is_applied FROM %s WHERE version_id=$1 ORDER BY tstamp DESC LIMIT 1", TableName())
}

func (pg PostgresDialect) deleteVersionSQL() string {
	return fmt.Sprintf("DELETE FROM %s WHERE version_id=$1;", TableName())
}

func (pg PostgresDialect) lock(db *sql.Tx) error {
	aid, err := generateAdvisoryLockId(TableName())
	if err != nil {
		return err
	}

	// This will wait indefinitely until the lock can be acquired.
	query := `SELECT pg_advisory_lock($1)`
	if _, err := db.ExecContext(context.Background(), query, aid); err != nil {
		return fmt.Errorf("try lock failed, err: %s", err.Error())
	}

	return nil
}

func (pg PostgresDialect) unlock(db *sql.Tx) error {
	aid, err := generateAdvisoryLockId(TableName())
	if err != nil {
		return err
	}

	query := `SELECT pg_advisory_unlock($1)`
	if _, err := db.ExecContext(context.Background(), query, aid); err != nil {
		return fmt.Errorf("try unlock failed, err: %s", err.Error())
	}
	return nil
}

////////////////////////////
// MySQL
////////////////////////////

// MySQLDialect struct.
type MySQLDialect struct{}

func (m MySQLDialect) createVersionTableSQL() string {
	return fmt.Sprintf(`CREATE TABLE %s (
                id serial NOT NULL,
                version_id bigint NOT NULL,
                is_applied boolean NOT NULL,
                tstamp timestamp NULL default now(),
                PRIMARY KEY(id)
            );`, TableName())
}

func (m MySQLDialect) insertVersionSQL() string {
	return fmt.Sprintf("INSERT INTO %s (version_id, is_applied) VALUES (?, ?);", TableName())
}

func (m MySQLDialect) dbVersionQuery(db *sql.DB) (*sql.Rows, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT version_id, is_applied from %s ORDER BY id DESC", TableName()))
	if err != nil {
		return nil, err
	}

	return rows, err
}

func (m MySQLDialect) migrationSQL() string {
	return fmt.Sprintf("SELECT tstamp, is_applied FROM %s WHERE version_id=? ORDER BY tstamp DESC LIMIT 1", TableName())
}

func (m MySQLDialect) deleteVersionSQL() string {
	return fmt.Sprintf("DELETE FROM %s WHERE version_id=?;", TableName())
}

func (m MySQLDialect) lock(db *sql.Tx) error {
	aid, err := generateAdvisoryLockId(TableName())
	if err != nil {
		return err
	}

	log.Printf("goose: lock lockid: %v", aid)

	// keep trying if get_lock timeout
	retryCount := 0
	for {
		if retryCount > maxRetry {
			return fmt.Errorf("fail get lock after %d retries", maxRetry)
		}
		query := "SELECT GET_LOCK(?, 30)"
		var success bool
		if err := db.QueryRowContext(context.Background(), query, aid).Scan(&success); err != nil {
			return fmt.Errorf("try lock failed, err: %s", err.Error())
		}

		if success {
			return nil
		}

		retryCount++
		log.Println("goose: lock needs retry")
	}
}

func (m MySQLDialect) unlock(db *sql.Tx) error {
	aid, err := generateAdvisoryLockId(TableName())
	if err != nil {
		return err
	}

	log.Printf("goose: unlock lockid: %v", aid)

	query := `SELECT RELEASE_LOCK(?)`
	if _, err := db.ExecContext(context.Background(), query, aid); err != nil {
		return fmt.Errorf("try unlock failed, err: %s", err.Error())
	}

	return nil
}

////////////////////////////
// TiDB
////////////////////////////

// TiDBDialect struct.
type TiDBDialect struct {
	isLocked bool
}

func (m TiDBDialect) createVersionTableSQL() string {
	return fmt.Sprintf(`CREATE TABLE %s (
                id BIGINT UNSIGNED NOT NULL AUTO_INCREMENT UNIQUE,
                version_id bigint NOT NULL,
                is_applied boolean NOT NULL,
                tstamp timestamp NULL default now(),
                PRIMARY KEY(id)
            );`, TableName())
}

func (m TiDBDialect) insertVersionSQL() string {
	return fmt.Sprintf("INSERT INTO %s (version_id, is_applied) VALUES (?, ?);", TableName())
}

func (m TiDBDialect) dbVersionQuery(db *sql.DB) (*sql.Rows, error) {
	rows, err := db.Query(fmt.Sprintf("SELECT version_id, is_applied from %s ORDER BY id DESC", TableName()))
	if err != nil {
		return nil, err
	}

	return rows, err
}

func (m TiDBDialect) migrationSQL() string {
	return fmt.Sprintf("SELECT tstamp, is_applied FROM %s WHERE version_id=? ORDER BY tstamp DESC LIMIT 1", TableName())
}

func (m TiDBDialect) deleteVersionSQL() string {
	return fmt.Sprintf("DELETE FROM %s WHERE version_id=?;", TableName())
}

func (m TiDBDialect) lock(db *sql.Tx) error {
	aid, err := generateAdvisoryLockId(TableName())
	if err != nil {
		return err
	}

	// keep trying if get_lock timeout
	retryCount := 0
	for {
		if retryCount > maxRetry {
			return fmt.Errorf("fail get lock after %d retries", retryCount)
		}
		query := "SELECT GET_LOCK(?, 30)"
		var success bool
		if err := db.QueryRowContext(context.Background(), query, aid).Scan(&success); err != nil {
			return fmt.Errorf("try lock failed, err: %s", err.Error())
		}

		if success {
			return nil
		}

		retryCount++
	}
}

func (m TiDBDialect) unlock(db *sql.Tx) error {
	aid, err := generateAdvisoryLockId(TableName())
	if err != nil {
		return err
	}

	log.Printf("goose: unlock lockid: %v", aid)

	query := `SELECT RELEASE_LOCK(?)`
	if _, err := db.ExecContext(context.Background(), query, aid); err != nil {
		return fmt.Errorf("try unlock failed, err: %s", err.Error())
	}
	return nil
}
