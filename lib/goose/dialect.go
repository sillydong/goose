package goose

import (
	"database/sql"
)

// SqlDialect abstracts the details of specific SQL dialects
// for goose's few SQL specific statements
type SqlDialect interface {
	createVersionTableSql() string // sql string to create the goose_db_version table
	insertVersionSql() string      // sql string to insert the initial version table row
	dbVersionQuery(db *sql.DB) (*sql.Rows, error)
}

// drivers that we don't know about can ask for a dialect by name
func dialectByName(d, t string) SqlDialect {
	switch d {
	case "postgres":
		return &PostgresDialect{
			Table: t,
		}
	case "mysql":
		return &MySqlDialect{
			Table: t,
		}
	}

	return nil
}

////////////////////////////
// Postgres
////////////////////////////

type PostgresDialect struct {
	Table string
}

func (pg PostgresDialect) createVersionTableSql() string {
	return `CREATE TABLE ` + pg.Table + ` (
            	id serial NOT NULL,
                version_id bigint NOT NULL,
                is_applied boolean NOT NULL,
                tstamp timestamp NULL default now(),
                PRIMARY KEY(id)
            );`
}

func (pg PostgresDialect) insertVersionSql() string {
	return "INSERT INTO " + pg.Table + " (version_id, is_applied) VALUES ($1, $2);"
}

func (pg PostgresDialect) dbVersionQuery(db *sql.DB) (*sql.Rows, error) {
	rows, err := db.Query("SELECT version_id, is_applied from " + pg.Table + " ORDER BY id DESC")

	// XXX: check for postgres specific error indicating the table doesn't exist.
	// for now, assume any error is because the table doesn't exist,
	// in which case we'll try to create it.
	if err != nil {
		return nil, ErrTableDoesNotExist
	}

	return rows, err
}

////////////////////////////
// MySQL
////////////////////////////

type MySqlDialect struct {
	Table string
}

func (m MySqlDialect) createVersionTableSql() string {
	return `CREATE TABLE ` + m.Table + ` (
                id serial NOT NULL,
                version_id bigint NOT NULL,
                is_applied boolean NOT NULL,
                tstamp timestamp NULL default now(),
                PRIMARY KEY(id)
            );`
}

func (m MySqlDialect) insertVersionSql() string {
	return "INSERT INTO " + m.Table + " (version_id, is_applied) VALUES (?, ?);"
}

func (m MySqlDialect) dbVersionQuery(db *sql.DB) (*sql.Rows, error) {
	rows, err := db.Query("SELECT version_id, is_applied from " + m.Table + " ORDER BY id DESC")

	// XXX: check for mysql specific error indicating the table doesn't exist.
	// for now, assume any error is because the table doesn't exist,
	// in which case we'll try to create it.
	if err != nil {
		return nil, ErrTableDoesNotExist
	}

	return rows, err
}
