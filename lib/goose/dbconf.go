package goose

import (
	"database/sql"
	"errors"
	"fmt"
	"os"

	"github.com/lib/pq"
)

// DBDriver encapsulates the info needed to work with
// a specific database driver
type DBDriver struct {
	Name    string
	OpenStr string
	Import  string
	Dialect SqlDialect
}

type DBConf struct {
	MigrationsDir string
	Driver        DBDriver
	Table         string
	PgSchema      string
}

// extract configuration details from the given file
func NewDBConf(p, table, pgschema string) (*DBConf, error) {
	f, err := os.Stat(p)
	if err != nil {
		return nil, fmt.Errorf("Fail open directory %s: %v", p, err)
	}
	if !f.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", p)
	}

	drv := os.Getenv("GOOSE_DRIVER")
	open := os.Getenv("GOOSE_DBSTRING")
	if drv == "" || open == "" {
		return nil, fmt.Errorf("Please provide environment variables GOOSE_DRIVER and GOOSE_DBSTRING to run this application")
	}
	if pgschema == "" {
		pgschema = os.Getenv("GOOSE_PGSCHEMA")
	}

	// Automatically parse postgres urls
	if drv == "postgres" {
		// Assumption: If we can parse the URL, we should
		if parsedURL, err := pq.ParseURL(open); err == nil && parsedURL != "" {
			open = parsedURL
		}
	}

	d := newDBDriver(drv, open, table)

	if !d.IsValid() {
		return nil, errors.New(fmt.Sprintf("Invalid DBConf: %v", d))
	}

	return &DBConf{
		MigrationsDir: p,
		Driver:        d,
		Table:         table,
		PgSchema:      pgschema,
	}, nil
}

// Create a new DBDriver and populate driver specific
// fields for drivers that we know about.
// Further customization may be done in NewDBConf
func newDBDriver(name, open, table string) DBDriver {

	d := DBDriver{
		Name:    name,
		OpenStr: open,
	}

	switch name {
	case "postgres":
		d.Import = "github.com/lib/pq"
		d.Dialect = &PostgresDialect{
			Table: table,
		}

	case "mysql":
		d.Import = "github.com/go-sql-driver/mysql"
		d.Dialect = &MySqlDialect{
			Table: table,
		}
	}

	return d
}

// ensure we have enough info about this driver
func (drv *DBDriver) IsValid() bool {
	return len(drv.Import) > 0 && drv.Dialect != nil
}

// OpenDBFromDBConf wraps database/sql.DB.Open() and configures
// the newly opened DB based on the given DBConf.
//
// Callers must Close() the returned DB.
func OpenDBFromDBConf(conf *DBConf) (*sql.DB, error) {
	db, err := sql.Open(conf.Driver.Name, conf.Driver.OpenStr)
	if err != nil {
		return nil, err
	}

	// if a postgres schema has been specified, apply it
	if conf.Driver.Name == "postgres" && conf.PgSchema != "" {
		if _, err := db.Exec("CREATE SCHEMA IF NOT EXISTS " + conf.PgSchema); err != nil {
			return nil, err
		}
		if _, err := db.Exec("SET search_path TO " + conf.PgSchema); err != nil {
			return nil, err
		}
	}

	return db, nil
}
