package main

import (
	"fmt"
	"log"

	"github.com/sillydong/goose/lib/goose"
)

var downCmd = &Command{
	Name:    "down",
	Usage:   "",
	Summary: "Roll back the version by 1",
	Help:    `down extended help here...`,
	Run:     downRun,
}

func downRun(cmd *Command, args ...string) {

	conf, err := dbConfFromFlags()
	if err != nil {
		log.Fatal(err)
	}

	if len(args) == 0 {
		if err := downAll(conf); err != nil {
			log.Fatal(err)
		}
	} else {
		if err := downOne(conf, args); err != nil {
			log.Fatal(err)
		}
	}
}

func downAll(conf *goose.DBConf) error {
	current, err := goose.GetDBVersion(conf)
	if err != nil {
		log.Fatal(err)
	}

	previous, err := goose.GetPreviousDBVersion(conf.MigrationsDir, current)
	if err != nil {
		return err
	}

	return goose.RunMigrations(conf, previous)
}

func downOne(conf *goose.DBConf, args []string) error {
	fmt.Printf("goose: migrating db using table %s\n", conf.Table)

	versions, err := goose.VersionExist(conf.MigrationsDir, args)
	if err != nil {
		return err
	}

	db, err := goose.OpenDBFromDBConf(conf)
	if err != nil {
		return err
	}
	defer db.Close()

	for v, f := range versions {
		fmt.Printf("running: target %d, file %s\n", v, f.Source)
		if err := goose.RunMigrationOnDb(conf, db, f, false); err != nil {
			return err
		}
	}
	return nil
}
