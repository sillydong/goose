package main

import (
	"fmt"
	"log"

	"github.com/sillydong/goose/lib/goose"
)

var upCmd = &Command{
	Name:    "up",
	Usage:   "",
	Summary: "Migrate the DB to the most recent version available",
	Help:    `up extended help here...`,
	Run:     upRun,
}

func upRun(cmd *Command, args ...string) {
	conf, err := dbConfFromFlags()
	if err != nil {
		log.Fatal(err)
	}

	if len(args) == 0 {
		if err := upAll(conf); err != nil {
			log.Fatal(err)
		}
	} else {
		if err := upOne(conf, args); err != nil {
			log.Fatal(err)
		}
	}

}

func upAll(conf *goose.DBConf) error {
	target, err := goose.GetMostRecentDBVersion(conf.MigrationsDir)
	if err != nil {
		return err
	}

	return goose.RunMigrations(conf, target)
}

func upOne(conf *goose.DBConf, args []string) error {
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
		if err := goose.RunMigrationOnDb(conf, db, f, true); err != nil {
			return err
		}
	}
	return nil
}
