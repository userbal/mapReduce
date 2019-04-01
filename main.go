package main

import (
	"database/sql"
	"log"
	"os"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// dbError is an error implementation that includes a time and message.
type dbError struct {
	When time.Time
	What string
}

func openDatabase(path string) (*sql.DB, error) {
	dbError := new(dbError)
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		log.Fatal(err)
		dbError.What = "died while opening"
		return db, errcode
	}
	defer db.Close()

	sqlStmt := `
	pragma synchronous = off;
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		dbError.What = ("died while running " + sqlStmt)
		return db, err
	}

	sqlStmt := `
	pragma journal_mode = off;
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		dbError.What = ("died while running %v", sqlStmt)
		return db, err
	}

	dbError.What = ("")
	return db, err
}

func createDatabase(path string) (*sql.DB, error) {

	os.Remove(path)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
}

func splitDatabase(source, outputPattern string, m int) ([]string, error) {
	// example call
	//paths, err := splitDatabase("input.sqlite3", "output-%d.sqlite3", 50)

}

func main() {}
