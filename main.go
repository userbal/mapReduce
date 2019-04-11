package main

import (
	"database/sql"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"os"

	_ "github.com/mattn/go-sqlite3"
)

func openDatabase(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		log.Fatal(err)
	}
	//defer db.Close()

	sqlStmt := `
	pragma synchronous = off;
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return db, err
	}

	sqlStmt = `
	pragma journal_mode = off;
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return db, err
	}

	return db, nil

}

func createDatabase(path string) (*sql.DB, error) {

	os.Remove(path)

	db, err := sql.Open("sqlite3", path)
	if err != nil {
		log.Fatal(err)
	}

	sqlStmt := `
	pragma synchronous = off;
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return db, err
	}

	sqlStmt = `
	pragma journal_mode = off;
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return db, err
	}

	sqlStmt = `
	create table pairs (key text, value text);
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return db, err
	}

	return db, nil
}

func splitDatabase(source, outputPattern string, m int) ([]string, error) {
	// example call: paths, err := splitDatabase("input.sqlite3", "output-%d.sqlite3", 50)

	var partitionNames []string

	//opening source
	db, err := openDatabase(source)

	//figure out how many pairs should be in each partition
	stmt, err := db.Prepare("select count(1) from pairs")
	if err != nil {
		log.Println(err)
		return partitionNames, err
	}
	var nPairs int
	err = stmt.QueryRow().Scan(&nPairs)
	if err != nil {
		log.Println(err)
		return partitionNames, err
	}

	if nPairs < m {
		log.Fatal("you must request fewer partitions than rows, (split database)")
	}
	x := float64(nPairs)/float64(m) + 0.5
	x = math.Floor(x)
	rowsPerPartition := int(x)

	//insert rows into partitions
	rows, err := db.Query("select key, value from pairs")
	if err != nil {
		log.Fatal(err)
	}

	var dbIN *sql.DB
	path := fmt.Sprintf(outputPattern, 0)
	partitionNames = append(partitionNames, path)
	dbIN, err = createDatabase(path)
	if err != nil {
		log.Fatal(err)
	}
	//database
	i := 0
	//rows inserted
	a := 0
	for rows.Next() {
		var key string
		var value string
		err = rows.Scan(&key, &value)
		if err != nil {
			log.Printf("key error")
		}

		if a >= rowsPerPartition && i < m-1 {
			i++
			a = 0
			dbIN.Close()
			x := fmt.Sprintf(outputPattern, i)
			partitionNames = append(partitionNames, x)
			dbIN, err = createDatabase(x)
			if err != nil {
				log.Fatal(err)
			}
		}

		tx, err := dbIN.Begin()
		if err != nil {
			log.Fatal(err)
		}
		_, err = tx.Exec("insert into pairs(key, value) values(?, ?)", key, value)
		if err != nil {
			log.Fatal(err)
		}
		tx.Commit()
		a++
	}

	stmt.Close()
	return partitionNames, nil
}

func mergeDatabases(urls []string, path string, temp string) (*sql.DB, error) {
	db, err := createDatabase(path)
	if err != nil {
		log.Fatal(err)
	}

	for _, url := range urls {
		download(url, temp)
		gatherInto(db, temp)
		os.Remove(temp)
	}
	return db, nil
}

func download(url, path string) error {
	res, err := http.Get(url)
	if err != nil {
		log.Println(err)
		return err
	}
	defer res.Body.Close()

	tempFile, err := os.Create(path)
	if err != nil {
		log.Println(err)
		return err
	}
	defer tempFile.Close()

	_, err = io.Copy(tempFile, res.Body)
	if err != nil {
		log.Println(err)
		return err
	}
	return nil
}

func gatherInto(db *sql.DB, path string) error {
	sqlStmt := `
	attach ? as merge
	`
	fmt.Println(path)
	_, err := db.Exec(sqlStmt, path)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return err
	}

	sqlStmt = `
	insert into pairs select * from merge.pairs
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return err
	}

	sqlStmt = `
	detach merge
	`
	_, err = db.Exec(sqlStmt)
	if err != nil {
		log.Printf("%q: %s\n", err, sqlStmt)
		return err
	}

	os.Remove(path)
	return nil
}
