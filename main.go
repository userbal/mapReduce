package main

import (
	"database/sql"
	"io"
	"log"
	"math"
	"net/http"
	"os"
	"strconv"

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
	defer db.Close()

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
	// example call:
	//paths, err := splitDatabase("input.sqlite3", "output-%d.sqlite3", 50)

	//creating databases
	var partitionNames []string
	for i := 0; i <= m; i++ {
		iS := strconv.Itoa(i)
		x := Sprintf(outputPattern, iS)
		_, err := createDatabase(x)
		if err != nil {
			log.Fatal(err)
		}
		partitionNames = append(partitionNames, x)
	}

	//opening source
	db, err := openDatabase(source)

	//figure out how many pairs should be in each partition
	stmt, err := db.Prepare("select count(1) from pairs")
	if err != nil {
		log.Println(err)
		return partitionNames, err
	}
	defer stmt.Close()
	var nPairs int
	err = stmt.QueryRow().Scan(&nPairs)
	if err != nil {
		log.Println(err)
		return partitionNames, err
	}

	if nPairs < m {
		log.Fatal("you must request fewer partitions than rows, (split database)")
	}
	x := nPairs / m
	y := float64(x)
	y = math.Floor(y)
	rowsPerPartition := int(y)

	//insert rows into partitions
	rows, err := db.Query("select key, value from pairs")
	if err != nil {
		log.Fatal(err)
	}

	var key string
	var value string
	i := 0
	a := 0
	for rows.Next() {
		if a >= rowsPerPartition && i < m {
			i++
			a = 0
		}
		if err != nil {
			log.Fatal(err)
		}
		db, err := openDatabase(partitionNames[i])
		tx, err := db.Begin()
		if err != nil {
			log.Fatal(err)
		}
		_, err = tx.Exec("insert into pairs(key, value) values(?, ?)", key, value)
		if err != nil {
			log.Fatal(err)
		}
		tx.Commit()
		db.Close()
	}

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

	tempFile, err := os.Create(path)
	if err != nil {
		log.Println(err)
		return err
	}

	_, err = io.Copy(tempFile, res.Body)
	if err != nil {
		log.Println(err)
		return err
	}
	res.Body.Close()
	tempFile.Close()
	return nil
}

func gatherInto(db *sql.DB, path string) error {
	sqlStmt := `
	attach ? as merge
	`
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

func main() {
	splitDatabase("austen.sqlite3", "output-%d.sqlite3", 5)

}
