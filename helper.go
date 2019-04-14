package mapreduce

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
	log.Println("opening database: " + path)
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
	log.Println("creating database: " + path)

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
	log.Println("splitting database, source = " + source)
	// example call: paths, err := splitDatabase("input.sqlite3", "output-%d.sqlite3", 50)

	var partitionNames []string

	//opening source
	db, err := openDatabase(source)

	//figure out how many pairs should be in each partition
	stmt, err := db.Prepare("select count(1) from pairs")
	if err != nil {
		return nil, err
	}
	var nPairs int
	err = stmt.QueryRow().Scan(&nPairs)
	if err != nil {
		return nil, err
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
			i := fmt.Sprintf(outputPattern, i)
			partitionNames = append(partitionNames, i)
			dbIN, err = createDatabase(i)
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
	log.Println("merging database, urls = ")
	for _, url := range urls {
		fmt.Println("	" + url)
	}
	log.Println("path = " + path)
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
	log.Printf("downloading database from: %v, saving to: %v", url, path)
	res, err := http.Get(url)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	log.Printf("res.Body %v", res.Body)

	tempFile, err := os.Create(path)
	log.Printf("file created %v", path)
	if err != nil {
		return err
	}
	defer tempFile.Close()

	_, err = io.Copy(tempFile, res.Body)
	if err != nil {
		return err
	}
	return nil
}

func gatherInto(db *sql.DB, path string) error {
	log.Println("gathering into path = " + path)
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
