package mapreduce

import (
	"database/sql"
	"fmt"
	"io"
	"log"
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
	var partitions []*sql.DB

	//opening source
	db, err := openDatabase(source)

	for i := 0; i < m; i++ {
		path := fmt.Sprintf(outputPattern, i)
		partitionNames = append(partitionNames, path)
		partition, err := createDatabase(path)
		if err != nil {
			log.Fatal(err)
		}
		partitions = append(partitions, partition)
	}

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

	//insert rows into partitions
	rows, err := db.Query("select key, value from pairs")
	if err != nil {
		log.Fatal(err)
	}

	var key string
	var value string
	j := 0
	for rows.Next() {
		err = rows.Scan(&key, &value)
		dbIN := partitions[j]

		tx, err := dbIN.Begin()
		if err != nil {
			log.Fatal(err)
		}
		_, err = tx.Exec("insert into pairs(key, value) values(?, ?)", key, value)
		if err != nil {
			log.Fatal(err)
		}
		tx.Commit()

		if j >= m-1 {
			j = 0
		} else {
			j++
		}
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
		err = download(url, temp)
		if err != nil {
			log.Printf("download failed, path: %v", path)
		}
		err = gatherInto(db, temp)
		if err != nil {
			log.Printf("gatherinto failed, path: %v", path)
		}
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

	tempFile, err := os.Create(path)
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

func mapSourceFile(m int) string       { return fmt.Sprintf("map_%d_source.sqlite3", m) }
func mapInputFile(m int) string        { return fmt.Sprintf("map_%d_input.sqlite3", m) }
func mapOutputFile(m, r int) string    { return fmt.Sprintf("map_%d_output_%d.sqlite3", m, r) }
func reduceInputFile(r int) string     { return fmt.Sprintf("reduce_%d_input.sqlite3", r) }
func reduceOutputFile(r int) string    { return fmt.Sprintf("reduce_%d_output.sqlite3", r) }
func reducePartialFile(r int) string   { return fmt.Sprintf("reduce_%d_partial.sqlite3", r) }
func reduceTempFile(r int) string      { return fmt.Sprintf("reduce_%d_temp.sqlite3", r) }
func makeURL(host, file string) string { return fmt.Sprintf("http://%s/data/%s", host, file) }
