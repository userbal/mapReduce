package main

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"log"
)

type MapTask struct {
	M, R       int    // total number of map and reduce tasks
	N          int    // map task number, 0-based
	SourceHost string // address of host with map input file
}

type ReduceTask struct {
	M, R        int      // total number of map and reduce tasks
	N           int      // reduce task number, 0-based
	SourceHosts []string // addresses of map workers
}

type Pair struct {
	Key   string
	Value string
}

type Interface interface {
	Map(key, value string, output chan<- Pair) error
	Reduce(key string, values <-chan string, output chan<- Pair) error
}

func mapSourceFile(m int) string       { return fmt.Sprintf("map_%d_source.sqlite3", m) }
func mapInputFile(m int) string        { return fmt.Sprintf("map_%d_input.sqlite3", m) }
func mapOutputFile(m, r int) string    { return fmt.Sprintf("map_%d_output_%d.sqlite3", m, r) }
func reduceInputFile(r int) string     { return fmt.Sprintf("reduce_%d_input.sqlite3", r) }
func reduceOutputFile(r int) string    { return fmt.Sprintf("reduce_%d_output.sqlite3", r) }
func reducePartialFile(r int) string   { return fmt.Sprintf("reduce_%d_partial.sqlite3", r) }
func reduceTempFile(r int) string      { return fmt.Sprintf("reduce_%d_temp.sqlite3", r) }
func makeURL(host, file string) string { return fmt.Sprintf("http://%s/data/%s", host, file) }

func (task *MapTask) Process(tempdir string, client Interface) error {
	pairsProcessed := 0
	pairsGenerated := 0
	//download and open input file
	fmt.Println(task.N)
	err := download(task.SourceHost+mapSourceFile(task.N), mapInputFile(task.N))
	if err != nil {
		log.Fatal(err)
	}
	sourceDB, err := openDatabase(mapInputFile(task.N))
	if err != nil {
		log.Fatal(err)
	}

	// create output files
	outputDBs := make([]*sql.DB, 0)
	for r := 0; r <= task.R; r++ {
		db, err := createDatabase(mapOutputFile(task.N, r))
		if err != nil {
			log.Fatal(err)
		}
		outputDBs = append(outputDBs, db)
	}

	// run a database query to select all pairs from the source file
	rows, err := sourceDB.Query("select key, value from pairs")
	if err != nil {
		log.Fatal(err)
	}
	c := make(chan Pair)
	go func() {
		for pair := range c {
			pairsGenerated++

			hash := fnv.New32()
			hash.Write([]byte(pair.Key))
			r := int(hash.Sum32()) % task.R
			tx, err := outputDBs[r].Begin()
			if err != nil {
				log.Fatal(err)
			}
			_, err = tx.Exec("insert into pairs(key, value) values(?, ?)", pair.Key, pair.Value)
			if err != nil {
				log.Fatal(err)
			}
			tx.Commit()

		}
	}()

	var key string
	var value string
	for rows.Next() {
		pairsProcessed++
		err = rows.Scan(&key, &value)
		if err != nil {
			log.Printf("key error")
		}

		client.Map(key, value, c)
		//insert pairs sent back through the output database
		sourceDB.Close()
	}
	for _, elt := range outputDBs {
		elt.Close()
	}
	close(c)
	log.Printf("map taks processed %v pairs, generated %v pairs", pairsProcessed, pairsGenerated)
	return nil
}

//func (task *ReduceTask) Process(tempdir string, client Interface) error {
//	//jobs:
//	//1. create input database by merging all of the apporpiate output databases from the map phase
//	inputDB, err := mergeDatabases(task.SourceHosts, reduceInputFile(task.R), tempdir+"temp.sqlite3")
//
//	//2. create the output database
//	outputDB, err := createDatabase(reduceOutputFile(task.R))
//	if err != nil {
//		return err
//	}
//
//	//3. process all pairs in the correct order. This is trickier than in the map phase Use this query:
//	rows, err := db.Query("select key, value from pairs order by key, value")
//	if err != nil {
//		return err
//	}
//
//	var key string
//	var value string
//	for rows.Next() {
//		err = rows.Scan(&key, &value)
//		if err != nil {
//			log.Printf("key error")
//		}
//		client.Reduce(key, value, c)
//		//insert pairs sent back through the output database
//
//		read := make(chan Pair)
//		write := make(chan Pair)
//		pKey := ""
//		go func (){
//		for pair := range c {
//			if pair.Key != pKey && pKey != "" {
//				close(c)
//			}
//
//			db, err := openDatabase(mapOutputFile(task.M, r))
//			if err != nil {
//				return err
//			}
//			tx, err := db.Begin()
//			if err != nil {
//				return err
//			}
//			_, err = tx.Exec("insert into pairs(key, value) values(?, ?)", pair.Key, pair.Value)
//			if err != nil {
//				return err
//			}
//			tx.Commit()
//			db.Close()
//		}()
//		}
//	}
//}
