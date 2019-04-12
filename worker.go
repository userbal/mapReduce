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
	err := download(makeURL(task.SourceHost, mapSourceFile(task.N)), tempdir+mapInputFile(task.N))
	if err != nil {
		log.Fatal(err)
	}
	sourceDB, err := openDatabase(tempdir + mapInputFile(task.N))
	if err != nil {
		log.Fatal(err)
	}
	defer sourceDB.Close()

	// create output files
	outputDBs := make([]*sql.DB, 0)
	for r := 0; r < task.R; r++ {
		db, err := createDatabase(tempdir + mapOutputFile(task.N, r))
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

	var key string
	var value string
	for rows.Next() {
		c := make(chan Pair)
		finished := make(chan error)
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
					fmt.Errorf("Insert failed in map inserts %e", err)
				}
				tx.Commit()

			}
			finished <- err
		}()
		err = rows.Scan(&key, &value)
		if err != nil {
			log.Printf("key error")
		}

		pairsProcessed++
		client.Map(key, value, c)
		// wait for err to come  back
		err = <-finished
		//insert pairs sent back through the output database
	}
	for _, elt := range outputDBs {
		elt.Close()
	}

	log.Printf("map tasks processed %v pairs, generated %v pairs", pairsProcessed, pairsGenerated)
	return nil
}
func launchReduceGoRoutines(values chan string, finished chan error, key string, client Interface, outputDB *sql.DB) {
	output := make(chan Pair)
	go client.Reduce(key, values, output)
	pair := <-output
	log.Printf("go3 recieved from output value: %v\n", pair.Value)

	tx, err := outputDB.Begin()
	if err != nil {
		finished <- err
	}
	_, err = tx.Exec("insert into pairs(key, value) values(?, ?)", pair.Key, pair.Value)
	if err != nil {
		finished <- err
	}
	tx.Commit()
	log.Printf("inserted %v, %v into output\n", pair.Key, pair.Value)
	finished <- nil
}

func (task *ReduceTask) Process(tempdir string, client Interface) error {
	//jobs:
	//1. create input database by merging all of the apporpiate output databases from the map phase
	log.Println("1")
	inputDB, err := mergeDatabases(task.SourceHosts, tempdir+reduceInputFile(task.N), tempdir+"temp.sqlite3")
	defer inputDB.Close()

	log.Println("2")
	//2. create the output database
	outputDB, err := createDatabase(tempdir + reduceOutputFile(task.N))
	if err != nil {
		return err
	}
	log.Println("3")
	defer outputDB.Close()

	log.Println("4")
	//3. process all pairs in the correct order. This is trickier than in the map phase Use this query:
	rows, err := inputDB.Query("select key, value from pairs order by key, value")
	if err != nil {
		return err
	}

	log.Println("5")
	var key string
	var value string
	x := 0

	log.Println("6")
	pKey := ""
	log.Println("7")
	Values := make(chan string)
	Finished := make(chan error)
	for rows.Next() {
		err = rows.Scan(&key, &value)
		if err != nil {
			log.Printf("key error")
		}
		log.Println("8")

		log.Println("9")
		if key != pKey && pKey != "" {
			pKey = key
			close(Values)
			log.Println("closed channel")
			err = <-Finished
			log.Println("recieved finished signal")
			if err != nil {
				log.Fatal(err)
			}
			close(Finished)
			Values = make(chan string)
			Finished = make(chan error)
			go launchReduceGoRoutines(Values, Finished, pKey, client, outputDB)
			log.Println("launched go routines", x)
			x++
		} else if pKey == "" {
			log.Println("10")
			pKey = key
			go launchReduceGoRoutines(Values, Finished, pKey, client, outputDB)
			log.Println("launched go routines", x)
			x++
		}
		log.Println("sent value through values channel")
		Values <- value

	}

	return nil
}
