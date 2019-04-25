package mapreduce

import (
	"database/sql"
	"fmt"
	"hash/fnv"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"time"
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
	defer rows.Close()

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
			finished <- nil
		}()
		err = rows.Scan(&key, &value)
		if err != nil {
			log.Printf("key error")
		}

		pairsProcessed++
		err = client.Map(key, value, c)
		if err != nil {
			log.Fatalf("error calling client map: %v", err)
		}
		// wait for err to come  back
		err = <-finished
		if err != nil {
			log.Fatalf("error calling client map: %v", err)
		}
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
	for pair := range output {

		tx, err := outputDB.Begin()
		if err != nil {
			finished <- err
		}
		_, err = tx.Exec("insert into pairs(key, value) values(?, ?)", pair.Key, pair.Value)
		if err != nil {
			finished <- err
		}
		tx.Commit()
	}
	finished <- nil
}

func (task *ReduceTask) Process(tempdir string, client Interface) error {
	//jobs:
	//1. create input database by merging all of the apporpiate output databases from the map phase
	inputDB, err := mergeDatabases(task.SourceHosts, tempdir+reduceInputFile(task.N), tempdir+"temp.sqlite3")
	defer inputDB.Close()

	//2. create the output database
	outputDB, err := createDatabase(tempdir + reduceOutputFile(task.N))
	if err != nil {
		return err
	}
	defer outputDB.Close()

	//3. process all pairs in the correct order. This is trickier than in the map phase Use this query:
	rows, err := inputDB.Query("select key, value from pairs order by key, value")
	if err != nil {
		return err
	}
	defer rows.Close()

	var key string
	var value string

	pKey := ""
	Values := make(chan string)
	Finished := make(chan error)
	for rows.Next() {
		err = rows.Scan(&key, &value)
		if err != nil {
			log.Printf("key error")
		}

		if key != pKey && pKey != "" {
			pKey = key
			close(Values)
			err = <-Finished
			if err != nil {
				log.Fatal(err)
			}
			close(Finished)
			Values = make(chan string)
			Finished = make(chan error)
			go launchReduceGoRoutines(Values, Finished, pKey, client, outputDB)
		} else if pKey == "" {
			pKey = key
			go launchReduceGoRoutines(Values, Finished, pKey, client, outputDB)
		}
		Values <- value

	}

	return nil
}

func worker(address string, masterAddress string, notClient Interface) {

	tempdir := filepath.Join(os.TempDir(), fmt.Sprintf("mapreduce.%d", os.Getpid()))
	os.Mkdir(tempdir, 755)
	//tempdir := "data/"
	go func() {
		http.Handle(tempdir+"/", http.StripPrefix(tempdir, http.FileServer(http.Dir(tempdir))))
		if err := http.ListenAndServe(address, nil); err != nil {
			log.Printf("Error in HTTP server for %s: %v", address, err)
		}
	}()

	for {
		Task := dialGetTask(masterAddress)

		if Task.Finished {
			return
		}

		if Task.MapTask != nil {
			log.Println("processing maptask")
			Task.MapTask.Process(tempdir+"/", notClient)
			dialFinished(masterAddress, Task.TaskID, address, tempdir+"/")

		} else if Task.ReduceTask != nil {
			log.Println("processing reducetask")
			Task.ReduceTask.Process(tempdir+"/", notClient)
			dialFinished(masterAddress, Task.TaskID, address, tempdir+"/")
		} else {
			log.Println("sleeping 1 second")
			time.Sleep(1000 * time.Millisecond)

		}
	}
	log.Printf("cleaning up\n")
	os.RemoveAll(tempdir)
	os.Exit(0)
}

func dialFinished(masterAddress string, id int, address string, tempdir string) {

	client, err := rpc.DialHTTP("tcp", masterAddress)
	if err != nil {
		log.Fatalf("rpc.DialHTTP: %v", err)
	}
	var none Nothing
	var TaskFinInfo TaskFinInfo
	TaskFinInfo.TaskID = id
	TaskFinInfo.Address = address
	TaskFinInfo.SourceHost = address
	TaskFinInfo.Directory = tempdir
	err = client.Call("Work.FinishedTask", TaskFinInfo, &none)
	if err != nil {
		log.Fatalf("Work.FinishedTask: %v", err)
	}

	if err = client.Close(); err != nil {
		log.Fatalf("error closing the client connection: %v", err)
	}
}

func dialGetTask(masterAddress string) *Task {

	client, err := rpc.DialHTTP("tcp", masterAddress)
	if err != nil {
		log.Fatalf("rpc.DialHTTP: %v", err)
	}
	var none Nothing
	var Task *Task
	err = client.Call("Work.GetTask", none, &Task)
	if err != nil {
		log.Fatalf("Work.GetTask: %v", err)
	}

	if err = client.Close(); err != nil {
		log.Fatalf("error closing the client connection: %v", err)
	}
	return Task

}
