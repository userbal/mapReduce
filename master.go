package mapreduce

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
)

var m int
var r int

type Work struct {
	mapTasks         []*MapTask
	reduceTasks      []*ReduceTask
	phase            int
	nextTask         int
	tasksCompleted   int
	reduceOutputUrls []string
	Mux              sync.Mutex
}

type Task struct {
	MapTask    *MapTask
	ReduceTask *ReduceTask
	TaskID     int
	Finished   bool
}

type TaskFinInfo struct {
	TaskID     int
	SourceHost string
	Address    string
	Directory  string
}

type handler func(*Work)
type Nothing struct{}
type Server chan<- handler

func Start(client Interface) error {
	var address string
	var masteraddress string
	var mstr string
	var rstr string
	var err error
	var sourcefile string
	var isMaster bool
	flag.BoolVar(&isMaster, "master", false, "start as a master")
	flag.Parse()

	switch flag.NArg() {

	case 2:
		if !isMaster {
			address = flag.Arg(0)
			masteraddress = flag.Arg(1)
		} else {
			printUsage()
		}

	case 4:
		if isMaster {
			address = flag.Arg(0)
			mstr = flag.Arg(1)
			rstr = flag.Arg(2)
			m, err = strconv.Atoi(mstr)
			if err != nil {
				log.Fatal("m is not an integer")
			}
			r, err = strconv.Atoi(rstr)
			if err != nil {
				log.Fatal("m is not an integer")
			}
			sourcefile = flag.Arg(3)
		} else {
			printUsage()
		}

	default:
		printUsage()
	}

	if isMaster {
		master(address, m, r, sourcefile)
	} else {
		worker(address, masteraddress, client)
	}
	return err
}

func printUsage() {
	fmt.Printf("\nUsage: %s :\n", os.Args[0])
	fmt.Println("master: [-master address (int mapTasks) (in reduceTasks) filename ]")
	fmt.Println("worker: [address masteraddress]")
	flag.PrintDefaults()
	os.Exit(0)
}

func master(address string, m int, r int, sourcefile string) {
	fmt.Println("Setting Up")

	_, err := splitDatabase(sourcefile, "data/map_%d_source.sqlite3", m)
	if err != nil {
		log.Fatal(err)
	}

	w := new(Work)

	for i := 0; i < m; i++ {
		mapTask := new(MapTask)
		mapTask.M = m
		mapTask.R = r
		mapTask.N = i
		mapTask.SourceHost = address
		w.mapTasks = append(w.mapTasks, mapTask)
	}

	for i := 0; i < r; i++ {
		reduceTask := new(ReduceTask)
		reduceTask.M = m
		reduceTask.R = r
		reduceTask.N = i
		w.reduceTasks = append(w.reduceTasks, reduceTask)
	}
	fmt.Println("Ready")
	server(w, address)

}

func (w *Work) GetTask(junk *Nothing, Task *Task) error {
	w.Mux.Lock()
	defer w.Mux.Unlock()

	fmt.Printf("work phase: %v  nextTask: %v  tasksCompleted: %v  len(maptasks): %v  len(reducetasks): %v\n", w.phase, w.nextTask, w.tasksCompleted, len(w.mapTasks), len(w.reduceTasks))
	switch w.phase {
	case 0:
		if w.nextTask < len(w.mapTasks) {
			Task.MapTask = w.mapTasks[w.nextTask]
			Task.TaskID = w.nextTask
			w.nextTask++
			return nil
		} else {

		}
	case 1:
		if w.nextTask < len(w.reduceTasks) {
			Task.ReduceTask = w.reduceTasks[w.nextTask]
			Task.TaskID = w.nextTask
			w.nextTask += 1
			return nil
		}

	case 2:
		Task.Finished = true
	}
	return nil
}

func (w *Work) FinishedTask(TaskFinInfo TaskFinInfo, reply *Nothing) error {
	w.Mux.Lock()
	defer w.Mux.Unlock()
	switch w.phase {
	case 0:
		if w.tasksCompleted < len(w.mapTasks)-1 {
			for i := 0; i < r; i++ {
				w.reduceTasks[TaskFinInfo.TaskID%r].SourceHosts = append(w.reduceTasks[TaskFinInfo.TaskID%r].SourceHosts, "http://"+TaskFinInfo.Address+TaskFinInfo.Directory+mapOutputFile(TaskFinInfo.TaskID, i))
			}
			w.tasksCompleted++
			return nil
		} else {
			for i := 0; i < r; i++ {
				w.reduceTasks[TaskFinInfo.TaskID%r].SourceHosts = append(w.reduceTasks[TaskFinInfo.TaskID%r].SourceHosts, "http://"+TaskFinInfo.Address+TaskFinInfo.Directory+mapOutputFile(TaskFinInfo.TaskID, i))
			}
			w.phase = 1
			w.nextTask = 0
			w.tasksCompleted = 0
		}

	case 1:
		if w.tasksCompleted < len(w.reduceTasks)-1 {
			w.reduceOutputUrls = append(w.reduceOutputUrls, "http://"+TaskFinInfo.Address+TaskFinInfo.Directory+reduceOutputFile(TaskFinInfo.TaskID))
			w.tasksCompleted++
			return nil
		} else {
			w.reduceOutputUrls = append(w.reduceOutputUrls, "http://"+TaskFinInfo.Address+TaskFinInfo.Directory+reduceOutputFile(TaskFinInfo.TaskID))
			w.phase = 2
			fmt.Println(w.reduceOutputUrls)
			inputDB, err := mergeDatabases(w.reduceOutputUrls, "data/final.sqlite3", "data/temp.sqlite3")
			if err != nil {
				log.Fatalf("final merge %v", err)
			}
			inputDB.Close()
		}
	case 2:
		os.Exit(0)
	}
	return nil
}

func server(w *Work, address string) {
	rpc.Register(w)
	rpc.HandleHTTP()

	http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir("data"))))
	if err := http.ListenAndServe(address, nil); err != nil {
		log.Printf("Error in HTTP server for %s: %v", address, err)
	}

}
