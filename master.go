package mapreduce

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"time"
)

var m int
var r int
var mapTasks []*MapTask
var reduceTasks []*ReduceTask
var phase int
var nextTask int
var tasksCompleted int

type Work struct {
	mapTasks       []*MapTask
	reduceTasks    []*ReduceTask
	phase          int
	nextTask       int
	tasksCompleted int
}

type Task struct {
	MapTask    *MapTask
	ReduceTask *ReduceTask
	TaskID     int
}

type TaskFinInfo struct {
	TaskID     int
	SourceHost string
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
		fmt.Printf("address %v m %v r %v sourcefile %v\n", address, m, r, sourcefile)
	} else {
		worker(address, masteraddress, client)
		fmt.Printf("address %v masteraddress %v\n", address, masteraddress)
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
	fmt.Printf("m: %v r: %v\n", m, r)
	fmt.Printf("master address: %v", address)

	//_, err := splitDatabase(sourcefile, "data/map_%d_source.sqlite3", m)
	//if err != nil {
	//	log.Fatal(err)
	//}

	Work := new(Work)

	for i := 0; i < m; i++ {
		mapTask := new(MapTask)
		mapTask.M = m
		mapTask.R = r
		mapTask.N = i
		mapTask.SourceHost = address
		mapTasks = append(mapTasks, mapTask)
	}

	for i := 0; i < r; i++ {
		reduceTask := new(ReduceTask)
		reduceTask.M = m
		reduceTask.R = r
		reduceTask.N = i
		reduceTasks = append(reduceTasks, reduceTask)
	}
	server(Work, address)
}

func worker(address string, masterAddress string, notClient Interface) {
	tempdir := "data/"

	go func() {
		http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir("data"))))
		if err := http.ListenAndServe(address, nil); err != nil {
			log.Printf("Error in HTTP server for %s: %v", address, err)
		}
	}()
	for {
		dialGetTask(masterAddress)

		if Task.MapTask != nil {
			log.Println("processing maptask")
			Task.MapTask.Process(tempdir, notClient)
			dialFinished(masterAddress, id)

		} else if Task.ReduceTask != nil {
			log.Println("processing reducetask")
			Task.ReduceTask.Process(tempdir, notClient)
			dialFinished(masterAddress, id)
		} else {
			log.Println("sleeping 1 second")
			time.Sleep(1000 * time.Millisecond)

		}
	}
}

func dialFinished(masterAddress string, id int) {

	client, err := rpc.DialHTTP("tcp", masterAddress)
	if err != nil {
		log.Fatalf("rpc.DialHTTP: %v", err)
	}
	var TaskFinInfo *TaskFinInfo
	var none Nothing
	TaskFinInfo.TaskID = id
	TaskFinInfo.TaskID = id
	err = client.Call("Work.FinishedTask", TaskFinInfo, none)
	if err != nil {
		log.Fatalf("Work.FinishedTask: %v", err)
	}

	if err = client.Close(); err != nil {
		log.Fatalf("error closing the client connection: %v", err)
	}
}

func dialGetTask(masterAddress string, id int) {

	client, err := rpc.DialHTTP("tcp", masterAddress)
	if err != nil {
		log.Fatalf("rpc.DialHTTP: %v", err)
	}
	var none Nothing
	var Task *Task
	err = client.Call("Work.GetTask", none, Task)
	if err != nil {
		log.Fatalf("Work.GetTask: %v", err)
	}

	if err = client.Close(); err != nil {
		log.Fatalf("error closing the client connection: %v", err)
	}

}

func (w Work) GetTask(Nothing, Task *Task) error {
	fmt.Printf("work phase:  %v nextTask:  %v tasksCompleted:  %v len(maptasks): %v len(reducetasks): %v\n", phase, nextTask, tasksCompleted, len(mapTasks), len(reduceTasks))
	if phase == 0 {
		fmt.Println("	1")
		if nextTask < len(mapTasks) {
			fmt.Println("	2")
			Task.MapTask = mapTasks[nextTask]
			Task.MapTask.TaskID = nextTask
			nextTask++
			return nil
		} else {

		}
	} else if phase == 1 {
		fmt.Println("	4")
		if nextTask < len(reduceTasks) {
			fmt.Println("	5")
			Task.ReduceTask = reduceTasks[nextTask]
			Task.ReduceTask.TaskID = nextTask
			nextTask += 1
			return nil
		}
	}
	fmt.Println("	6")
	return nil
}

func (w Work) FinishedTask(TaskFinInfo *TaskFinInfo, junk *Nothing) error {
	if phase == 0 {
		if tasksCompleted < len(mapTasks) {
			tasksCompleted++
			reduceTasks[TaskFinInfo.id%r].SourceHosts = append(reduceTasks[TaskFinInfo.id%r].SourceHosts, TaskFinInfo.SourceHost)
			return nil
		}
		phase = 1
		nextTask = 0
		tasksCompleted = 0
		tasksCompleted++

	} else if phase == 1 {
		if tasksCompleted >= len(reduceTasks) {
			tasksCompleted++
			return nil
		}
		phase = 2
		nextTask = 0
		tasksCompleted = 0
	}
	return nil
}

func server(Work *Work, address string) {

	fmt.Println(address)

	rpc.Register(Work)
	rpc.HandleHTTP()

	http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir("data"))))
	if err := http.ListenAndServe(address, nil); err != nil {
		log.Printf("Error in HTTP server for %s: %v", address, err)
	}

}
