package main

import (
	"fmt"
	"log"
	"net/http"
	"runtime"
	"strconv"
	"strings"
	"unicode"
)

type Client struct{}

func (c Client) Map(key, value string, output chan<- Pair) error {
	defer close(output)
	lst := strings.Fields(value)
	for _, elt := range lst {
		word := strings.Map(func(r rune) rune {
			if unicode.IsLetter(r) || unicode.IsDigit(r) {
				return unicode.ToLower(r)
			}
			return -1
		}, elt)
		if len(word) > 0 {
			output <- Pair{Key: word, Value: "1"}
		}
	}
	return nil
}

func (c Client) Reduce(key string, values <-chan string, output chan<- Pair) error {
	defer close(output)
	count := 0
	for v := range values {
		fmt.Println("reduce client value: " + v)
		i, err := strconv.Atoi(v)
		if err != nil {
			return err
		}
		count += i
	}
	p := Pair{Key: key, Value: strconv.Itoa(count)}
	fmt.Printf("reduce client putting pair in output. Pair key: %v value: %v\n", p.Key, p.Value)
	output <- p
	return nil
}

func main() {
	runtime.GOMAXPROCS(1)

	m := 9
	r := 1

	tempdir := "data/"

	go func() {
		address := "localhost:8080"
		http.Handle("/data/", http.StripPrefix("/data", http.FileServer(http.Dir("data"))))
		if err := http.ListenAndServe(address, nil); err != nil {
			log.Printf("Error in HTTP server for %s: %v", address, err)
		}
	}()

	_, err := splitDatabase("austen.sqlite3", "data/map_%d_source.sqlite3", m)
	if err != nil {
		log.Fatal(err)
	}

	client := Client{}
	for i := 0; i < m; i++ {
		fmt.Println("Starting map task", i)
		mapTask := new(MapTask)
		mapTask.M = m
		mapTask.R = r
		mapTask.N = i
		mapTask.SourceHost = "localhost:8080"
		mapTask.Process(tempdir, client)
	}

	var source []string
	source = append(source, "http://localhost:8080/data/map_%d_output_0.sqlite3")
	source = append(source, "http://localhost:8080/data/map_%d_output_1.sqlite3")
	source = append(source, "http://localhost:8080/data/map_%d_output_2.sqlite3")
	for i := 0; i < r; i++ {
		fmt.Println("Starting reduce task", i)
		reduceTask := new(ReduceTask)
		reduceTask.M = m
		reduceTask.R = r
		reduceTask.N = i
		var source2 []string
		for _, elt := range source {
			source2 = append(source2, fmt.Sprintf(elt, i))
		}
		reduceTask.SourceHosts = source2
		reduceTask.Process(tempdir, client)

	}

}
