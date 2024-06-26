package main

import (
	"github.com/haxii/rmq/v5"
	"log"
	"time"
)

func main() {
	connection, err := rmq.OpenConnection("cleaner", "tcp", "localhost:6379", 2, nil)
	if err != nil {
		panic(err)
	}

	cleaner := rmq.NewCleaner(connection)

	for range time.Tick(time.Second) {
		returned, err := cleaner.Clean()
		if err != nil {
			log.Printf("failed to clean: %s", err)
			continue
		}
		log.Printf("cleaned %d", returned)
	}
}
