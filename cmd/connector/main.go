package main

import (
	"log"

	"github.com/damianiandrea/go-mongo-nats-connector/internal/connector"
)

func main() {
	if err := connector.New().Run(); err != nil {
		log.Fatalf("exiting: %v", err)
	}
}
