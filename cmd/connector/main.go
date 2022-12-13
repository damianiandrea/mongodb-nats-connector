package main

import (
	"log"
	"os"

	"github.com/damianiandrea/go-mongo-nats-connector/internal/config"
	"github.com/damianiandrea/go-mongo-nats-connector/internal/connector"
)

const defaultConfigFileName = "connector.yaml"

func main() {
	configFileName, found := os.LookupEnv("CONFIG_FILE")
	if !found {
		configFileName = defaultConfigFileName
	}
	cfg, err := config.Load(configFileName)
	if err != nil {
		log.Fatalf("error while loading config: %v", err)
	}
	if err = connector.New(cfg).Run(); err != nil {
		log.Fatalf("exiting: %v", err)
	}
}
