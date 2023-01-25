package main

import (
	"log"
	"os"

	"github.com/damianiandrea/mongodb-nats-connector/pkg/config"
	"github.com/damianiandrea/mongodb-nats-connector/pkg/connector"
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
	if conn, err := connector.New(cfg); err != nil {
		log.Fatalf("could not create connector: %v", err)
	} else {
		log.Fatalf("exiting: %v", conn.Run())
	}
}
