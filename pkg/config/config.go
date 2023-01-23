package config

import (
	"fmt"
	"os"

	"gopkg.in/yaml.v3"
)

func Load(configFileName string) (*Config, error) {
	configFile, err := os.Open(configFileName)
	if err != nil {
		return nil, fmt.Errorf("could not read config file: %v", err)
	}
	defer func() {
		_ = configFile.Close()
	}()
	config := &Config{}
	if err = yaml.NewDecoder(configFile).Decode(config); err != nil {
		return nil, fmt.Errorf("could not unmarshal config file: %v", err)
	}
	return config, nil
}

type Config struct {
	Connector *Connector `yaml:"connector"`
}

type Connector struct {
	Addr        string        `yaml:"addr"`
	Mongo       Mongo         `yaml:"mongo"`
	Nats        Nats          `yaml:"nats"`
	Log         Log           `yaml:"log"`
	Collections []*Collection `yaml:"collections"`
}

type Mongo struct {
	Uri string `yaml:"uri"`
}

type Nats struct {
	Url string `yaml:"url"`
}

type Log struct {
	Level string `yaml:"level"`
}

type Collection struct {
	DbName                       string `yaml:"dbName,omitempty"`
	CollName                     string `yaml:"collName,omitempty"`
	ChangeStreamPreAndPostImages *bool  `yaml:"changeStreamPreAndPostImages,omitempty"`
	TokensDbName                 string `yaml:"tokensDbName,omitempty"`
	TokensCollName               string `yaml:"tokensCollName,omitempty"`
	TokensCollCapped             *bool  `yaml:"tokensCollCapped,omitempty"`
	TokensCollSizeInBytes        *int64 `yaml:"tokensCollSizeInBytes,omitempty"`
	StreamName                   string `yaml:"streamName,omitempty"`
}
