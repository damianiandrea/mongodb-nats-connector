package config

import (
	"errors"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	defaultChangeStreamPreAndPostImages = false
	defaultTokensDbName                 = "resume-tokens"
	defaultTokensCollCapped             = true
	defaultTokensCollSizeInBytes        = 4096
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
	if err = validateAndSetDefaults(config); err != nil {
		return nil, fmt.Errorf("invalid config: %v", err)
	}
	return config, nil
}

func validateAndSetDefaults(config *Config) error {
	if config.Connector.Addr == "" {
		config.Connector.Addr = os.Getenv("SERVER_ADDR")
	}

	if config.Connector.Mongo.Uri == "" {
		config.Connector.Mongo.Uri = os.Getenv("MONGO_URI")
	}

	if config.Connector.Nats.Url == "" {
		config.Connector.Nats.Url = os.Getenv("NATS_URL")
	}

	for _, coll := range config.Connector.Collections {
		if coll.DbName == "" {
			return errors.New("dbName property is missing")
		}
		if coll.CollName == "" {
			return errors.New("collName property is missing")
		}
		if coll.ChangeStreamPreAndPostImages == nil {
			defVal := defaultChangeStreamPreAndPostImages
			coll.ChangeStreamPreAndPostImages = &defVal
		}
		if coll.TokensDbName == "" {
			coll.TokensDbName = defaultTokensDbName
		}
		// if missing, use the coll name
		if coll.TokensCollName == "" {
			coll.TokensCollName = coll.CollName
		}
		if coll.TokensCollCapped == nil {
			defVal := defaultTokensCollCapped
			coll.TokensCollCapped = &defVal
		}
		if coll.TokensCollSizeInBytes == nil {
			var defVal int64 = defaultTokensCollSizeInBytes
			coll.TokensCollSizeInBytes = &defVal
		}
		// if missing, use the uppercase of the coll name
		if coll.StreamName == "" {
			coll.StreamName = strings.ToUpper(coll.CollName)
		}
		if strings.EqualFold(coll.DbName, coll.TokensDbName) && strings.EqualFold(coll.CollName, coll.TokensCollName) {
			return fmt.Errorf("cannot store tokens in the same db and collection of the collection to be watched")
		}
	}

	return nil
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
