package config

import (
	"io/fs"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

var validYamlConfig = `
connector:
  addr: ":8080"
  mongo:
    uri: "mongodb://127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019/?replicaSet=mongodb-nats-connector"
  nats:
    url: "nats://127.0.0.1:4222"
  log:
    level: "debug"
  collections:
    - dbName: "test-connector"
      collName: "coll1"
      changeStreamPreAndPostImages: true
      tokensDbName: "resume-tokens"
      tokensCollName: "coll1"
      tokensCollCapped: true
      tokensCollSizeInBytes: 4096
      streamName: "COLL1"
    - dbName: "test-connector"
      collName: "coll2"
      changeStreamPreAndPostImages: true
      tokensDbName: "resume-tokens"
      tokensCollName: "coll2"
      tokensCollCapped: false
      streamName: "COLL2"
`

var invalidYamlConfig = `
abc12345
`

func TestLoad(t *testing.T) {
	t.Run("should correctly load config from yaml file", func(t *testing.T) {
		dir := t.TempDir()
		configFile := filepath.Join(dir, "connector.yaml")
		_ = os.WriteFile(configFile, []byte(validYamlConfig), fs.ModePerm)

		config, err := Load(configFile)

		addr := ":8080"
		mongoUri := "mongodb://127.0.0.1:27017,127.0.0.1:27018,127.0.0.1:27019/?replicaSet=mongodb-nats-connector"
		natsUrl := "nats://127.0.0.1:4222"
		logLevel := "debug"
		csPrePostImages := true
		capped := true
		nonCapped := false
		collSize := int64(4096)
		require.NoError(t, err)
		require.Equal(t, addr, config.Connector.Addr)
		require.Equal(t, mongoUri, config.Connector.Mongo.Uri)
		require.Equal(t, natsUrl, config.Connector.Nats.Url)
		require.Equal(t, logLevel, config.Connector.Log.Level)
		require.Contains(t, config.Connector.Collections, &Collection{
			DbName:                       "test-connector",
			CollName:                     "coll1",
			ChangeStreamPreAndPostImages: &csPrePostImages,
			TokensDbName:                 "resume-tokens",
			TokensCollName:               "coll1",
			TokensCollCapped:             &capped,
			TokensCollSizeInBytes:        &collSize,
			StreamName:                   "COLL1",
		})
		require.Contains(t, config.Connector.Collections, &Collection{
			DbName:                       "test-connector",
			CollName:                     "coll2",
			ChangeStreamPreAndPostImages: &csPrePostImages,
			TokensDbName:                 "resume-tokens",
			TokensCollName:               "coll2",
			TokensCollCapped:             &nonCapped,
			StreamName:                   "COLL2",
		})
	})
	t.Run("when file not found should return error", func(t *testing.T) {
		dir := t.TempDir()
		configFile := filepath.Join(dir, "connector.yaml")

		config, err := Load(configFile)

		require.Nil(t, config)
		require.Error(t, err)
	})
	t.Run("when yaml decoder fails should return error", func(t *testing.T) {
		dir := t.TempDir()
		configFile := filepath.Join(dir, "connector.yaml")
		_ = os.WriteFile(configFile, []byte(invalidYamlConfig), fs.ModePerm)

		config, err := Load(configFile)

		require.Nil(t, config)
		require.Error(t, err)
	})
}
