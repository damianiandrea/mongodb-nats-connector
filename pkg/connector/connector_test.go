package connector

import (
	"context"
	"log/slog"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/damianiandrea/mongodb-nats-connector/internal/mongo"
	"github.com/damianiandrea/mongodb-nats-connector/internal/nats"
)

func TestNew(t *testing.T) {
	t.Run("should create connector with defaults", func(t *testing.T) {
		var (
			mongoClient = &mockMongoClient{}
			natsClient  = &mockNatsClient{}
		)

		conn, err := New(
			withMongoClient(mongoClient), // avoid connecting to a real mongo instance
			withNatsClient(natsClient),   // avoid connecting to a real nats instance
		)

		require.NoError(t, err)
		require.Equal(t, slog.LevelInfo, conn.options.logLevel)
		require.Empty(t, conn.options.mongoUri)
		require.Equal(t, mongoClient, conn.options.mongoClient)
		require.Empty(t, conn.options.natsUrl)
		require.Equal(t, natsClient, conn.options.natsClient)
		require.NotNil(t, conn.options.ctx)
		require.NotNil(t, conn.options.stop)
		require.Empty(t, conn.options.serverAddr)
		require.NotNil(t, conn.logger)
		require.NotNil(t, conn.server)
		require.Empty(t, conn.options.collections)
	})
	t.Run("should create connector with all supported log levels", func(t *testing.T) {
		var (
			mongoClient = &mockMongoClient{}
			natsClient  = &mockNatsClient{}
		)

		supportedLevels := map[string]slog.Level{
			"info":  slog.LevelInfo,
			"debug": slog.LevelDebug,
			"warn":  slog.LevelWarn,
			"error": slog.LevelError,
		}

		for levelStr, level := range supportedLevels {
			conn, err := New(
				withMongoClient(mongoClient), // avoid connecting to a real mongo instance
				withNatsClient(natsClient),   // avoid connecting to a real nats instance
				WithLogLevel(levelStr),
			)

			require.NoError(t, err)
			require.Equal(t, level, conn.options.logLevel)
		}
	})
	t.Run("should create connector with given options", func(t *testing.T) {
		var (
			logLevel    = "debug"
			mongoUri    = "localhost:27017"
			mongoClient = &mockMongoClient{}
			natsUrl     = "localhost:4222"
			natsClient  = &mockNatsClient{}
			serverAddr  = ":8080"
		)

		conn, err := New(
			WithLogLevel(logLevel),
			WithMongoUri(mongoUri),
			withMongoClient(mongoClient),
			WithNatsUrl(natsUrl),
			withNatsClient(natsClient),
			WithContext(context.TODO()),
			WithServerAddr(serverAddr),
		)

		require.NoError(t, err)
		require.Equal(t, slog.LevelDebug, conn.options.logLevel)
		require.Equal(t, mongoUri, conn.options.mongoUri)
		require.Equal(t, mongoClient, conn.options.mongoClient)
		require.Equal(t, natsUrl, conn.options.natsUrl)
		require.Equal(t, natsClient, conn.options.natsClient)
		require.NotNil(t, conn.options.ctx)
		require.NotNil(t, conn.options.stop)
		require.Equal(t, serverAddr, conn.options.serverAddr)
		require.NotNil(t, conn.logger)
		require.NotNil(t, conn.server)
		require.Empty(t, conn.options.collections)
	})
	t.Run("should create connector with collection defaults", func(t *testing.T) {
		var (
			mongoClient = &mockMongoClient{}
			natsClient  = &mockNatsClient{}
			dbName      = "connector-db"
			collName    = "coll1"
		)

		conn, err := New(
			withMongoClient(mongoClient), // avoid connecting to a real mongo instance
			withNatsClient(natsClient),   // avoid connecting to a real nats instance
			WithCollection(dbName, collName),
		)

		require.NoError(t, err)
		require.Contains(t, conn.options.collections, &collection{
			dbName:                       dbName,
			collName:                     collName,
			changeStreamPreAndPostImages: false,
			tokensDbName:                 "resume-tokens",
			tokensCollName:               collName,
			tokensCollCapped:             false,
			tokensCollSizeInBytes:        0,
			streamName:                   strings.ToUpper(collName),
		})
	})
	t.Run("should create connector with given collection options", func(t *testing.T) {
		var (
			mongoClient     = &mockMongoClient{}
			natsClient      = &mockNatsClient{}
			dbName          = "connector-db"
			collName        = "coll1"
			tokensDbName    = "tokens-db"
			tokensCollName  = "coll1-tokens"
			collSizeInBytes = int64(2048)
			streamName      = "coll1-stream"
		)

		conn, err := New(
			withMongoClient(mongoClient), // avoid connecting to a real mongo instance
			withNatsClient(natsClient),   // avoid connecting to a real nats instance
			WithCollection(dbName, collName,
				WithChangeStreamPreAndPostImages(),
				WithTokensDbName(tokensDbName),
				WithTokensCollName(tokensCollName),
				WithTokensCollCapped(collSizeInBytes),
				WithStreamName(streamName),
			),
		)

		require.NoError(t, err)
		require.Contains(t, conn.options.collections, &collection{
			dbName:                       dbName,
			collName:                     collName,
			changeStreamPreAndPostImages: true,
			tokensDbName:                 tokensDbName,
			tokensCollName:               tokensCollName,
			tokensCollCapped:             true,
			tokensCollSizeInBytes:        collSizeInBytes,
			streamName:                   streamName,
		})
	})
	t.Run("should return error cause dbName is missing", func(t *testing.T) {
		conn, err := New(
			WithCollection("", "test-coll"),
		)

		require.Nil(t, conn)
		require.EqualError(t, err, ErrDbNameMissing.Error())
	})
	t.Run("should return error cause collName is missing", func(t *testing.T) {
		conn, err := New(
			WithCollection("test-db", ""),
		)

		require.Nil(t, conn)
		require.EqualError(t, err, ErrCollNameMissing.Error())
	})
	t.Run("should return error cause collSizeInBytes is less than 0", func(t *testing.T) {
		conn, err := New(
			WithCollection("test-db", "test-coll", WithTokensCollCapped(-1)),
		)

		require.Nil(t, conn)
		require.EqualError(t, err, ErrInvalidCollSizeInBytes.Error())
	})
	t.Run("should return error cause collSizeInBytes is 0", func(t *testing.T) {
		conn, err := New(
			WithCollection("test-db", "test-coll", WithTokensCollCapped(0)),
		)

		require.Nil(t, conn)
		require.EqualError(t, err, ErrInvalidCollSizeInBytes.Error())
	})
	t.Run("should return error cause tokens cannot be stored in the collection to be watched", func(t *testing.T) {
		var (
			dbName   = "test-db"
			collName = "test-coll"
		)

		conn, err := New(
			WithCollection(dbName, collName, WithTokensDbName(dbName), WithTokensCollName(collName)),
		)

		require.Nil(t, conn)
		require.EqualError(t, err, ErrInvalidDbAndCollNames.Error())
	})
}

func TestConnector_Run(t *testing.T) {
	t.Run("should run connector", func(t *testing.T) {
		var (
			mongoClient     = &mockMongoClient{}
			natsClient      = &mockNatsClient{}
			ctx, cancel     = context.WithCancel(context.Background())
			dbName          = "connector-db"
			collName        = "coll1"
			tokensDbName    = "tokens-db"
			tokensCollName  = "coll1-tokens"
			collSizeInBytes = int64(2048)
			streamName      = "coll1-stream"
		)

		conn, _ := New(
			withMongoClient(mongoClient), // avoid connecting to a real mongo instance
			withNatsClient(natsClient),   // avoid connecting to a real nats instance
			WithContext(ctx),
			WithCollection(dbName, collName,
				WithChangeStreamPreAndPostImages(),
				WithTokensDbName(tokensDbName),
				WithTokensCollName(tokensCollName),
				WithTokensCollCapped(collSizeInBytes),
				WithStreamName(streamName),
			),
		)

		errCh := make(chan error)
		go func() {
			errCh <- conn.Run()
		}()

		require.Eventually(t, func() bool {
			return slices.Contains(mongoClient.createCollectionOpts, mongo.CreateCollectionOptions{
				DbName:                       dbName,
				CollName:                     collName,
				Capped:                       false,
				SizeInBytes:                  0,
				ChangeStreamPreAndPostImages: true,
			})
		}, 1*time.Second, 100*time.Millisecond)
		require.Eventually(t, func() bool {
			return slices.Contains(mongoClient.createCollectionOpts, mongo.CreateCollectionOptions{
				DbName:                       tokensDbName,
				CollName:                     tokensCollName,
				Capped:                       true,
				SizeInBytes:                  collSizeInBytes,
				ChangeStreamPreAndPostImages: false,
			})
		}, 1*time.Second, 100*time.Millisecond)
		require.Eventually(t, func() bool {
			return slices.Contains(natsClient.addStreamOpts, nats.AddStreamOptions{
				StreamName: streamName,
			})
		}, 1*time.Second, 100*time.Millisecond)
		require.Eventually(t, func() bool {
			return slices.ContainsFunc(mongoClient.WatchCollectionOpts, func(o mongo.WatchCollectionOptions) bool {
				return o.WatchedDbName == dbName &&
					o.WatchedCollName == collName &&
					o.ResumeTokensDbName == tokensDbName &&
					o.ResumeTokensCollName == tokensCollName &&
					o.ResumeTokensCollCapped == true &&
					o.StreamName == streamName &&
					o.ChangeEventHandler != nil
			})
		}, 1*time.Second, 100*time.Millisecond)

		cancel()
		err := <-errCh
		require.ErrorIs(t, err, http.ErrServerClosed)
	})
}

type mockMongoClient struct {
	closed               bool
	name                 string
	monitorErr           error
	createCollectionOpts []mongo.CreateCollectionOptions
	WatchCollectionOpts  []mongo.WatchCollectionOptions
}

func (m *mockMongoClient) Close() error {
	m.closed = false
	return nil
}

func (m *mockMongoClient) Name() string {
	return m.name
}

func (m *mockMongoClient) Monitor(_ context.Context) error {
	return m.monitorErr
}

func (m *mockMongoClient) CreateCollection(_ context.Context, opts *mongo.CreateCollectionOptions) error {
	m.createCollectionOpts = append(m.createCollectionOpts, *opts)
	return nil
}

func (m *mockMongoClient) WatchCollection(_ context.Context, opts *mongo.WatchCollectionOptions) error {
	m.WatchCollectionOpts = append(m.WatchCollectionOpts, *opts)
	return nil
}

type mockNatsClient struct {
	closed        bool
	name          string
	monitorErr    error
	addStreamOpts []nats.AddStreamOptions
	publishOpts   []nats.PublishOptions
}

func (m *mockNatsClient) Close() error {
	m.closed = true
	return nil
}

func (m *mockNatsClient) Name() string {
	return m.name
}

func (m *mockNatsClient) Monitor(_ context.Context) error {
	return m.monitorErr
}

func (m *mockNatsClient) AddStream(_ context.Context, opts *nats.AddStreamOptions) error {
	m.addStreamOpts = append(m.addStreamOpts, *opts)
	return nil
}

func (m *mockNatsClient) Publish(_ context.Context, opts *nats.PublishOptions) error {
	m.publishOpts = append(m.publishOpts, *opts)
	return nil
}
