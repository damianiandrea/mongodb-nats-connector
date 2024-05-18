package nats

import (
	"context"
	"log/slog"
	"os"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natstest "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func TestNewDefaultClient(t *testing.T) {
	t.Run("should create client with defaults", func(t *testing.T) {
		s := natstest.RunDefaultServer()
		defer s.Shutdown()
		_ = s.EnableJetStream(&natsserver.JetStreamConfig{})

		client, err := NewDefaultClient()

		require.NoError(t, err)
		require.NoError(t, err)
		require.Empty(t, client.url)
		require.Equal(t, "nats", client.name)
		require.Equal(t, slog.Default(), client.logger)
		require.NotNil(t, client.conn)
		require.NotNil(t, client.js)
	})
	t.Run("should create client with the configured options", func(t *testing.T) {
		s := natstest.RunDefaultServer()
		defer s.Shutdown()
		_ = s.EnableJetStream(&natsserver.JetStreamConfig{})
		url := nats.DefaultURL
		logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))

		client, err := NewDefaultClient(
			WithNatsUrl(url),
			WithLogger(logger),
		)

		require.NoError(t, err)
		require.Equal(t, url, client.url)
		require.Equal(t, "nats", client.name)
		require.Equal(t, logger, client.logger)
		require.NotNil(t, client.conn)
		require.NotNil(t, client.js)
	})
	t.Run("should return error cause nats is not available", func(t *testing.T) {
		client, err := NewDefaultClient()

		require.Nil(t, client)
		require.Error(t, err)
	})
}

func TestClient_Name(t *testing.T) {
	t.Run("should return client's name", func(t *testing.T) {
		s := natstest.RunDefaultServer()
		defer s.Shutdown()
		_ = s.EnableJetStream(&natsserver.JetStreamConfig{})
		client, _ := NewDefaultClient()

		name := client.Name()

		require.Equal(t, "nats", name)
	})
}

func TestClient_Monitor(t *testing.T) {
	t.Run("should return nil when client is connected", func(t *testing.T) {
		s := natstest.RunDefaultServer()
		defer s.Shutdown()
		_ = s.EnableJetStream(&natsserver.JetStreamConfig{})
		client, _ := NewDefaultClient()

		err := client.Monitor(context.Background())

		require.NoError(t, err)
	})
	t.Run("should return error when client is disconnected", func(t *testing.T) {
		s := natstest.RunDefaultServer()
		defer s.Shutdown()
		_ = s.EnableJetStream(&natsserver.JetStreamConfig{})
		client, _ := NewDefaultClient()
		client.conn.Close()

		err := client.Monitor(context.Background())

		require.EqualError(t, err, ErrClientDisconnected.Error())
	})
}

func TestClient_Close(t *testing.T) {
	t.Run("should close client connection", func(t *testing.T) {
		s := natstest.RunDefaultServer()
		defer s.Shutdown()
		_ = s.EnableJetStream(&natsserver.JetStreamConfig{})
		client, _ := NewDefaultClient()

		err := client.Close()

		require.NoError(t, err)
		require.True(t, client.conn.IsClosed())
	})
}

func TestClient_AddStream(t *testing.T) {
	t.Run("should add stream with the given name", func(t *testing.T) {
		s := natstest.RunDefaultServer()
		defer s.Shutdown()
		_ = s.EnableJetStream(&natsserver.JetStreamConfig{})
		client, _ := NewDefaultClient()

		err := client.AddStream(context.Background(), &AddStreamOptions{StreamName: "TEST"})

		require.NoError(t, err)
		stream, err := client.js.StreamInfo("TEST")
		require.NoError(t, err)
		require.Equal(t, "TEST", stream.Config.Name)
		require.Contains(t, stream.Config.Subjects, "TEST.*")
		require.Equal(t, nats.FileStorage, stream.Config.Storage)
	})
	t.Run("should return error cause nats is not available", func(t *testing.T) {
		s := natstest.RunDefaultServer()
		defer s.Shutdown()
		_ = s.EnableJetStream(&natsserver.JetStreamConfig{})
		client, _ := NewDefaultClient()
		client.conn.Close()

		err := client.AddStream(context.Background(), &AddStreamOptions{StreamName: "TEST"})

		require.Error(t, err)
	})
}

func TestClient_Publish(t *testing.T) {
	t.Run("should publish message based on the given options", func(t *testing.T) {
		s := natstest.RunDefaultServer()
		defer s.Shutdown()
		_ = s.EnableJetStream(&natsserver.JetStreamConfig{})
		client, _ := NewDefaultClient()
		_, _ = client.js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"TEST.*"},
			Storage:  nats.FileStorage,
		})

		err := client.Publish(context.Background(), &PublishOptions{
			Subj:  "TEST.insert",
			MsgId: "123",
			Data:  []byte("test"),
		})

		require.NoError(t, err)
		sub, err := client.js.SubscribeSync("TEST.insert", nats.OrderedConsumer())
		require.NoError(t, err)
		msg, err := sub.NextMsg(5 * time.Second)
		require.NoError(t, err)
		require.Equal(t, "TEST.insert", msg.Subject)
		require.Contains(t, msg.Header[nats.MsgIdHdr], "123")
		require.Equal(t, []byte("test"), msg.Data)
	})
	t.Run("should run hook after publishing the message", func(t *testing.T) {
		s := natstest.RunDefaultServer()
		defer s.Shutdown()
		_ = s.EnableJetStream(&natsserver.JetStreamConfig{})

		count := 0
		client, _ := NewDefaultClient(
			WithEventListeners(OnMsgPublishedEvent(func(subj string, duration time.Duration) {
				require.Equal(t, subj, "TEST.insert")
				require.NotZero(t, duration)
				count++
			})),
		)

		_, _ = client.js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"TEST.*"},
			Storage:  nats.FileStorage,
		})

		err := client.Publish(context.Background(), &PublishOptions{
			Subj:  "TEST.insert",
			MsgId: "123",
			Data:  []byte("test"),
		})

		require.NoError(t, err)
		require.Equal(t, 1, count)
	})
	t.Run("should return error cause nats is not available", func(t *testing.T) {
		s := natstest.RunDefaultServer()
		defer s.Shutdown()
		_ = s.EnableJetStream(&natsserver.JetStreamConfig{})
		client, _ := NewDefaultClient()
		client.conn.Close()

		err := client.Publish(context.Background(), &PublishOptions{
			Subj:  "TEST.insert",
			MsgId: "123",
			Data:  []byte("test"),
		})

		require.Error(t, err)
	})
	t.Run("should run hook after message publishing failed", func(t *testing.T) {
		s := natstest.RunDefaultServer()
		defer s.Shutdown()
		_ = s.EnableJetStream(&natsserver.JetStreamConfig{})

		count := 0
		client, _ := NewDefaultClient(
			WithEventListeners(OnMsgFailedEvent(func(subj string, duration time.Duration) {
				require.Equal(t, subj, "TEST.insert")
				require.NotZero(t, duration)
				count++
			})),
		)
		client.conn.Close()

		err := client.Publish(context.Background(), &PublishOptions{
			Subj:  "TEST.insert",
			MsgId: "123",
			Data:  []byte("test"),
		})

		require.Error(t, err)
		require.Equal(t, 1, count)
	})
}
