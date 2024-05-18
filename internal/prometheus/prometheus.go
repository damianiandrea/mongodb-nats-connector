package prometheus

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type MongoRegisterer struct {
	mongoCommandsStarted   *prometheus.CounterVec
	mongoCommandsSucceeded *prometheus.CounterVec
	mongoCommandsFailed    *prometheus.CounterVec
	mongoCommandDuration   *prometheus.HistogramVec
}

func NewMongoRegisterer(registerer prometheus.Registerer) *MongoRegisterer {
	return &MongoRegisterer{
		mongoCommandsStarted: promauto.With(registerer).NewCounterVec(
			prometheus.CounterOpts{
				Name: "mongodb_commands_started_total",
				Help: "Total number of started commands.",
			},
			[]string{"database", "command"},
		),
		mongoCommandsSucceeded: promauto.With(registerer).NewCounterVec(
			prometheus.CounterOpts{
				Name: "mongodb_commands_succeded_total",
				Help: "Total number of succeeded commands.",
			},
			[]string{"database", "command"},
		),
		mongoCommandsFailed: promauto.With(registerer).NewCounterVec(
			prometheus.CounterOpts{
				Name: "mongodb_commands_failed_total",
				Help: "Total number of failed commands.",
			},
			[]string{"database", "command"},
		),
		mongoCommandDuration: promauto.With(registerer).NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "mongodb_command_duration_seconds",
				Help:    "Duration of commands in seconds.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"database", "command"},
		),
	}
}

func (r *MongoRegisterer) IncMongoCmdStarted(dbName, cmdName string) {
	r.mongoCommandsStarted.WithLabelValues(dbName, cmdName).Inc()
}

func (r *MongoRegisterer) ObserveMongoCmdSucceeded(dbName, cmdName string, duration time.Duration) {
	r.mongoCommandsSucceeded.WithLabelValues(dbName, cmdName).Inc()
	r.mongoCommandDuration.WithLabelValues(dbName, cmdName).Observe(duration.Seconds())
}

func (r *MongoRegisterer) ObserveMongoCmdFailed(dbName, cmdName string, duration time.Duration) {
	r.mongoCommandsFailed.WithLabelValues(dbName, cmdName).Inc()
	r.mongoCommandDuration.WithLabelValues(dbName, cmdName).Observe(duration.Seconds())
}

type NatsRegisterer struct {
	natsMessagesPublished *prometheus.CounterVec
	natsMessagesFailed    *prometheus.CounterVec
	natsMessageDuration   *prometheus.HistogramVec
}

func NewNatsRegisterer(registerer prometheus.Registerer) *NatsRegisterer {
	return &NatsRegisterer{
		natsMessagesPublished: promauto.With(registerer).NewCounterVec(
			prometheus.CounterOpts{
				Name: "nats_messages_published_total",
				Help: "Total number of published messages.",
			},
			[]string{"subject"},
		),
		natsMessagesFailed: promauto.With(registerer).NewCounterVec(
			prometheus.CounterOpts{
				Name: "nats_messages_failed_total",
				Help: "Total number of failed messages.",
			},
			[]string{"subject"},
		),
		natsMessageDuration: promauto.With(registerer).NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "nats_message_duration_seconds",
				Help:    "Duration of messages in seconds.",
				Buckets: prometheus.DefBuckets,
			},
			[]string{"subject"},
		),
	}
}

func (r *NatsRegisterer) ObserveNatsMsgPublished(subj string, duration time.Duration) {
	r.natsMessagesPublished.WithLabelValues(subj).Inc()
	r.natsMessageDuration.WithLabelValues(subj).Observe(duration.Seconds())
}

func (r *NatsRegisterer) ObserveNatsMsgFailed(subj string, duration time.Duration) {
	r.natsMessagesFailed.WithLabelValues(subj).Inc()
	r.natsMessageDuration.WithLabelValues(subj).Observe(duration.Seconds())
}

func DefaultRegisterer() prometheus.Registerer {
	return prometheus.DefaultRegisterer
}

func HTTPHandler() http.Handler {
	return promhttp.Handler()
}
