package prometheus

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	commandsStarted = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mongodb_commands_started_total",
			Help: "Total number of started commands.",
		},
		[]string{"database", "command"},
	)

	commandsSucceeded = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mongodb_commands_succeded_total",
			Help: "Total number of succeeded commands.",
		},
		[]string{"database", "command"},
	)

	commandsFailed = promauto.NewCounterVec(
		prometheus.CounterOpts{
			Name: "mongodb_commands_failed_total",
			Help: "Total number of failed commands.",
		},
		[]string{"database", "command", "failure_message"},
	)

	commandSucceededDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "mongodb_succeeded_command_duration_seconds",
			Help:    "Duration of succeeded commands in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"database", "command"},
	)

	commandFailedDuration = promauto.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "mongodb_failed_command_duration_seconds",
			Help:    "Duration of failed commands in seconds.",
			Buckets: prometheus.DefBuckets,
		},
		[]string{"database", "command", "failure_message"},
	)
)

func IncCmdStarted(dbName, cmdName string) {
	commandsStarted.WithLabelValues(dbName, cmdName).Inc()
}

func ObserveCmdSucceeded(dbName, cmdName string, duration time.Duration) {
	commandsSucceeded.WithLabelValues(dbName, cmdName).Inc()
	commandSucceededDuration.WithLabelValues(dbName, cmdName).Observe(duration.Seconds())
}

func ObserveCmdFailed(dbName, cmdName, failure string, duration time.Duration) {
	commandsFailed.WithLabelValues(dbName, cmdName).Inc()
	commandFailedDuration.WithLabelValues(dbName, cmdName, failure).Observe(duration.Seconds())
}

func HTTPHandler() http.Handler {
	return promhttp.Handler()
}
