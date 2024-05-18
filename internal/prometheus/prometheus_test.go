package prometheus

import (
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
)

func TestMongoRegisterer_IncMongoCmdStarted(t *testing.T) {
	var (
		registerer     = prometheus.NewPedanticRegistry()
		expectedDbName = "test-db"
		expectedCmd    = "insert"
	)

	mr := NewMongoRegisterer(registerer)
	mr.IncMongoCmdStarted(expectedDbName, expectedCmd)

	startedTotal := getMetric(t, registerer, "mongodb_commands_started_total")
	require.NotNil(t, startedTotal)
	require.Equal(t, 1.0, startedTotal.Counter.GetValue())
	requireMetricHasLabel(t, startedTotal, "database", expectedDbName)
	requireMetricHasLabel(t, startedTotal, "command", expectedCmd)
}

func TestMongoRegisterer_ObserveMongoCmdSucceeded(t *testing.T) {
	var (
		registerer       = prometheus.NewPedanticRegistry()
		expectedDbName   = "test-db"
		expectedCmd      = "insert"
		expectedDuration = 1 * time.Second
	)

	mr := NewMongoRegisterer(registerer)
	mr.ObserveMongoCmdSucceeded(expectedDbName, expectedCmd, expectedDuration)

	succeededTotal := getMetric(t, registerer, "mongodb_commands_succeded_total")
	require.NotNil(t, succeededTotal)
	require.Equal(t, 1.0, succeededTotal.Counter.GetValue())
	requireMetricHasLabel(t, succeededTotal, "database", expectedDbName)
	requireMetricHasLabel(t, succeededTotal, "command", expectedCmd)

	duration := getMetric(t, registerer, "mongodb_command_duration_seconds")
	require.NotNil(t, duration)
	require.Equal(t, expectedDuration.Seconds(), duration.Histogram.GetSampleSum())
	requireMetricHasLabel(t, duration, "database", expectedDbName)
	requireMetricHasLabel(t, duration, "command", expectedCmd)
}

func TestMongoRegisterer_ObserveMongoCmdFailed(t *testing.T) {
	var (
		registerer       = prometheus.NewPedanticRegistry()
		expectedDbName   = "test-db"
		expectedCmd      = "insert"
		expectedDuration = 1 * time.Second
	)

	mr := NewMongoRegisterer(registerer)
	mr.ObserveMongoCmdFailed(expectedDbName, expectedCmd, expectedDuration)

	failedTotal := getMetric(t, registerer, "mongodb_commands_failed_total")
	require.NotNil(t, failedTotal)
	require.Equal(t, 1.0, failedTotal.Counter.GetValue())
	requireMetricHasLabel(t, failedTotal, "database", expectedDbName)
	requireMetricHasLabel(t, failedTotal, "command", expectedCmd)

	duration := getMetric(t, registerer, "mongodb_command_duration_seconds")
	require.NotNil(t, duration)
	require.Equal(t, expectedDuration.Seconds(), duration.Histogram.GetSampleSum())
	requireMetricHasLabel(t, duration, "database", expectedDbName)
	requireMetricHasLabel(t, duration, "command", expectedCmd)
}

func TestNatsRegisterer_ObserveNatsMsgPublished(t *testing.T) {
	var (
		registerer       = prometheus.NewPedanticRegistry()
		expectedSubject  = "coll1.insert"
		expectedDuration = 1 * time.Second
	)

	nr := NewNatsRegisterer(registerer)
	nr.ObserveNatsMsgPublished(expectedSubject, expectedDuration)

	publishedTotal := getMetric(t, registerer, "nats_messages_published_total")
	require.NotNil(t, publishedTotal)
	require.Equal(t, 1.0, publishedTotal.Counter.GetValue())
	requireMetricHasLabel(t, publishedTotal, "subject", expectedSubject)

	duration := getMetric(t, registerer, "nats_message_duration_seconds")
	require.NotNil(t, duration)
	require.Equal(t, expectedDuration.Seconds(), duration.Histogram.GetSampleSum())
	requireMetricHasLabel(t, duration, "subject", expectedSubject)
}

func TestNatsRegisterer_ObserveNatsMsgFailed(t *testing.T) {
	var (
		registerer       = prometheus.NewPedanticRegistry()
		expectedSubject  = "coll1.insert"
		expectedDuration = 1 * time.Second
	)

	nr := NewNatsRegisterer(registerer)
	nr.ObserveNatsMsgFailed(expectedSubject, expectedDuration)

	failedTotal := getMetric(t, registerer, "nats_messages_failed_total")
	require.NotNil(t, failedTotal)
	require.Equal(t, 1.0, failedTotal.Counter.GetValue())
	requireMetricHasLabel(t, failedTotal, "subject", expectedSubject)

	duration := getMetric(t, registerer, "nats_message_duration_seconds")
	require.NotNil(t, duration)
	require.Equal(t, expectedDuration.Seconds(), duration.Histogram.GetSampleSum())
	requireMetricHasLabel(t, duration, "subject", expectedSubject)
}

func TestDefaultRegisterer(t *testing.T) {
	registerer := DefaultRegisterer()

	require.Equal(t, prometheus.DefaultRegisterer, registerer)
}

func TestHTTPHandler(t *testing.T) {
	var (
		rec = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	)

	HTTPHandler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	res, err := io.ReadAll(rec.Body)
	require.NoError(t, err)
	require.NotEmpty(t, res)
}

func getMetric(t *testing.T, gatherer prometheus.Gatherer, metricFamilyName string) *dto.Metric {
	t.Helper()

	mfs, err := gatherer.Gather()
	require.NoError(t, err)

	for _, mf := range mfs {
		if mf.GetName() == metricFamilyName {
			return mf.GetMetric()[0]
		}
	}
	return nil
}

func requireMetricHasLabel(t *testing.T, metric *dto.Metric, name, value string) {
	t.Helper()

	for _, l := range metric.GetLabel() {
		if l.GetName() == name && l.GetValue() == value {
			return
		}
	}
	require.FailNowf(t, "metric has no label with name %s and value %s", name, value)
}
