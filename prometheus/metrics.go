package prometheus

import (
	"context"
	"encoding/json"
	"strconv"

	"github.com/Shopify/sarama"
	"github.com/golang/glog"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

const namespace = "kafka_accesslog"

var (
	labels = []string{"status", "request_method"}

	countTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "http_response_count_total",
		Help:      "Amount of processed HTTP requests",
	}, labels)

	bytesTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "http_response_size_bytes",
		Help:      "Total amount of transferred bytes",
	}, labels)

	upstreamSeconds = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: namespace,
		Name:      "http_upstream_time_seconds",
		Help:      "Time needed by upstream servers to handle requests",
	}, labels)

	upstreamSecondsHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "http_upstream_time_seconds_hist",
		Help:      "Time needed by upstream servers to handle requests",
	}, labels)

	responseSeconds = prometheus.NewSummaryVec(prometheus.SummaryOpts{
		Namespace: namespace,
		Name:      "http_response_time_seconds",
		Help:      "Time needed by NGINX to handle requests",
	}, labels)

	responseSecondsHist = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Namespace: namespace,
		Name:      "http_response_time_seconds_hist",
		Help:      "Time needed by NGINX to handle requests",
	}, labels)

	parseErrorsTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: namespace,
		Name:      "parse_errors_total",
		Help:      "Total number of log file lines that could not be parsed",
	})
)

func init() {
	prometheus.MustRegister(countTotal)
	prometheus.MustRegister(bytesTotal)
	prometheus.MustRegister(upstreamSeconds)
	prometheus.MustRegister(upstreamSecondsHist)
	prometheus.MustRegister(responseSeconds)
	prometheus.MustRegister(responseSecondsHist)
	prometheus.MustRegister(parseErrorsTotal)
}

type Metrics struct {
}

type accessLog map[string]interface{}

func (a accessLog) String(key string) (string, error) {
	value, ok := a[key]
	if !ok {
		return "", errors.New("not found")
	}
	result, ok := value.(string)
	if !ok {
		return "", errors.New("no string")
	}
	return result, nil
}

func (a accessLog) Float64(key string) (float64, error) {
	value, err := a.String(key)
	if err != nil {
		return 0, err
	}
	float, err := strconv.ParseFloat(value, 64)
	return float, errors.Wrap(err, "parse float failed")
}

func (a accessLog) Labels() (map[string]string, error) {
	result := make(map[string]string)
	for _, label := range labels {
		value, err := a.String(label)
		if err != nil {
			return nil, err
		}
		result[label] = value
	}
	return result, nil
}

func (m *Metrics) Process(ctx context.Context, msg *sarama.ConsumerMessage) error {
	glog.V(2).Infof("process message %d", msg.Offset)
	var log accessLog
	err := json.Unmarshal(msg.Value, &log)
	if err != nil {
		parseErrorsTotal.Inc()
		return errors.Wrap(err, "unmarshal json failed")
	}

	labels, err := log.Labels()
	if err != nil {
		parseErrorsTotal.Inc()
		return err
	}

	countTotal.With(labels).Inc()

	if bytes, err := log.Float64("body_bytes_sent"); err == nil {
		bytesTotal.With(labels).Add(bytes)
	}

	if upstreamTime, err := log.Float64("upstream_response_time"); err == nil {
		upstreamSeconds.With(labels).Observe(upstreamTime)
		upstreamSecondsHist.With(labels).Observe(upstreamTime)
	}

	if responseTime, err := log.Float64("request_time"); err == nil {
		responseSeconds.With(labels).Observe(responseTime)
		responseSecondsHist.With(labels).Observe(responseTime)
	}

	return nil
}
