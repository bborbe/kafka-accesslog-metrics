package prometheus

import (
	"context"
	"encoding/json"
	"github.com/golang/glog"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	countTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Namespace: "kafka_accesslog",
		Name:      "http_response_count_total",
		Help:      "Amount of processed HTTP requests",
	}, []string{"status", "request_method"})
)

func init() {
	prometheus.MustRegister(
		countTotal,
	)
}

type Metrics struct {
}

type accessLog struct {
	Status        string `json:"status"`
	RequestMethod string `json:"request_method"`
}

func (m *Metrics) Process(ctx context.Context, msg *sarama.ConsumerMessage) error {
	glog.V(2).Infof("process message %d", msg.Offset)
	var log accessLog
	err := json.Unmarshal(msg.Value, &log)
	if err != nil {
		return errors.Wrap(err, "unmarshal json failed")
	}
	countTotal.With(map[string]string{
		"status":         log.Status,
		"request_method": log.RequestMethod,
	}).Inc()
	return nil
}
