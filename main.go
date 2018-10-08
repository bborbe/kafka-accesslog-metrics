package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/bborbe/kafka-accesslog-metrics/kafka"
	"github.com/bborbe/kafka-accesslog-metrics/prometheus"
	"github.com/bborbe/run"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/Shopify/sarama"
	flag "github.com/bborbe/flagenv"
	"github.com/golang/glog"
)

func main() {
	defer glog.Flush()
	glog.CopyStandardLogTo("info")
	runtime.GOMAXPROCS(runtime.NumCPU())

	consumer := &kafka.Consumer{
		MessageHandler: &prometheus.Metrics{},
	}

	flag.StringVar(&consumer.KafkaBrokers, "brokers", "", "Kafka brokers to connect to, as a comma separated list")
	flag.StringVar(&consumer.KafkaTopic, "topic", "", "Kafka topic to consume")
	verbose := flag.Bool("verbose", false, "Turn on Sarama logging")
	port := flag.Int("port", 9000, "Metrics Port")

	flag.Set("logtostderr", "true")
	flag.Parse()

	glog.V(0).Infof("port: %d", *port)
	glog.V(0).Infof("brokers: %s", consumer.KafkaBrokers)
	glog.V(0).Infof("topic: %s", consumer.KafkaTopic)
	glog.V(0).Infof("verbose: %v", *verbose)

	if *verbose {
		sarama.Logger = log.New(os.Stdout, "[sarama] ", log.LstdFlags)
	}

	if consumer.KafkaBrokers == "" || consumer.KafkaTopic == "" {
		flag.PrintDefaults()
		os.Exit(1)
	}

	ctx := contextWithSig(context.Background())

	runServer := func(ctx context.Context) error {
		server := &http.Server{
			Addr:    fmt.Sprintf(":%d", *port),
			Handler: promhttp.Handler(),
		}
		go func() {
			select {
			case <-ctx.Done():
				server.Shutdown(ctx)
			}
		}()
		return server.ListenAndServe()
	}

	glog.V(0).Infof("app started")
	if err := run.CancelOnFirstFinish(ctx, consumer.Consume, runServer); err != nil {
		glog.Exitf("app failed: %+v", err)
	}
	glog.V(0).Infof("app finished")
}

func contextWithSig(ctx context.Context) context.Context {
	ctxWithCancel, cancel := context.WithCancel(ctx)
	go func() {
		defer cancel()

		signalCh := make(chan os.Signal, 1)
		signal.Notify(signalCh, os.Interrupt, syscall.SIGINT, syscall.SIGTERM)

		select {
		case <-signalCh:
		case <-ctx.Done():
		}
	}()

	return ctxWithCancel
}
