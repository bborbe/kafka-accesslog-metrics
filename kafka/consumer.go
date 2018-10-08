package kafka

import (
	"context"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/golang/glog"
	"github.com/pkg/errors"
)

type messageHandler interface {
	Process(ctx context.Context, msg *sarama.ConsumerMessage) error
}

type Consumer struct {
	MessageHandler messageHandler
	KafkaBrokers   string
	KafkaTopic     string
}

func (c *Consumer) Consume(ctx context.Context) error {
	glog.V(0).Infof("consume topic %s started", c.KafkaTopic)

	config := sarama.NewConfig()
	config.Version = sarama.V2_0_0_0

	consumer, err := sarama.NewConsumer(strings.Split(c.KafkaBrokers, ","), config)
	if err != nil {
		return errors.Wrapf(err, "create consumer with brokers %s failed", c.KafkaBrokers)
	}
	defer consumer.Close()

	partitions, err := consumer.Partitions(c.KafkaTopic)
	if err != nil {
		return errors.Wrapf(err, "get partitions for topic %s failed", c.KafkaTopic)
	}
	glog.V(2).Infof("found kafka partitions: %v", partitions)

	ctx, cancel := context.WithCancel(ctx)

	var wg sync.WaitGroup
	for _, partition := range partitions {
		wg.Add(1)
		go func(partition int32) {
			defer wg.Done()

			glog.V(1).Infof("consume topic %s partition %d started", c.KafkaTopic, partition)
			defer glog.V(1).Infof("consume topic %s partition %d finished", c.KafkaTopic, partition)

			partitionConsumer, err := consumer.ConsumePartition(c.KafkaTopic, partition, sarama.OffsetNewest)
			if err != nil {
				glog.Warningf("create partitionConsumer for topic %s failed: %v", c.KafkaTopic, err)
				return
			}
			defer partitionConsumer.Close()
			for {
				select {
				case <-ctx.Done():
					return
				case err := <-partitionConsumer.Errors():
					glog.Warningf("get error %v", err)
					cancel()
					return
				case msg := <-partitionConsumer.Messages():
					if glog.V(4) {
						glog.Infof("handle message: %s", string(msg.Value))
					}
					if err := c.MessageHandler.Process(ctx, msg); err != nil {
						glog.Warningf("handle message failed: %v", err)
					}
				}
			}
		}(partition)
	}
	wg.Wait()
	glog.V(0).Infof("import to %s finish", c.KafkaTopic)
	return nil
}
