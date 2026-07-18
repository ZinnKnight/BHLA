package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"BHLA/shared/logging"
)

type Message struct {
	Topic  string
	Key    []byte
	Value  []byte
	Header map[string]string
}

type Handler func(ctx context.Context, message Message) error

type ConsumerConfig struct {
	Brokers    []string
	Group      string
	Topics     []string
	StartAtEnd bool
}

type Consumer struct {
	client *kgo.Client
	logger logging.Logger
}

func NewConsumer(ctx context.Context, cfg ConsumerConfig, logger logging.Logger) (*Consumer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.DisableAutoCommit(),
	}
	if cfg.StartAtEnd {
		opts = append(opts, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka consumer: new client: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := cl.Ping(pingCtx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("kafka consumer: ping: %w", err)
	}
	return &Consumer{client: cl, logger: logger}, nil
}

func (c *Consumer) Run(ctx context.Context, handler Handler) {
	c.logger.LogInfo("kafka consumer started")
	for {
		fetches := c.client.PollFetches(ctx)
		if fetches.IsClientClosed() {
			c.logger.LogInfo("kafka consumer stopped (client closed)")
			return
		}
		if ctx.Err() != nil {
			c.logger.LogInfo("kafka consumer stopped (ctx done)")
			return
		}

		fetches.EachError(func(topic string, partition int32, err error) {
			c.logger.LogError("kafka consumer fetch error",
				logging.Field{Key: "topic", Value: topic},
				logging.Field{Key: "partition", Value: partition},
				logging.Err(err),
			)
		})

		var ok []*kgo.Record
		iter := fetches.RecordIter()
		for !iter.Done() {
			rec := iter.Next()
			headers := make(map[string]string, len(rec.Headers))
			for _, h := range rec.Headers {
				headers[h.Key] = string(h.Value)
			}
			if err := handler(ctx, Message{Topic: rec.Topic, Key: rec.Key, Value: rec.Value, Header: headers}); err != nil {
				c.logger.LogError("kafka consumer handler error",
					logging.Field{Key: "topic", Value: rec.Topic},
					logging.Err(err),
				)
				break
			}
			ok = append(ok, rec)
		}

		if len(ok) > 0 {
			if err := c.client.CommitRecords(ctx, ok...); err != nil {
				c.logger.LogError("kafka consumer commit error", logging.Err(err))
			}
		}
	}
}

func (c *Consumer) Close() { c.client.Close() }
