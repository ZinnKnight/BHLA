package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Config struct {
	Brokers  []string
	ClientID string
}

type Header struct {
	Key   string
	Value []byte
}

type Record struct {
	Topic   string
	Key     []byte
	Value   []byte
	Headers []Header
}

type Producer struct {
	client *kgo.Client
}

func NewProducer(ctx context.Context, cfg Config) (*Producer, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.RequiredAcks(kgo.AllISRAcks()),
	}
	if cfg.ClientID != "" {
		opts = append(opts, kgo.ClientID(cfg.ClientID))
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("kafka: new client: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := cl.Ping(pingCtx); err != nil {
		cl.Close()
		return nil, fmt.Errorf("kafka: ping brokers: %w", err)
	}
	return &Producer{client: cl}, nil
}

func toKgoRecord(topic string, key, value []byte, headers []Header) *kgo.Record {
	rec := &kgo.Record{Topic: topic, Key: key, Value: value}
	for _, h := range headers {
		rec.Headers = append(rec.Headers, kgo.RecordHeader{Key: h.Key, Value: h.Value})
	}
	return rec
}

func (p *Producer) Publish(ctx context.Context, topic string, key, value []byte, headers ...Header) error {
	rec := toKgoRecord(topic, key, value, headers)
	if err := p.client.ProduceSync(ctx, rec).FirstErr(); err != nil {
		return fmt.Errorf("kafka: produce to %q: %w", topic, err)
	}
	return nil
}

func (p *Producer) PublishBatch(ctx context.Context, recs []Record) []error {
	if len(recs) == 0 {
		return nil
	}
	krecs := make([]*kgo.Record, len(recs))
	for i, r := range recs {
		krecs[i] = toKgoRecord(r.Topic, r.Key, r.Value, r.Headers)
	}
	results := p.client.ProduceSync(ctx, krecs...)

	errByRec := make(map[*kgo.Record]error, len(results))
	for _, res := range results {
		errByRec[res.Record] = res.Err
	}
	errs := make([]error, len(recs))
	for i := range krecs {
		errs[i] = errByRec[krecs[i]]
	}
	return errs
}

func (p *Producer) Close() { p.client.Close() }
