// Package port defines ports to external systems.
package port

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/alukart32/tagtweet/internal/pkg/model"
	kafka "github.com/segmentio/kafka-go"
)

// kafkaTweetTagsReader represents  kafka tweet tags reader.
type kafkaTweetTagsReader struct {
	r     *kafka.Reader
	topic string
}

// KafkaTweetTagsReader returns a new kafka tweet tags reader.
func KafkaTweetTagsReader(topic string, brokers []string) (*kafkaTweetTagsReader, error) {
	if len(topic) == 0 {
		return nil, fmt.Errorf("empty topic")
	}
	if len(brokers) == 0 {
		return nil, fmt.Errorf("empty brokers list")
	}

	return &kafkaTweetTagsReader{
		topic: topic,
		r: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			Topic:    topic,
			MaxBytes: 1e6, // 1MB
		}),
	}, nil
}

// Fetch reads tweet tags from kafka topic.
func (r *kafkaTweetTagsReader) Fetch(ctx context.Context) chan model.TweetTags {
	out := make(chan model.TweetTags, 1)

	go func() {
		defer func() {
			close(out)
			if err := r.r.Close(); err != nil {
				log.Fatal("failed to close reader:", err)
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			m, err := r.r.ReadMessage(context.Background())
			if err != nil {
				log.Printf("unexpected error %v", err)
				break
			}

			var t model.TweetTags
			if _, err := t.UnmarshalMsg(m.Value); err != nil {
				log.Printf("can't unmarshal message at %v/%v/%v: %s\n: %v",
					m.Topic, m.Partition, m.Offset, string(m.Value), err)
			}
			log.Printf("message at topic/part/off %v/%v/%v:  %s\n",
				m.Topic, m.Partition, m.Offset, t)

			out <- t
		}
	}()
	return out
}

// kafkaTweetWriter represents kafka tweet writer.
type kafkaTweetWriter struct {
	w     *kafka.Writer
	q     chan model.Tweet
	done  chan struct{}
	topic string
}

// KafkaTweetWriter returns a new kafkaTweetWriter.
func KafkaTweetWriter(topic string, addrs []string, qs int) (*kafkaTweetWriter, error) {
	if len(topic) == 0 {
		return nil, fmt.Errorf("empty topic")
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("empty addrs list")
	}
	if qs < 0 {
		qs = 1
	}

	w := kafkaTweetWriter{
		topic: topic,
		q:     make(chan model.Tweet, qs),
		w: &kafka.Writer{
			Addr:                   kafka.TCP(addrs...),
			Topic:                  topic,
			Balancer:               &kafka.LeastBytes{},
			AllowAutoTopicCreation: false,
		},
	}
	go w.write()

	return &w, nil
}

// Write writes tweet to the kafka topic.
func (w *kafkaTweetWriter) Write(t model.Tweet) {
	w.q <- t
}

func (w *kafkaTweetWriter) write() {
	const retries = 3

	defer func() {
		if err := w.w.Close(); err != nil {
			log.Fatal("failed to close writer:", err)
		}
	}()

	for {
		select {
		case <-w.done:
			return
		case tweet, ok := <-w.q:
			if !ok {
				return
			}
			b, err := tweet.MarshalMsg(nil)
			if err != nil {
				log.Printf("can't marshal %v: %v", tweet.String(), err)
			}

			messages := []kafka.Message{
				{
					Value: b,
				},
			}
			for i := 0; i < retries; i++ {
				select {
				case <-w.done:
					return
				default:
				}

				writeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				err = w.w.WriteMessages(writeCtx, messages...)
				if errors.Is(err, kafka.LeaderNotAvailable) ||
					errors.Is(err, context.DeadlineExceeded) {
					log.Printf("unexpected timeout error %v", err)
					<-time.After(time.Millisecond * 250)
					continue
				}
				if err != nil {
					log.Printf("unexpected error %v", err)
				}
				break
			}
		}
	}
}

// Done stops writing to the kafka topic.
func (w *kafkaTweetWriter) Done() {
	w.done <- struct{}{}
}
