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

// kafkaTweetReader represents kafka tweet reader.
type kafkaTweetReader struct {
	r     *kafka.Reader
	topic string
}

// KafkaTweetReader returns a new kafka tweet reader.
func KafkaTweetReader(topic string, brokers []string) (*kafkaTweetReader, error) {
	if len(topic) == 0 {
		return nil, fmt.Errorf("empty topic")
	}
	if len(brokers) == 0 {
		return nil, fmt.Errorf("empty brokers list")
	}

	return &kafkaTweetReader{
		topic: topic,
		r: kafka.NewReader(kafka.ReaderConfig{
			Brokers:  brokers,
			Topic:    topic,
			MaxBytes: 1e6, // 1MB
		}),
	}, nil
}

// Fetch reads tweets from kafka topic.
func (r *kafkaTweetReader) Fetch(ctx context.Context) chan model.Tweet {
	out := make(chan model.Tweet, 1)

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

			var t model.Tweet
			if _, err := t.UnmarshalMsg(m.Value); err != nil {
				log.Printf("can't unmarshal message at %v/%v/%v: %s\n: %v",
					m.Topic, m.Partition, m.Offset, string(m.Value), err)
			}
			log.Printf("message at topic/part/off %v/%v/%v: %s\n",
				m.Topic, m.Partition, m.Offset, t)

			out <- t
		}
	}()
	return out
}

// kafkaTweetTagsWriter represents kafka tweet tags writer.
type kafkaTweetTagsWriter struct {
	w     *kafka.Writer
	in    chan model.TweetTags
	done  chan struct{}
	topic string
}

// KafkaTweetTagsWriter returns a new kafka tweet tags writer.
func KafkaTweetTagsWriter(topic string, addrs []string, qs int) (*kafkaTweetTagsWriter, error) {
	if len(topic) == 0 {
		return nil, fmt.Errorf("empty topic")
	}
	if len(addrs) == 0 {
		return nil, fmt.Errorf("empty addrs list")
	}
	if qs < 0 {
		qs = 1
	}

	w := kafkaTweetTagsWriter{
		topic: topic,
		in:    make(chan model.TweetTags, qs),
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

// Write writes tweet tags to the kafka topic.
func (w *kafkaTweetTagsWriter) Write(t model.TweetTags) {
	w.in <- t
}

func (w *kafkaTweetTagsWriter) write() {
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
		case tags, ok := <-w.in:
			if !ok {
				return
			}
			b, err := tags.MarshalMsg(nil)
			if err != nil {
				log.Printf("can't marshal %v: %v", tags.String(), err)
			}

			messages := []kafka.Message{
				{
					Value: b,
				},
			}
			// try to write a message.
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
					log.Printf("unexpected error %v", err)
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
func (w *kafkaTweetTagsWriter) Done() {
	w.done <- struct{}{}
}
