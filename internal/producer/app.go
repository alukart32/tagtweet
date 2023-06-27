package producer

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/alukart32/tagtweet/internal/pkg/model"
	"github.com/alukart32/tagtweet/internal/producer/port"
	env "github.com/caarlos0/env/v8"
	"github.com/google/uuid"
)

// config defines the configuration of tagtweet application.
type config struct {
	ReadTopic  string `env:"KAFKA_READ_TOPIC,notEmpty"`
	WriteTopic string `env:"KAFKA_WRITE_TOPIC,notEmpty"`
	Broker     string `env:"KAFKA_BROKERS,notEmpty"`
}

// Empty checks for empty values.
func (c *config) Empty() bool {
	return len(c.ReadTopic) == 0 &&
		len(c.WriteTopic) == 0 &&
		len(c.Broker) == 0
}

// Run starts the tweet generation workflow.
func Run() {
	// Parse config.
	cfg := config{}

	flag.StringVar(&cfg.ReadTopic, "read-topic", "", "Kafka topic for analyzing tweets")
	flag.StringVar(&cfg.WriteTopic, "write-topic", "", "Kafka processed tweet tags topic")
	flag.StringVar(&cfg.Broker, "kafka-broker", "", "Kafka broker address")
	flag.Parse()

	if cfg.Empty() {
		if err := env.Parse(&cfg); err != nil {
			log.Panic(err)
		}
	}

	// Start the tweet creation routine.
	appCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	produceTweets(appCtx, &cfg)

	// Waiting signals.
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	s := <-interrupt
	log.Println(s.String())
}

// produceTweets starts the tweet creation routine.
func produceTweets(ctx context.Context, cfg *config) {
	kafkaBrokers := strings.Split(cfg.Broker, ";")

	log.Printf("run kafka reader: [ topic: %v, brokers: %v ]", cfg.ReadTopic, cfg.Broker)
	tagsReader, err := port.KafkaTweetTagsReader(cfg.ReadTopic, kafkaBrokers)
	if err != nil {
		log.Panic(err)
	}

	log.Printf("run kafka writer: [ topic: %v, brokers: %v ]", cfg.WriteTopic, cfg.Broker)
	tweetsWriter, err := port.KafkaTweetWriter(cfg.WriteTopic, kafkaBrokers, 1)
	if err != nil {
		log.Panic(err)
	}

	// Tweets generator
	tweets := make(chan model.Tweet, 1)
	go func() {
		defer func() {
			close(tweets)
		}()
		rand.NewSource(time.Now().Unix())

		t := time.NewTimer(1 * time.Second)
		defer func() {
			if !t.Stop() {
				<-t.C
			}
		}()

		msgs := []string{
			"car", "geeks", "movie",
			"nature", "fashion", "food",
			"travel", "comic", "DIY",
			"wine", "plant", "interior",
			"design"}

		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				msg := msgs[rand.Intn(len(msgs))]
				tweets <- model.Tweet{
					ID:   uuid.NewString(),
					Body: []byte(msg),
				}
			}
			_ = t.Reset(time.Duration(100+rand.Intn(900)) * time.Millisecond)
		}
	}()

	// Write tweets.
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case tweet, ok := <-tweets:
				if !ok {
					return
				}
				tweetsWriter.Write(tweet)
			}
		}
	}()

	// Read tweet tags.
	go func() {
		for range tagsReader.Fetch(ctx) {
		}
	}()
}
