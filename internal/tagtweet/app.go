// Package tagtweet provides functionality for tagging tweets.
//
// In this implementation, there are no tweet tags defined randomly.
package tagtweet

import (
	"context"
	"flag"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/alukart32/tagtweet/internal/pkg/model"
	"github.com/alukart32/tagtweet/internal/tagtweet/port"
	env "github.com/caarlos0/env/v8"
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

// Run starts the tweet tag analyzer workflow.
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

	// Start the tweet analysis routine.
	appCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	workers := runtime.NumCPU()
	kafkaBrokers := strings.Split(cfg.Broker, ";")

	log.Printf("run kafka reader: [ topic: %v, brokers: %v ]", cfg.ReadTopic, cfg.Broker)
	tweetsReader, err := port.KafkaTweetReader(cfg.ReadTopic, kafkaBrokers)
	if err != nil {
		log.Panic(err)
	}

	log.Printf("run kafka writer: [ topic: %v, brokers: %v ]", cfg.WriteTopic, cfg.Broker)
	tagsWriter, err := port.KafkaTweetTagsWriter(cfg.WriteTopic, kafkaBrokers, workers)
	if err != nil {
		log.Panic(err)
	}

	// Prepare analysis workers.
	topic := tweetsReader.Fetch(appCtx)
	tweets := fanOut(topic, workers)
	tags := make([]chan model.TweetTags, 0, workers)
	for _, tweet := range tweets {
		out := analyze(appCtx, tweet)
		tags = append(tags, out)
	}

	// Send processed tags back.
	go func() {
		for t := range fanIn(tags...) {
			select {
			case <-appCtx.Done():
				return
			default:
			}
			tagsWriter.Write(t)
		}
	}()

	// Waiting signals.
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM, syscall.SIGQUIT)
	s := <-interrupt
	log.Println(s.String())
}

// analyze generates tags for a tweet.
func analyze(ctx context.Context, in chan model.Tweet) chan model.TweetTags {
	out := make(chan model.TweetTags, 1)

	go func() {
		defer close(out)
		rand.NewSource(time.Now().Unix())

		tags := []string{
			"movie_lovers", "sports_fans",
			"music_lovers", "fashion_lovers",
			"foodies", "gamers",
			"history_lovers", "eco_warriors",
			"outdoor_enthusiasts", "comic_lovers",
			"wine_connoisseurs", "home_decor_lovers",
			"sports_enthusiasts", "plant_lovers",
			"interior_design_lovers", "nature_aficionados",
			"DIY_crafts", "photography_fanatics",
			"outdoors_adventurers",
		}

		timer := time.NewTimer(1 * time.Second)
		defer func() {
			if !timer.Stop() {
				<-timer.C
			}
		}()

		for {
			select {
			case <-ctx.Done():
				return
			case t, ok := <-in:
				if !ok {
					return
				}
				select {
				case <-ctx.Done():
					return
				case <-timer.C:
					tag := tags[rand.Intn(len(tags))]
					out <- model.TweetTags{
						TweetID: t.ID,
						Tags:    []string{tag},
					}
				}
			}
			_ = timer.Reset(time.Duration(100+rand.Intn(900)) * time.Millisecond)
		}
	}()
	return out
}
