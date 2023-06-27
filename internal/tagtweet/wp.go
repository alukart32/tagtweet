package tagtweet

import (
	"sync"

	"github.com/alukart32/tagtweet/internal/pkg/model"
)

// fanOut multiplexes a channel into n channels. The values are sent to each channel
// in round-robin order.
func fanOut(in chan model.Tweet, n int) []chan model.Tweet {
	chs := make([]chan model.Tweet, n)
	for i := 0; i < n; i++ {
		ch := make(chan model.Tweet, 1)
		chs[i] = ch
	}

	go func() {
		defer func(chs []chan model.Tweet) {
			for _, ch := range chs {
				close(ch)
			}
		}(chs)

		for i := 0; ; i++ {
			if i == len(chs) {
				i = 0
			}

			tweet, ok := <-in
			if !ok {
				return
			}

			chs[i] <- tweet
		}
	}()

	return chs
}

// fanIn aggregates data from multiple channels into one.
func fanIn(in ...chan model.TweetTags) chan model.TweetTags {
	out := make(chan model.TweetTags)

	go func() {
		defer close(out)

		var wg sync.WaitGroup
		for _, ch := range in {
			wg.Add(1)
			go func(in chan model.TweetTags) {
				defer wg.Done()
				for t := range in {
					out <- t
				}
			}(ch)
		}
		wg.Wait()
	}()

	return out
}
