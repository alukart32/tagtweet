//go:generate msgp
package model

import "fmt"

// Tweet represents twitter tweet.
type Tweet struct {
	ID   string
	Body []byte
}

// String returns string form of tweet.
func (t Tweet) String() string {
	return fmt.Sprintf("[id: %v, body: %v]", t.ID, string(t.Body))
}

// TweetTags represents twitter tweet tags.
type TweetTags struct {
	TweetID string
	Tags    []string
}

// String returns string form of tweet tags.
func (t TweetTags) String() string {
	return fmt.Sprintf("[tweet_id: %v, tags: %v]", t.TweetID, t.Tags)
}
