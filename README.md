# Tagtweet

Project to search for tags of various tweets.

This is a training project on working with go-kafka. For this reason, real responses
are not sent and tags are randomly selected.

## Model

Analysis of tweets and tagging are presented in the following workflow:

1. kafka reader reads from _tweets_ topic
2. analysis worker gets a tweet
3. tags selection
4. kafka writer writes the tags to the _tweets-tags_ topic.

## Demo

Producer was written for the demonstration. It randomly sends random tweets
to kafka and accepts the selected tags.

To start demo:

```bash
make run
```

To end demo:

```text
press Ctrl+C.
```

To terminate resources:

```bash
make stop
```
