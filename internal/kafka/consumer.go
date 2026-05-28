package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"time"

	"instagram_clone/internal/model"
	"instagram_clone/internal/service"

	"github.com/segmentio/kafka-go"
)

type KafkaConsumer struct {
	mediaReader *kafka.Reader
	storyReader *kafka.Reader
	processor   *service.MediaProcessor
	feed        *service.FeedService
}

func NewKafkaConsumer(broker string, processor *service.MediaProcessor, feed *service.FeedService) *KafkaConsumer {
	return &KafkaConsumer{
		mediaReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        []string{broker},
			Topic:          MediaUploadedTopic,
			GroupID:        "instagram-media-processor",
			MinBytes:       1,
			MaxBytes:       10e6,
			CommitInterval: time.Second,
		}),
		storyReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        []string{broker},
			Topic:          StoryUploadedTopic,
			GroupID:        "instagram-story-events",
			MinBytes:       1,
			MaxBytes:       10e6,
			CommitInterval: time.Second,
		}),
		processor: processor,
		feed:      feed,
	}
}

func (c *KafkaConsumer) Start(ctx context.Context) {
	go c.consumeStories(ctx)
	c.consumeMedia(ctx)
}

func (c *KafkaConsumer) consumeMedia(ctx context.Context) {
	for {
		msg, err := c.mediaReader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("read kafka message: %v", err)
			continue
		}

		var event model.MediaUploadedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("decode media uploaded event: %v", err)
			continue
		}

		if err := c.processor.Process(ctx, event.S3Key, event.MediaType); err != nil {
			log.Printf("process media %s: %v", event.MediaID, err)
			continue
		}

		c.feed.AddFeedItem(event.UserID, model.FeedItem{
			MediaID:      event.MediaID,
			UserID:       event.UserID,
			S3Key:        event.S3Key,
			ThumbnailKey: event.S3Key + "/thumb",
			CreatedAt:    event.CreatedAt,
		})
	}
}

func (c *KafkaConsumer) consumeStories(ctx context.Context) {
	for {
		msg, err := c.storyReader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Printf("read story kafka message: %v", err)
			continue
		}

		var event model.StoryUploadedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("decode story uploaded event: %v", err)
			continue
		}
		// TODO: Replace this acknowledgement log with story-side processing when
		// story fanout, notifications, or analytics are added.
		log.Printf("story uploaded event consumed: story_id=%s user_id=%s s3_key=%s", event.StoryID, event.UserID, event.S3Key)
	}
}

func (c *KafkaConsumer) Close() error {
	mediaErr := c.mediaReader.Close()
	storyErr := c.storyReader.Close()
	return errors.Join(mediaErr, storyErr)
}
