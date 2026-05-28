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
	reader    *kafka.Reader
	processor *service.MediaProcessor
	feed      *service.FeedService
}

func NewKafkaConsumer(broker string, processor *service.MediaProcessor, feed *service.FeedService) *KafkaConsumer {
	return &KafkaConsumer{
		reader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        []string{broker},
			Topic:          MediaUploadedTopic,
			GroupID:        "instagram-media-processor",
			MinBytes:       1,
			MaxBytes:       10e6,
			CommitInterval: time.Second,
		}),
		processor: processor,
		feed:      feed,
	}
}

func (c *KafkaConsumer) Start(ctx context.Context) {
	for {
		msg, err := c.reader.ReadMessage(ctx)
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
			ThumbnailURL: event.S3Key + "/thumb",
			CreatedAt:    event.CreatedAt,
		})
	}
}

func (c *KafkaConsumer) Close() error {
	return c.reader.Close()
}
