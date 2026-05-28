package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"instagram_clone/internal/model"

	"github.com/segmentio/kafka-go"
)

const (
	MediaUploadedTopic = "media-uploaded"
	StoryUploadedTopic = "story-uploaded"
)

type KafkaProducer struct {
	writer *kafka.Writer
}

func NewKafkaProducer(broker string) *KafkaProducer {
	return &KafkaProducer{
		writer: &kafka.Writer{
			Addr:         kafka.TCP(broker),
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireOne,
		},
	}
}

func (p *KafkaProducer) PublishMediaUploaded(ctx context.Context, mediaID, userID, s3Key, mediaType string) error {
	event := model.MediaUploadedEvent{
		MediaID:   mediaID,
		UserID:    userID,
		S3Key:     s3Key,
		MediaType: mediaType,
		CreatedAt: time.Now().UTC(),
	}
	return p.publish(ctx, MediaUploadedTopic, mediaID, event)
}

func (p *KafkaProducer) PublishStoryUploaded(ctx context.Context, storyID, userID, s3Key string) error {
	event := model.StoryUploadedEvent{
		StoryID:   storyID,
		UserID:    userID,
		S3Key:     s3Key,
		CreatedAt: time.Now().UTC(),
	}
	return p.publish(ctx, StoryUploadedTopic, storyID, event)
}

func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}

func (p *KafkaProducer) publish(ctx context.Context, topic, key string, payload any) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: data,
	})
	if err != nil {
		return fmt.Errorf("publish kafka message: %w", err)
	}
	return nil
}
