package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"instagram_clone/internal/model"
	"instagram_clone/internal/telemetry"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

const (
	MediaUploadedTopic = "media-uploaded"
	StoryUploadedTopic = "story-uploaded"
)

var tracer = otel.Tracer("instagram_clone/kafka/producer")

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
	ctx, span := tracer.Start(ctx, "kafka.publish "+topic)
	defer span.End()
	span.SetAttributes(
		attribute.String("messaging.system", "kafka"),
		attribute.String("messaging.destination", topic),
		attribute.String("messaging.message_id", key),
	)

	data, err := json.Marshal(payload)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("marshal event: %w", err)
	}

	// Inject trace context into message headers so consumers can link spans.
	headers := make(telemetry.KafkaHeaderCarrier, 0)
	otel.GetTextMapPropagator().Inject(ctx, &headers)

	err = p.writer.WriteMessages(ctx, kafka.Message{
		Topic:   topic,
		Key:     []byte(key),
		Value:   data,
		Headers: []kafka.Header(headers),
	})
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return fmt.Errorf("publish kafka message: %w", err)
	}
	return nil
}
