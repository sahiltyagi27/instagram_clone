package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"time"

	"instagram_clone/internal/model"
	"instagram_clone/internal/service"
	"instagram_clone/internal/telemetry"

	"github.com/segmentio/kafka-go"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var consumerTracer = otel.Tracer("instagram_clone/kafka/consumer")

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
			slog.Error("read media kafka message", "error", err)
			telemetry.KafkaMessagesConsumed.WithLabelValues(MediaUploadedTopic, "error").Inc()
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}

		// Extract the producer's trace context from headers to link spans.
		carrier := telemetry.KafkaHeaderCarrier(msg.Headers)
		msgCtx := otel.GetTextMapPropagator().Extract(ctx, &carrier)
		msgCtx, span := consumerTracer.Start(msgCtx, "kafka.consume "+MediaUploadedTopic)
		span.SetAttributes(attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.source", MediaUploadedTopic))

		var event model.MediaUploadedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			slog.Error("decode media uploaded event", "error", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.End()
			telemetry.KafkaMessagesConsumed.WithLabelValues(MediaUploadedTopic, "error").Inc()
			continue
		}

		span.SetAttributes(attribute.String("media.id", event.MediaID),
			attribute.String("user.id", event.UserID))

		if err := c.processor.Process(msgCtx, event.S3Key, event.MediaType); err != nil {
			slog.Error("process media", "media_id", event.MediaID, "error", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.End()
			telemetry.KafkaMessagesConsumed.WithLabelValues(MediaUploadedTopic, "error").Inc()
			continue
		}

		c.feed.AddFeedItem(event.UserID, model.FeedItem{
			MediaID:      event.MediaID,
			UserID:       event.UserID,
			S3Key:        event.S3Key,
			ThumbnailKey: event.S3Key + "/thumb",
			CreatedAt:    event.CreatedAt,
		})

		slog.Info("media event processed", "media_id", event.MediaID, "user_id", event.UserID)
		telemetry.KafkaMessagesConsumed.WithLabelValues(MediaUploadedTopic, "ok").Inc()
		span.End()
	}
}

func (c *KafkaConsumer) consumeStories(ctx context.Context) {
	for {
		msg, err := c.storyReader.ReadMessage(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			slog.Error("read story kafka message", "error", err)
			telemetry.KafkaMessagesConsumed.WithLabelValues(StoryUploadedTopic, "error").Inc()
			select {
			case <-ctx.Done():
				return
			case <-time.After(2 * time.Second):
			}
			continue
		}

		carrier := telemetry.KafkaHeaderCarrier(msg.Headers)
		msgCtx := otel.GetTextMapPropagator().Extract(ctx, &carrier)
		msgCtx, span := consumerTracer.Start(msgCtx, "kafka.consume "+StoryUploadedTopic)
		span.SetAttributes(attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.source", StoryUploadedTopic))

		var event model.StoryUploadedEvent
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			slog.Error("decode story uploaded event", "error", err)
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			span.End()
			telemetry.KafkaMessagesConsumed.WithLabelValues(StoryUploadedTopic, "error").Inc()
			continue
		}

		span.SetAttributes(attribute.String("story.id", event.StoryID),
			attribute.String("user.id", event.UserID))

		// TODO: Replace with story-side processing (fanout, notifications, analytics).
		slog.Info("story uploaded event consumed", "story_id", event.StoryID, "user_id", event.UserID, "s3_key", event.S3Key)
		telemetry.KafkaMessagesConsumed.WithLabelValues(StoryUploadedTopic, "ok").Inc()
		span.End()

		// suppress unused variable warning for msgCtx until processing is added
		_ = msgCtx
	}
}

func (c *KafkaConsumer) Close() error {
	mediaErr := c.mediaReader.Close()
	storyErr := c.storyReader.Close()
	return errors.Join(mediaErr, storyErr)
}
