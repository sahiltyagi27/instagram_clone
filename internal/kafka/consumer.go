package kafka

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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

const maxProcessRetries = 3

type KafkaConsumer struct {
	mediaReader *kafka.Reader
	storyReader *kafka.Reader
	dlqWriter   *kafka.Writer
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
			CommitInterval: 0, // manual commit — offset advances only after processing succeeds
		}),
		storyReader: kafka.NewReader(kafka.ReaderConfig{
			Brokers:        []string{broker},
			Topic:          StoryUploadedTopic,
			GroupID:        "instagram-story-events",
			MinBytes:       1,
			MaxBytes:       10e6,
			CommitInterval: 0, // manual commit
		}),
		dlqWriter: &kafka.Writer{
			Addr:         kafka.TCP(broker),
			Balancer:     &kafka.LeastBytes{},
			RequiredAcks: kafka.RequireOne,
		},
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
		msg, err := c.mediaReader.FetchMessage(ctx)
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
			// Malformed message — no point retrying; route to DLQ and commit only if DLQ succeeds.
			if dlqErr := c.writeToDLQ(MediaUploadedTopic, msg, err); dlqErr != nil {
				slog.Error("DLQ unavailable, leaving offset uncommitted", "topic", MediaUploadedTopic, "error", dlqErr)
				continue
			}
			_ = c.mediaReader.CommitMessages(ctx, msg)
			continue
		}

		span.SetAttributes(attribute.String("media.id", event.MediaID),
			attribute.String("user.id", event.UserID))

		// Process the media and index the feed item as a single atomic unit so a
		// Redis failure after a successful S3 transcode is retried and caught.
		feedItem := model.FeedItem{
			MediaID:      event.MediaID,
			UserID:       event.UserID,
			S3Key:        event.S3Key,
			ThumbnailKey: event.S3Key + "/thumb",
			CreatedAt:    event.CreatedAt,
		}
		processErr := retryWithBackoff(msgCtx, maxProcessRetries, func() error {
			if err := c.processor.Process(msgCtx, event.S3Key, event.MediaType); err != nil {
				return fmt.Errorf("process media: %w", err)
			}
			if err := c.feed.AddFeedItem(msgCtx, event.UserID, feedItem); err != nil {
				return fmt.Errorf("index feed item: %w", err)
			}
			return nil
		})
		if processErr != nil {
			slog.Error("media processing failed: retries exhausted", "media_id", event.MediaID, "error", processErr)
			span.RecordError(processErr)
			span.SetStatus(codes.Error, processErr.Error())
			span.End()
			telemetry.KafkaMessagesConsumed.WithLabelValues(MediaUploadedTopic, "error").Inc()
			if dlqErr := c.writeToDLQ(MediaUploadedTopic, msg, processErr); dlqErr != nil {
				slog.Error("DLQ unavailable, leaving offset uncommitted", "topic", MediaUploadedTopic, "error", dlqErr)
				continue
			}
			_ = c.mediaReader.CommitMessages(ctx, msg)
			continue
		}

		slog.Info("media event processed", "media_id", event.MediaID, "user_id", event.UserID)
		telemetry.KafkaMessagesConsumed.WithLabelValues(MediaUploadedTopic, "ok").Inc()
		span.End()
		_ = c.mediaReader.CommitMessages(ctx, msg)
	}
}

func (c *KafkaConsumer) consumeStories(ctx context.Context) {
	for {
		msg, err := c.storyReader.FetchMessage(ctx)
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
			if dlqErr := c.writeToDLQ(StoryUploadedTopic, msg, err); dlqErr != nil {
				slog.Error("DLQ unavailable, leaving offset uncommitted", "topic", StoryUploadedTopic, "error", dlqErr)
				continue
			}
			_ = c.storyReader.CommitMessages(ctx, msg)
			continue
		}

		span.SetAttributes(attribute.String("story.id", event.StoryID),
			attribute.String("user.id", event.UserID))

		slog.Info("story uploaded event consumed", "story_id", event.StoryID, "user_id", event.UserID, "s3_key", event.S3Key)
		telemetry.KafkaMessagesConsumed.WithLabelValues(StoryUploadedTopic, "ok").Inc()
		span.End()
		_ = c.storyReader.CommitMessages(ctx, msg)
	}
}

func (c *KafkaConsumer) Close() error {
	mediaErr := c.mediaReader.Close()
	storyErr := c.storyReader.Close()
	dlqErr := c.dlqWriter.Close()
	return errors.Join(mediaErr, storyErr, dlqErr)
}

// writeToDLQ publishes a failed message to the dead-letter topic for the given
// source topic. Uses a background context so a cancelled parent context (e.g. on
// shutdown) does not prevent the DLQ write from completing.
//
// Returns an error if the DLQ write itself fails. Callers must NOT commit the
// source offset when this returns an error — leaving it uncommitted means the
// message will be replayed on the next consumer restart.
func (c *KafkaConsumer) writeToDLQ(topic string, msg kafka.Message, cause error) error {
	dlqTopic := topic + "-dlq"
	dlqMsg := kafka.Message{
		Topic:   dlqTopic,
		Key:     msg.Key,
		Value:   msg.Value,
		Headers: append(msg.Headers, kafka.Header{Key: "dlq-error", Value: []byte(cause.Error())}),
	}
	if err := c.dlqWriter.WriteMessages(context.Background(), dlqMsg); err != nil {
		return fmt.Errorf("write to DLQ %q: %w", dlqTopic, err)
	}
	slog.Warn("message routed to DLQ", "topic", dlqTopic, "key", string(msg.Key))
	return nil
}

// retryWithBackoff calls op up to maxRetries times. On each failure after the
// first it waits 1s, 2s, 4s, … before trying again. Returns the last error if
// all attempts fail, or nil on the first success.
func retryWithBackoff(ctx context.Context, maxRetries int, op func() error) error {
	var err error
	for attempt := range maxRetries {
		if attempt > 0 {
			delay := time.Duration(1<<uint(attempt-1)) * time.Second
			slog.Warn("retrying kafka message processing", "attempt", attempt+1, "max", maxRetries, "error", err)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(delay):
			}
		}
		if err = op(); err == nil {
			return nil
		}
	}
	return err
}
