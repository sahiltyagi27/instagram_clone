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

const (
	maxProcessRetries = 3
	dlqRetryInterval  = 2 * time.Second
)

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
			// Malformed message — no point retrying; route to DLQ then commit.
			if !c.dlqThenCommit(ctx, c.mediaReader, MediaUploadedTopic, msg, err) {
				return // shutdown before DLQ succeeded; offset left uncommitted
			}
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
			// Shutdown is not a processing failure: leave the offset uncommitted so
			// the still-valid message is replayed on restart rather than dead-lettered.
			if ctx.Err() != nil {
				span.End()
				return
			}
			slog.Error("media processing failed: retries exhausted", "media_id", event.MediaID, "error", processErr)
			span.RecordError(processErr)
			span.SetStatus(codes.Error, processErr.Error())
			span.End()
			telemetry.KafkaMessagesConsumed.WithLabelValues(MediaUploadedTopic, "error").Inc()
			if !c.dlqThenCommit(ctx, c.mediaReader, MediaUploadedTopic, msg, processErr) {
				return
			}
			continue
		}

		slog.Info("media event processed", "media_id", event.MediaID, "user_id", event.UserID)
		telemetry.KafkaMessagesConsumed.WithLabelValues(MediaUploadedTopic, "ok").Inc()
		span.End()
		c.commit(ctx, c.mediaReader, MediaUploadedTopic, msg)
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
			if !c.dlqThenCommit(ctx, c.storyReader, StoryUploadedTopic, msg, err) {
				return
			}
			continue
		}

		span.SetAttributes(attribute.String("story.id", event.StoryID),
			attribute.String("user.id", event.UserID))

		slog.Info("story uploaded event consumed", "story_id", event.StoryID, "user_id", event.UserID, "s3_key", event.S3Key)
		telemetry.KafkaMessagesConsumed.WithLabelValues(StoryUploadedTopic, "ok").Inc()
		span.End()
		c.commit(ctx, c.storyReader, StoryUploadedTopic, msg)
	}
}

func (c *KafkaConsumer) Close() error {
	mediaErr := c.mediaReader.Close()
	storyErr := c.storyReader.Close()
	dlqErr := c.dlqWriter.Close()
	return errors.Join(mediaErr, storyErr, dlqErr)
}

// dlqThenCommit routes msg to its dead-letter topic and then commits the source
// offset. The DLQ write is retried with a fixed interval until it succeeds,
// because committing the source offset past an un-dead-lettered message would
// lose it: a Kafka commit advances the partition's high-water mark, so any
// later successful commit would implicitly commit this offset too. Blocking
// here keeps the consumer from advancing past a message it could not persist.
//
// Returns false if ctx is cancelled before the DLQ write succeeds, in which
// case the offset is left uncommitted and the message is replayed on restart.
func (c *KafkaConsumer) dlqThenCommit(ctx context.Context, reader *kafka.Reader, topic string, msg kafka.Message, cause error) bool {
	for {
		if err := c.writeToDLQ(topic, msg, cause); err != nil {
			slog.Error("DLQ write failed; not advancing past this offset", "topic", topic, "offset", msg.Offset, "error", err)
			telemetry.KafkaMessagesConsumed.WithLabelValues(topic, "dlq_error").Inc()
			select {
			case <-ctx.Done():
				slog.Warn("shutdown before DLQ write succeeded; offset left uncommitted for replay", "topic", topic, "offset", msg.Offset)
				return false
			case <-time.After(dlqRetryInterval):
				continue
			}
		}
		c.commit(ctx, reader, topic, msg)
		return true
	}
}

// commit advances the consumer offset for msg, logging and counting any failure.
// A failed commit causes at-most a duplicate (re-processed) message on restart,
// never a lost one, so it is safe to log and move on.
func (c *KafkaConsumer) commit(ctx context.Context, reader *kafka.Reader, topic string, msg kafka.Message) {
	if err := reader.CommitMessages(ctx, msg); err != nil {
		slog.Error("commit kafka offset", "topic", topic, "offset", msg.Offset, "error", err)
		telemetry.KafkaMessagesConsumed.WithLabelValues(topic, "commit_error").Inc()
	}
}

// writeToDLQ publishes a failed message to the dead-letter topic for the given
// source topic. Uses a background context so a cancelled parent context (e.g. on
// shutdown) does not interrupt an in-flight DLQ write. Returns an error if the
// write fails so the caller can decide whether to commit the source offset.
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
// all attempts fail, or nil on the first success. A cancelled ctx aborts early
// and returns ctx.Err().
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
