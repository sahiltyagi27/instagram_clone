package service

import (
	"bytes"
	"context"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"log/slog"
	"strings"

	"github.com/disintegration/imaging"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
)

var processorTracer = otel.Tracer("instagram_clone/service/processor")

type MediaProcessor struct {
	storage *Storage
}

func NewMediaProcessor(storage *Storage) *MediaProcessor {
	return &MediaProcessor{storage: storage}
}

func (p *MediaProcessor) Process(ctx context.Context, s3Key, mediaType string) error {
	ctx, span := processorTracer.Start(ctx, "processor.Process")
	defer span.End()
	span.SetAttributes(
		attribute.String("media.s3_key", s3Key),
		attribute.String("media.type", mediaType),
	)

	switch strings.ToLower(mediaType) {
	case "photo":
		if err := p.processPhoto(ctx, s3Key); err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
			return err
		}
		return nil
	case "video":
		slog.Info("video transcoding queued", "s3_key", s3Key)
		return nil
	default:
		err := fmt.Errorf("unsupported media type: %s", mediaType)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
}

func (p *MediaProcessor) processPhoto(ctx context.Context, s3Key string) error {
	ctx, span := processorTracer.Start(ctx, "processor.processPhoto")
	defer span.End()

	data, contentType, err := p.storage.GetObject(ctx, s3Key)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		err = fmt.Errorf("decode image: %w", err)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}

	thumb := imaging.Fill(img, 150, 150, imaging.Center, imaging.Lanczos)
	medium := imaging.Fit(img, 640, 640, imaging.Lanczos)

	if err := p.uploadJPEG(ctx, s3Key+"/thumb", thumb); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if err := p.uploadJPEG(ctx, s3Key+"/medium", medium); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	if err := p.storage.PutObject(ctx, s3Key+"/original", contentType, data); err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
		return err
	}
	return nil
}

func (p *MediaProcessor) uploadJPEG(ctx context.Context, key string, img image.Image) error {
	var buf bytes.Buffer
	if err := imaging.Encode(&buf, img, imaging.JPEG); err != nil {
		return fmt.Errorf("encode image: %w", err)
	}
	return p.storage.PutObject(ctx, key, "image/jpeg", buf.Bytes())
}
