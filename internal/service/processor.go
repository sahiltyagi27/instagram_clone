package service

import (
	"bytes"
	"context"
	"fmt"
	"image"
	_ "image/gif"
	_ "image/jpeg"
	_ "image/png"
	"log"
	"strings"

	"github.com/disintegration/imaging"
)

type MediaProcessor struct {
	storage *Storage
}

func NewMediaProcessor(storage *Storage) *MediaProcessor {
	return &MediaProcessor{storage: storage}
}

func (p *MediaProcessor) Process(ctx context.Context, s3Key, mediaType string) error {
	switch strings.ToLower(mediaType) {
	case "photo":
		return p.processPhoto(ctx, s3Key)
	case "video":
		log.Printf("video transcoding queued for %s", s3Key)
		return nil
	default:
		return fmt.Errorf("unsupported media type: %s", mediaType)
	}
}

func (p *MediaProcessor) processPhoto(ctx context.Context, s3Key string) error {
	data, contentType, err := p.storage.GetObject(ctx, s3Key)
	if err != nil {
		return err
	}

	img, _, err := image.Decode(bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("decode image: %w", err)
	}

	thumb := imaging.Fill(img, 150, 150, imaging.Center, imaging.Lanczos)
	medium := imaging.Fit(img, 640, 640, imaging.Lanczos)

	if err := p.uploadJPEG(ctx, s3Key+"/thumb", contentType, thumb); err != nil {
		return err
	}
	if err := p.uploadJPEG(ctx, s3Key+"/medium", contentType, medium); err != nil {
		return err
	}
	if err := p.storage.PutObject(ctx, s3Key+"/original", contentType, data); err != nil {
		return err
	}
	return nil
}

func (p *MediaProcessor) uploadJPEG(ctx context.Context, key, contentType string, img image.Image) error {
	var buf bytes.Buffer
	if err := imaging.Encode(&buf, img, imaging.JPEG); err != nil {
		return fmt.Errorf("encode image: %w", err)
	}
	if contentType == "" {
		contentType = "image/jpeg"
	}
	return p.storage.PutObject(ctx, key, contentType, buf.Bytes())
}
