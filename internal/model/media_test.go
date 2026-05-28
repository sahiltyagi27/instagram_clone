package model

import "testing"

func TestMediaTypeValid(t *testing.T) {
	tests := []struct {
		name      string
		mediaType MediaType
		want      bool
	}{
		{name: "photo is valid", mediaType: MediaTypePhoto, want: true},
		{name: "video is valid", mediaType: MediaTypeVideo, want: true},
		{name: "empty is invalid", mediaType: "", want: false},
		{name: "unknown is invalid", mediaType: "story", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.mediaType.Valid(); got != tt.want {
				t.Fatalf("Valid() = %v, want %v", got, tt.want)
			}
		})
	}
}
