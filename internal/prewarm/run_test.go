package prewarm

import (
	"testing"

	"worker-prewarm/internal/core/enums"
	"worker-prewarm/internal/db/models"
)

func TestPrewarmAttempt(t *testing.T) {
	if got := prewarmAttempt(&models.PrewarmQueue{}); got != 1 {
		t.Fatalf("first attempt = %d, want 1", got)
	}
	retries := 2
	if got := prewarmAttempt(&models.PrewarmQueue{RetryCount: &retries}); got != 3 {
		t.Fatalf("attempt after two retries = %d, want 3", got)
	}
}

func TestPlaylistNameForMediaType(t *testing.T) {
	tests := []struct {
		mediaType string
		want      string
	}{
		{mediaType: enums.MediaTypeVideo, want: "video.m3u8"},
		{mediaType: enums.MediaTypeAudio, want: "audio.m3u8"},
	}

	for _, tt := range tests {
		got, err := playlistNameForMediaType(tt.mediaType)
		if err != nil {
			t.Fatalf("playlistNameForMediaType(%q): %v", tt.mediaType, err)
		}
		if got != tt.want {
			t.Fatalf("playlistNameForMediaType(%q) = %q, want %q", tt.mediaType, got, tt.want)
		}
	}

	if _, err := playlistNameForMediaType(enums.MediaTypeSubtitle); err == nil {
		t.Fatal("playlistNameForMediaType(subtitle) should reject unsupported media")
	}
}
