package prewarm

import (
	"errors"
	"testing"

	"worker-prewarm/internal/core/enums"
	"worker-prewarm/internal/db/models"
	"worker-prewarm/internal/queue"
)

func TestFailedPercent(t *testing.T) {
	tests := []struct {
		name  string
		stats WarmStats
		want  float64
	}{
		{name: "empty", stats: WarmStats{}, want: 0},
		{name: "half", stats: WarmStats{Total: 10, Failed: 5}, want: 50},
		{name: "over half", stats: WarmStats{Total: 10, Failed: 6}, want: 60},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := failedPercent(tt.stats); got != tt.want {
				t.Fatalf("failedPercent() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCountPayloadURLsExcludesManifests(t *testing.T) {
	urls := []string{
		"https://cdn.example/video.m3u8",
		"https://cdn.example/child.m3u8?token=1",
		"https://cdn.example/video0.jpeg",
		"https://cdn.example/video1.ts?token=1",
		"https://cdn.example/sprite.vtt",
		"https://cdn.example/sprite-0.jpg",
	}
	if got := countPayloadURLs(urls); got != 3 {
		t.Fatalf("countPayloadURLs() = %d, want 3", got)
	}
}

func TestPersistentPlaylistFailureDropsOrphansWithoutOpeningStorageCircuit(t *testing.T) {
	err := classifyPersistentPlaylistFailure("playlist unavailable", false, nil)
	if errors.Is(err, queue.ErrStorageFailure) || errors.Is(err, queue.ErrJobRequeue) {
		t.Fatalf("orphan error must be terminal, got %v", err)
	}

	err = classifyPersistentPlaylistFailure("playlist unavailable", true, nil)
	if !errors.Is(err, queue.ErrStorageFailure) {
		t.Fatalf("playable source must report storage failure, got %v", err)
	}

	verifyErr := errors.New("database unavailable")
	err = classifyPersistentPlaylistFailure("playlist unavailable", false, verifyErr)
	if !errors.Is(err, queue.ErrJobRequeue) {
		t.Fatalf("verification failure must requeue, got %v", err)
	}
}

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
