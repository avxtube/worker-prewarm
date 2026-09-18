package prewarm

import (
	"reflect"
	"testing"
)

func TestParseSegmentsAllowsOnlyPrewarmSegmentTypes(t *testing.T) {
	content := `#EXTM3U
video-1.ts
video-2.mp4?token=test
https://cdn.example/video-3.m4s
//cdn.example/video-4.jpeg#frame
video-5.jpg
video-6.png
https://cdn.example/file.exe
child.m3u8`
	want := []string{
		"video-1.ts",
		"video-2.mp4?token=test",
		"https://cdn.example/video-3.m4s",
		"//cdn.example/video-4.jpeg#frame",
	}
	if got := parseSegments(content, ".ts", ".mp4", ".m4s", ".jpeg"); !reflect.DeepEqual(got, want) {
		t.Fatalf("parseSegments() = %#v, want %#v", got, want)
	}
}

func TestParseAudioSegmentsAllowsEveryReferencedSegment(t *testing.T) {
	content := `#EXTM3U
audio-1.aac
audio-2.m4s?token=test
https://cdn.example/audio-3.bin
child.m3u8`
	want := []string{
		"audio-1.aac",
		"audio-2.m4s?token=test",
		"https://cdn.example/audio-3.bin",
	}
	if got := parseSegments(content); !reflect.DeepEqual(got, want) {
		t.Fatalf("parseSegments() = %#v, want %#v", got, want)
	}
}

func TestParseVTTImagesAllowsOnlySpriteImages(t *testing.T) {
	content := `WEBVTT

00:00.000 --> 00:05.000
sprite-001.jpg#xywh=0,0,160,90

00:05.000 --> 00:10.000
images/sprite-002.JPEG?version=1#xywh=0,0,160,90

00:10.000 --> 00:15.000
poster.jpg#xywh=0,0,160,90

00:15.000 --> 00:20.000
sprite-003.webp#xywh=0,0,160,90`
	want := []string{"sprite-001.jpg", "images/sprite-002.JPEG?version=1"}
	if got := parseVTTImages(content); !reflect.DeepEqual(got, want) {
		t.Fatalf("parseVTTImages() = %#v, want %#v", got, want)
	}
}
