package main

import "testing"

func TestWantsVersion(t *testing.T) {
	for _, args := range [][]string{{"--version"}, {"-version"}, {"version"}} {
		if !wantsVersion(args) {
			t.Fatalf("wantsVersion(%q) = false", args)
		}
	}

	for _, args := range [][]string{nil, {"--help"}, {"--version", "extra"}} {
		if wantsVersion(args) {
			t.Fatalf("wantsVersion(%q) = true", args)
		}
	}
}
