package queue

import (
	"testing"
	"time"
)

func TestStorageCircuitOpensAfterFiveFailuresWithinWindow(t *testing.T) {
	breaker := newStorageCircuitBreaker()
	start := time.Date(2026, 9, 18, 8, 0, 0, 0, time.UTC)
	for i := 0; i < storageFailureThreshold; i++ {
		now := start.Add(time.Duration(i) * time.Minute)
		lease := breaker.begin("storage-a", now)
		if !lease.allowed || lease.probe {
			t.Fatalf("failure %d received unexpected lease: %+v", i+1, lease)
		}
		openUntil := breaker.finish(lease, circuitFailure, now)
		if i < storageFailureThreshold-1 && !openUntil.IsZero() {
			t.Fatalf("circuit opened after only %d failures", i+1)
		}
		if i == storageFailureThreshold-1 && !openUntil.Equal(now.Add(storageCooldown)) {
			t.Fatalf("openUntil = %v, want %v", openUntil, now.Add(storageCooldown))
		}
	}
	blocked := breaker.blockedStorageIDs(start.Add(5 * time.Minute))
	if len(blocked) != 1 || blocked[0] != "storage-a" {
		t.Fatalf("blocked storages = %v", blocked)
	}
}

func TestStorageCircuitUsesOneProbeAfterCooldown(t *testing.T) {
	breaker := newStorageCircuitBreaker()
	now := time.Date(2026, 9, 18, 8, 0, 0, 0, time.UTC)
	state := &storageCircuitState{openUntil: now.Add(storageCooldown)}
	breaker.states["storage-a"] = state
	probeAt := state.openUntil
	probe := breaker.begin("storage-a", probeAt)
	if !probe.allowed || !probe.probe {
		t.Fatalf("probe lease = %+v", probe)
	}
	if second := breaker.begin("storage-a", probeAt); second.allowed {
		t.Fatalf("second concurrent probe was allowed: %+v", second)
	}
	if until := breaker.finish(probe, circuitSuccess, probeAt.Add(time.Second)); !until.IsZero() {
		t.Fatalf("successful probe kept circuit open until %v", until)
	}
	if next := breaker.begin("storage-a", probeAt.Add(2*time.Second)); !next.allowed || next.probe {
		t.Fatalf("storage did not close after successful probe: %+v", next)
	}
}

func TestStorageCircuitFailedProbeRestartsCooldown(t *testing.T) {
	breaker := newStorageCircuitBreaker()
	now := time.Date(2026, 9, 18, 8, 0, 0, 0, time.UTC)
	breaker.states["storage-a"] = &storageCircuitState{openUntil: now}
	probe := breaker.begin("storage-a", now)
	openUntil := breaker.finish(probe, circuitFailure, now.Add(time.Second))
	want := now.Add(time.Second).Add(storageCooldown)
	if !openUntil.Equal(want) {
		t.Fatalf("openUntil = %v, want %v", openUntil, want)
	}
	if next := breaker.begin("storage-a", now.Add(time.Minute)); next.allowed {
		t.Fatal("storage was allowed during restarted cooldown")
	}
}

func TestStorageCircuitExpiresOldFailures(t *testing.T) {
	breaker := newStorageCircuitBreaker()
	start := time.Date(2026, 9, 18, 8, 0, 0, 0, time.UTC)
	for i := 0; i < storageFailureThreshold-1; i++ {
		now := start.Add(time.Duration(i) * time.Minute)
		lease := breaker.begin("storage-a", now)
		breaker.finish(lease, circuitFailure, now)
	}
	later := start.Add(storageFailureWindow + time.Minute)
	lease := breaker.begin("storage-a", later)
	if until := breaker.finish(lease, circuitFailure, later); !until.IsZero() {
		t.Fatalf("expired failures opened circuit until %v", until)
	}
}
