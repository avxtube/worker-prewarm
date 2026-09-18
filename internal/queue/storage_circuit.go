package queue

import (
	"sort"
	"sync"
	"time"

	"worker-prewarm/internal/db/models"
)

const (
	storageFailureWindow    = 10 * time.Minute
	storageFailureThreshold = 5
	storageCooldown         = 10 * time.Minute
)

type circuitOutcome uint8

const (
	circuitNeutral circuitOutcome = iota
	circuitSuccess
	circuitFailure
)

type storageCircuitLease struct {
	storageID string
	probe     bool
	allowed   bool
}

type storageCircuitState struct {
	failures      []time.Time
	openUntil     time.Time
	probeInFlight bool
}

type storageCircuitBreaker struct {
	mu     sync.Mutex
	states map[string]*storageCircuitState
}

func newStorageCircuitBreaker() *storageCircuitBreaker {
	return &storageCircuitBreaker{states: make(map[string]*storageCircuitState)}
}

var prewarmStorageCircuit = newStorageCircuitBreaker()

func storageIDForJob(job *models.PrewarmQueue) string {
	if job == nil {
		return ""
	}
	if job.StorageID != nil && *job.StorageID != "" {
		return *job.StorageID
	}
	if job.TargetStorageID != nil {
		return *job.TargetStorageID
	}
	return ""
}

// blockedStorageIDs returns storage IDs that must remain in pending state.
// Once a cooldown expires the storage becomes claimable for one half-open probe.
func (b *storageCircuitBreaker) blockedStorageIDs(now time.Time) []string {
	b.mu.Lock()
	defer b.mu.Unlock()

	blocked := make([]string, 0, len(b.states))
	for storageID, state := range b.states {
		b.pruneFailures(state, now)
		if now.Before(state.openUntil) || state.probeInFlight {
			blocked = append(blocked, storageID)
		}
		if len(state.failures) == 0 && state.openUntil.IsZero() && !state.probeInFlight {
			delete(b.states, storageID)
		}
	}
	sort.Strings(blocked)
	return blocked
}

// begin reserves a single half-open probe after a storage cooldown. Jobs for a
// healthy storage do not need a reservation and may continue concurrently.
func (b *storageCircuitBreaker) begin(storageID string, now time.Time) storageCircuitLease {
	lease := storageCircuitLease{storageID: storageID, allowed: true}
	if storageID == "" {
		return lease
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	state := b.states[storageID]
	if state == nil {
		return lease
	}
	b.pruneFailures(state, now)
	if now.Before(state.openUntil) || state.probeInFlight {
		lease.allowed = false
		return lease
	}
	if !state.openUntil.IsZero() {
		state.probeInFlight = true
		lease.probe = true
	}
	return lease
}

// finish records only storage-level failures. Configuration, cancellation and
// content metadata errors are neutral and do not influence storage health.
// It returns the active cooldown deadline, or the zero time when closed.
func (b *storageCircuitBreaker) finish(lease storageCircuitLease, outcome circuitOutcome, now time.Time) time.Time {
	if lease.storageID == "" || !lease.allowed {
		return time.Time{}
	}

	b.mu.Lock()
	defer b.mu.Unlock()
	state := b.states[lease.storageID]
	if state == nil {
		if outcome != circuitFailure {
			return time.Time{}
		}
		state = &storageCircuitState{}
		b.states[lease.storageID] = state
	}

	if lease.probe {
		state.probeInFlight = false
		switch outcome {
		case circuitSuccess:
			delete(b.states, lease.storageID)
			return time.Time{}
		case circuitFailure:
			state.failures = []time.Time{now}
			state.openUntil = now.Add(storageCooldown)
			return state.openUntil
		default:
			return time.Time{}
		}
	}

	// Ignore results from jobs that started before another concurrent job opened
	// the circuit. They must not extend or close the current cooldown.
	if !state.openUntil.IsZero() {
		if now.Before(state.openUntil) {
			return state.openUntil
		}
		return time.Time{}
	}
	b.pruneFailures(state, now)
	if outcome == circuitFailure {
		state.failures = append(state.failures, now)
		if len(state.failures) >= storageFailureThreshold {
			state.openUntil = now.Add(storageCooldown)
			return state.openUntil
		}
	}
	if len(state.failures) == 0 && state.openUntil.IsZero() {
		delete(b.states, lease.storageID)
	}
	return time.Time{}
}

func (b *storageCircuitBreaker) pruneFailures(state *storageCircuitState, now time.Time) {
	cutoff := now.Add(-storageFailureWindow)
	first := 0
	for first < len(state.failures) && state.failures[first].Before(cutoff) {
		first++
	}
	if first > 0 {
		state.failures = append([]time.Time(nil), state.failures[first:]...)
	}
}
