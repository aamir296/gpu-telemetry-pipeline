package server

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"sync"
	"time"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/broker/protocol"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/shard"
)

type cycleState struct {
	ID         uint64          `json:"id"`
	Generation uint64          `json:"generation"`
	StartedAt  time.Time       `json:"started_at"`
	Members    []string        `json:"-"`
	Completed  map[string]bool `json:"-"`
}

type cycles struct {
	mu       sync.Mutex
	path     string
	interval time.Duration
	values   map[string]*cycleState
}

func openCycles(dir string, interval time.Duration) (*cycles, error) {
	c := &cycles{path: filepath.Join(dir, "cycles.json"), interval: interval, values: make(map[string]*cycleState)}
	data, err := os.ReadFile(c.path)
	if err == nil {
		if err := json.Unmarshal(data, &c.values); err != nil {
			return nil, err
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	// Membership is intentionally rebuilt after a broker restart. The same cycle
	// ID is replayed and the broker's source-key deduplication removes repeated work.
	for _, state := range c.values {
		state.Members = nil
		state.Completed = make(map[string]bool)
	}
	return c, nil
}

func (c *cycles) acquire(dataset, member string, generation uint64, active []string, now time.Time, incarnation string) (protocol.CycleResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.values[dataset]
	if state == nil {
		state = &cycleState{ID: 1, Generation: generation, StartedAt: now, Members: append([]string(nil), active...), Completed: make(map[string]bool)}
		c.values[dataset] = state
		if err := c.saveLocked(); err != nil {
			return protocol.CycleResponse{}, err
		}
	}
	// A membership or generation change invalidates completion recorded for the
	// previous assignment. Active producers must replay this same cycle under the
	// new assignment; source-key deduplication makes already accepted rows safe.
	if state.Generation != generation || !slices.Equal(state.Members, active) {
		state.Completed = make(map[string]bool)
	}
	state.Members = append([]string(nil), active...)
	state.Generation = generation
	allComplete := len(state.Members) > 0
	for _, id := range state.Members {
		allComplete = allComplete && state.Completed[id]
	}
	if allComplete {
		readyAt := state.StartedAt.Add(c.interval)
		if now.Before(readyAt) {
			return protocol.CycleResponse{CycleID: state.ID, Generation: state.Generation, IncarnationID: incarnation, Members: append([]string(nil), state.Members...), Ready: false, WaitMillis: readyAt.Sub(now).Milliseconds()}, nil
		}
		state.ID++
		state.StartedAt = now
		state.Completed = make(map[string]bool)
		state.Members = append([]string(nil), active...)
		if err := c.saveLocked(); err != nil {
			return protocol.CycleResponse{}, err
		}
	}
	return protocol.CycleResponse{CycleID: state.ID, Generation: state.Generation, IncarnationID: incarnation, Members: append([]string(nil), state.Members...), Ready: true}, nil
}

func (c *cycles) complete(dataset, member string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.values[dataset]
	if state == nil {
		return errors.New("cycle not found")
	}
	state.Completed[member] = true
	return nil
}

func (c *cycles) validate(dataset string, cycleID, generation uint64, member, gpuKey string) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	state := c.values[dataset]
	if state == nil || state.ID != cycleID || state.Generation != generation {
		return false
	}
	return shard.Owner("gpu", gpuKey, state.Members) == member
}

func (c *cycles) saveLocked() error {
	data, err := json.Marshal(c.values)
	if err != nil {
		return err
	}
	tmp := c.path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o640); err != nil {
		return err
	}
	return os.Rename(tmp, c.path)
}
