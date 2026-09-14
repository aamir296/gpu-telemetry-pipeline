package server

import (
	"sort"
	"sync"
	"time"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/shard"
)

type groupState struct {
	generation uint64
	members    map[string]time.Time
}

type memberships struct {
	mu     sync.Mutex
	ttl    time.Duration
	groups map[string]*groupState
}

func newMemberships(ttl time.Duration) *memberships {
	return &memberships{ttl: ttl, groups: make(map[string]*groupState)}
}

func (m *memberships) touch(group, member string, now time.Time) (uint64, []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.groups[group]
	if g == nil {
		g = &groupState{members: make(map[string]time.Time)}
		m.groups[group] = g
	}
	changed := false
	for id, seen := range g.members {
		// The request itself proves the caller is alive. Do not expire and
		// re-add that same member, which would manufacture a generation change
		// when a legitimate unit of work takes longer than the lease.
		if id != member && now.Sub(seen) > m.ttl {
			delete(g.members, id)
			changed = true
		}
	}
	if _, exists := g.members[member]; !exists {
		changed = true
	}
	g.members[member] = now
	// Every membership transition creates a new assignment generation. This is
	// also required when a group scales down to one member so stale workers are
	// fenced and cycle completion is replayed under the surviving assignment.
	if changed {
		g.generation++
	}
	return g.generation, sortedMembers(g.members)
}

func (m *memberships) snapshot(group string, now time.Time) (uint64, []string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.groups[group]
	if g == nil {
		return 0, nil
	}
	changed := false
	for id, seen := range g.members {
		if now.Sub(seen) > m.ttl {
			delete(g.members, id)
			changed = true
		}
	}
	if changed {
		g.generation++
	}
	return g.generation, sortedMembers(g.members)
}

func sortedMembers(input map[string]time.Time) []string {
	result := make([]string, 0, len(input))
	for member := range input {
		result = append(result, member)
	}
	sort.Strings(result)
	return result
}

func assignedPartitions(member string, members []string, count uint32) []uint32 {
	result := make([]uint32, 0)
	for partition := uint32(0); partition < count; partition++ {
		if shard.Owner("partition", stringKey(partition), members) == member {
			result = append(result, partition)
		}
	}
	return result
}

func stringKey(value uint32) string {
	return string([]byte{byte(value >> 24), byte(value >> 16), byte(value >> 8), byte(value)})
}
