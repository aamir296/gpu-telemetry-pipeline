package collector

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/broker/protocol"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/config"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/model"
)

type fakeBroker struct {
	mu            sync.Mutex
	registerCalls int
	fetchCalls    int
	commitCalls   int
	registerErrs  []error
	fetchResults  [][]protocol.Delivery
	fetchErrs     []error
	commitErrs    []error
	cancel        context.CancelFunc
}

func (f *fakeBroker) RegisterConsumer(context.Context, string, string) (protocol.MembershipResponse, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	index := f.registerCalls
	f.registerCalls++
	if index < len(f.registerErrs) && f.registerErrs[index] != nil {
		return protocol.MembershipResponse{}, f.registerErrs[index]
	}
	return protocol.MembershipResponse{IncarnationID: "inc", Generation: uint64(f.registerCalls)}, nil
}

func (f *fakeBroker) Fetch(context.Context, protocol.FetchRequest) ([]protocol.Delivery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	index := f.fetchCalls
	f.fetchCalls++
	if index < len(f.fetchErrs) && f.fetchErrs[index] != nil {
		return nil, f.fetchErrs[index]
	}
	if index < len(f.fetchResults) {
		return f.fetchResults[index], nil
	}
	if f.cancel != nil {
		f.cancel()
	}
	return nil, nil
}

func (f *fakeBroker) Commit(context.Context, protocol.CommitRequest) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	index := f.commitCalls
	f.commitCalls++
	if f.cancel != nil {
		f.cancel()
	}
	if index < len(f.commitErrs) {
		return f.commitErrs[index]
	}
	return nil
}

type fakeRepository struct {
	mu      sync.Mutex
	calls   int
	errors  []error
	offsets map[uint32]uint64
}

func (f *fakeRepository) PersistBatch(context.Context, []protocol.Delivery) (map[uint32]uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	index := f.calls
	f.calls++
	if index < len(f.errors) && f.errors[index] != nil {
		return nil, f.errors[index]
	}
	return f.offsets, nil
}

func collectorConfig() config.Collector {
	return config.Collector{CollectorID: "collector", BatchSize: 10, PollWait: time.Millisecond}
}

func delivery() protocol.Delivery {
	return protocol.Delivery{Partition: 1, Offset: 4, Event: model.Telemetry{UUID: "GPU-1", SourceEventKey: "source-1"}}
}

func TestRunPersistsBeforeCommit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	broker := &fakeBroker{fetchResults: [][]protocol.Delivery{{delivery()}}, cancel: cancel}
	repo := &fakeRepository{offsets: map[uint32]uint64{1: 5}}
	if err := newWithDependencies(collectorConfig(), broker, repo).Run(ctx); err != nil {
		t.Fatal(err)
	}
	if repo.calls != 1 || broker.commitCalls != 1 {
		t.Fatalf("persist calls=%d commit calls=%d", repo.calls, broker.commitCalls)
	}
}

func TestRunRetriesRegistrationFetchPersistAndCommit(t *testing.T) {
	tests := []struct {
		name          string
		broker        *fakeBroker
		repo          *fakeRepository
		wantRegisters int
		wantPersists  int
	}{
		{
			name: "registration",
			broker: &fakeBroker{
				registerErrs: []error{errors.New("offline")},
				fetchResults: [][]protocol.Delivery{{delivery()}},
			},
			repo:          &fakeRepository{offsets: map[uint32]uint64{1: 5}},
			wantRegisters: 2,
			wantPersists:  1,
		},
		{
			name: "fetch",
			broker: &fakeBroker{
				fetchErrs:    []error{errors.New("stale"), nil},
				fetchResults: [][]protocol.Delivery{nil, {delivery()}},
			},
			repo:          &fakeRepository{offsets: map[uint32]uint64{1: 5}},
			wantRegisters: 2,
			wantPersists:  1,
		},
		{
			name:   "persist",
			broker: &fakeBroker{fetchResults: [][]protocol.Delivery{{delivery()}, {delivery()}}},
			repo: &fakeRepository{
				errors:  []error{errors.New("database unavailable"), nil},
				offsets: map[uint32]uint64{1: 5},
			},
			wantRegisters: 1,
			wantPersists:  2,
		},
		{
			name: "commit",
			broker: &fakeBroker{
				fetchResults: [][]protocol.Delivery{{delivery()}},
				commitErrs:   []error{errors.New("stale")},
			},
			repo:          &fakeRepository{offsets: map[uint32]uint64{1: 5}},
			wantRegisters: 1,
			wantPersists:  1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			test.broker.cancel = cancel
			if err := newWithDependencies(collectorConfig(), test.broker, test.repo).Run(ctx); err != nil {
				t.Fatal(err)
			}
			if test.broker.registerCalls != test.wantRegisters || test.repo.calls != test.wantPersists {
				t.Fatalf("register calls=%d persist calls=%d", test.broker.registerCalls, test.repo.calls)
			}
		})
	}
}

func TestWaitStopsOnCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	start := time.Now()
	(&Runner{cfg: config.Collector{PollWait: time.Hour}}).wait(ctx)
	if time.Since(start) > 100*time.Millisecond {
		t.Fatal("wait ignored cancellation")
	}
}
