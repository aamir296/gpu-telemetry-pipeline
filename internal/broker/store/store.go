package store

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"hash/fnv"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/aamir296/gpu-telemetry-pipeline/internal/broker/protocol"
	"github.com/aamir296/gpu-telemetry-pipeline/internal/model"
)

const recordHeaderSize = 16
const storeFormatVersion = 1

var crcTable = crc32.MakeTable(crc32.Castagnoli)
var ErrCapacity = errors.New("queue capacity reached")

type location struct {
	path           string
	pos            int64
	size           uint32
	sourceEventKey string
}

type partition struct {
	mu           sync.RWMutex
	id           uint32
	dir          string
	segmentBytes int64
	maxRecord    int
	nextOffset   uint64
	locations    map[uint64]location
	dedup        map[string]uint64
	file         *os.File
	fileSize     int64
	totalBytes   int64
}

// Store is a durable, partitioned append-only telemetry log.
type Store struct {
	dir        string
	partitions []*partition
	maxBytes   int64
	appendMu   sync.Mutex
	commitMu   sync.Mutex
	commits    map[string]map[uint32]uint64 // committed value is the next offset to fetch
}

type storeMetadata struct {
	FormatVersion int    `json:"format_version"`
	Partitions    uint32 `json:"partitions"`
}

func Open(dir string, partitionCount uint32, segmentBytes int64, maxRecord int) (*Store, error) {
	return OpenWithLimit(dir, partitionCount, segmentBytes, maxRecord, 0)
}

// OpenWithLimit opens a store and rejects publication before its durable log
// exceeds maxBytes. A zero limit disables capacity backpressure.
func OpenWithLimit(dir string, partitionCount uint32, segmentBytes int64, maxRecord int, maxBytes int64) (*Store, error) {
	if partitionCount == 0 {
		return nil, errors.New("partition count must be positive")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, err
	}
	if err := ensureMetadata(dir, partitionCount); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, maxBytes: maxBytes, commits: make(map[string]map[uint32]uint64)}
	for i := uint32(0); i < partitionCount; i++ {
		p := &partition{
			id: i, dir: filepath.Join(dir, fmt.Sprintf("partition-%04d", i)),
			segmentBytes: segmentBytes, maxRecord: maxRecord,
			locations: make(map[uint64]location), dedup: make(map[string]uint64),
		}
		if err := p.open(); err != nil {
			s.Close()
			return nil, fmt.Errorf("open partition %d: %w", i, err)
		}
		s.partitions = append(s.partitions, p)
	}
	if err := s.loadCommits(); err != nil {
		s.Close()
		return nil, err
	}
	return s, nil
}

func ensureMetadata(dir string, partitionCount uint32) error {
	path := filepath.Join(dir, "metadata.json")
	data, err := os.ReadFile(path)
	if err == nil {
		var metadata storeMetadata
		if err := json.Unmarshal(data, &metadata); err != nil {
			return fmt.Errorf("decode queue metadata: %w", err)
		}
		if metadata.FormatVersion != storeFormatVersion {
			return fmt.Errorf("unsupported queue format version %d", metadata.FormatVersion)
		}
		if metadata.Partitions != partitionCount {
			return fmt.Errorf("queue partition count is immutable: stored=%d configured=%d", metadata.Partitions, partitionCount)
		}
		return nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	// Upgrade a pre-metadata store without allowing its existing layout to be
	// silently reinterpreted under a different partition count.
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	existingPartitions := uint32(0)
	for _, entry := range entries {
		if entry.IsDir() && strings.HasPrefix(entry.Name(), "partition-") {
			existingPartitions++
		}
	}
	if existingPartitions > 0 && existingPartitions != partitionCount {
		return fmt.Errorf("queue partition count is immutable: stored=%d configured=%d", existingPartitions, partitionCount)
	}
	data, err = json.Marshal(storeMetadata{FormatVersion: storeFormatVersion, Partitions: partitionCount})
	if err != nil {
		return err
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, data, 0o640); err != nil {
		return err
	}
	return os.Rename(temporary, path)
}

func (s *Store) Close() error {
	var errs []error
	for _, p := range s.partitions {
		if p != nil && p.file != nil {
			errs = append(errs, p.file.Close())
		}
	}
	return errors.Join(errs...)
}

func (s *Store) PartitionCount() uint32 { return uint32(len(s.partitions)) }

func (s *Store) PartitionFor(key string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return h.Sum32() % uint32(len(s.partitions))
}

type AppendResult struct {
	Accepted   int
	Duplicates int
}

func (s *Store) Append(ctx context.Context, events []model.Telemetry) (AppendResult, error) {
	s.appendMu.Lock()
	defer s.appendMu.Unlock()
	var result AppendResult
	byPartition := make(map[uint32][]model.Telemetry)
	var incomingBytes int64
	for _, event := range events {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if event.GPUKey() == "" || event.SourceEventKey == "" {
			return result, errors.New("event requires GPU identity and source_event_key")
		}
		partitionID := s.PartitionFor(event.GPUKey())
		partition := s.partitions[partitionID]
		partition.mu.RLock()
		_, duplicate := partition.dedup[event.SourceEventKey]
		partition.mu.RUnlock()
		if event.EventID == "" {
			event.EventID = newEventID()
		}
		event.QueuePartition = partitionID
		byPartition[partitionID] = append(byPartition[partitionID], event)
		if !duplicate {
			encoded, err := protocol.EncodeTelemetry(event)
			if err != nil {
				return result, err
			}
			incomingBytes += recordHeaderSize + int64(len(encoded))
		}
	}
	if s.maxBytes > 0 && s.bytes()+incomingBytes > s.maxBytes {
		return result, fmt.Errorf("%w: %d-byte limit", ErrCapacity, s.maxBytes)
	}
	ids := make([]int, 0, len(byPartition))
	for id := range byPartition {
		ids = append(ids, int(id))
	}
	sort.Ints(ids)
	for _, rawID := range ids {
		accepted, duplicates, err := s.partitions[rawID].appendBatch(byPartition[uint32(rawID)])
		result.Accepted += accepted
		result.Duplicates += duplicates
		if err != nil {
			return result, err
		}
	}
	return result, nil
}

func (s *Store) bytes() int64 {
	var total int64
	for _, p := range s.partitions {
		p.mu.RLock()
		total += p.totalBytes
		p.mu.RUnlock()
	}
	return total
}

func (s *Store) Fetch(group string, partitionID uint32, max int) ([]protocol.Delivery, error) {
	if int(partitionID) >= len(s.partitions) {
		return nil, errors.New("invalid partition")
	}
	s.commitMu.Lock()
	start := s.commits[group][partitionID]
	s.commitMu.Unlock()
	return s.partitions[partitionID].fetch(start, max)
}

func (s *Store) Commit(group string, offsets map[uint32]uint64) error {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	if s.commits[group] == nil {
		s.commits[group] = make(map[uint32]uint64)
	}
	for partitionID, next := range offsets {
		if int(partitionID) >= len(s.partitions) || next > s.partitions[partitionID].nextOffset {
			return fmt.Errorf("invalid commit offset %d for partition %d", next, partitionID)
		}
		if next > s.commits[group][partitionID] {
			s.commits[group][partitionID] = next
		}
	}
	if err := s.saveCommitsLocked(); err != nil {
		return err
	}
	return s.reclaimLocked()
}

func (s *Store) Committed(group string, partitionID uint32) uint64 {
	s.commitMu.Lock()
	defer s.commitMu.Unlock()
	return s.commits[group][partitionID]
}

func (s *Store) HighWatermarks() map[uint32]uint64 {
	result := make(map[uint32]uint64, len(s.partitions))
	for _, p := range s.partitions {
		p.mu.RLock()
		result[p.id] = p.nextOffset
		p.mu.RUnlock()
	}
	return result
}

func (p *partition) open() error {
	if err := os.MkdirAll(p.dir, 0o750); err != nil {
		return err
	}
	entries, err := os.ReadDir(p.dir)
	if err != nil {
		return err
	}
	var paths []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".log") {
			paths = append(paths, filepath.Join(p.dir, entry.Name()))
		}
	}
	sort.Strings(paths)
	for _, path := range paths {
		if err := p.scan(path); err != nil {
			return err
		}
	}
	if len(paths) == 0 {
		return p.rotate()
	}
	last := paths[len(paths)-1]
	p.file, err = os.OpenFile(last, os.O_RDWR|os.O_APPEND, 0o640)
	if err != nil {
		return err
	}
	info, err := p.file.Stat()
	if err != nil {
		return err
	}
	p.fileSize = info.Size()
	return nil
}

func (p *partition) scan(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0o640)
	if err != nil {
		return err
	}
	defer f.Close()
	var pos int64
	for {
		header := make([]byte, recordHeaderSize)
		_, err := io.ReadFull(f, header)
		if errors.Is(err, io.EOF) {
			return nil
		}
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return f.Truncate(pos)
		}
		if err != nil {
			return err
		}
		n := binary.BigEndian.Uint32(header[:4])
		offset := binary.BigEndian.Uint64(header[4:12])
		wantCRC := binary.BigEndian.Uint32(header[12:16])
		if n == 0 || int(n) > p.maxRecord {
			return fmt.Errorf("corrupt record length at %s:%d", path, pos)
		}
		payload := make([]byte, n)
		_, err = io.ReadFull(f, payload)
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			return f.Truncate(pos)
		}
		if err != nil {
			return err
		}
		if crc32.Checksum(payload, crcTable) != wantCRC {
			return fmt.Errorf("checksum mismatch at %s offset %d", path, offset)
		}
		event, err := protocol.DecodeTelemetry(payload, p.maxRecord)
		if err != nil {
			return fmt.Errorf("decode %s offset %d: %w", path, offset, err)
		}
		p.locations[offset] = location{path: path, pos: pos, size: n, sourceEventKey: event.SourceEventKey}
		p.dedup[event.SourceEventKey] = offset
		if offset >= p.nextOffset {
			p.nextOffset = offset + 1
		}
		pos += recordHeaderSize + int64(n)
		p.totalBytes += recordHeaderSize + int64(n)
	}
}

func (p *partition) appendBatch(events []model.Telemetry) (int, int, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	accepted, duplicates := 0, 0
	for _, event := range events {
		if _, exists := p.dedup[event.SourceEventKey]; exists {
			duplicates++
			continue
		}
		if event.EventID == "" {
			event.EventID = newEventID()
		}
		event.QueuePartition = p.id
		event.QueueOffset = p.nextOffset
		payload, err := protocol.EncodeTelemetry(event)
		if err != nil {
			return accepted, duplicates, err
		}
		if len(payload) > p.maxRecord {
			return accepted, duplicates, errors.New("encoded record exceeds maximum")
		}
		if p.fileSize > 0 && p.fileSize+recordHeaderSize+int64(len(payload)) > p.segmentBytes {
			if err := p.rotate(); err != nil {
				return accepted, duplicates, err
			}
		}
		pos := p.fileSize
		header := make([]byte, recordHeaderSize)
		binary.BigEndian.PutUint32(header[:4], uint32(len(payload)))
		binary.BigEndian.PutUint64(header[4:12], p.nextOffset)
		binary.BigEndian.PutUint32(header[12:16], crc32.Checksum(payload, crcTable))
		if _, err := p.file.Write(header); err != nil {
			return accepted, duplicates, err
		}
		if _, err := p.file.Write(payload); err != nil {
			return accepted, duplicates, err
		}
		p.locations[p.nextOffset] = location{path: p.file.Name(), pos: pos, size: uint32(len(payload)), sourceEventKey: event.SourceEventKey}
		p.dedup[event.SourceEventKey] = p.nextOffset
		p.nextOffset++
		p.fileSize += int64(len(header) + len(payload))
		p.totalBytes += int64(len(header) + len(payload))
		accepted++
	}
	if accepted > 0 {
		// Publication is acknowledged only after durable sync. One sync covers the batch.
		if err := p.file.Sync(); err != nil {
			return accepted, duplicates, err
		}
	}
	return accepted, duplicates, nil
}

func (p *partition) fetch(start uint64, max int) ([]protocol.Delivery, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if max <= 0 {
		return nil, nil
	}
	result := make([]protocol.Delivery, 0, max)
	files := make(map[string]*os.File)
	defer func() {
		for _, file := range files {
			_ = file.Close()
		}
	}()
	for offset := start; offset < p.nextOffset && len(result) < max; offset++ {
		loc, ok := p.locations[offset]
		if !ok {
			return nil, fmt.Errorf("missing index for partition %d offset %d", p.id, offset)
		}
		f := files[loc.path]
		if f == nil {
			var err error
			f, err = os.Open(loc.path)
			if err != nil {
				return nil, err
			}
			files[loc.path] = f
		}
		payload := make([]byte, loc.size)
		_, err := f.ReadAt(payload, loc.pos+recordHeaderSize)
		if err != nil {
			return nil, err
		}
		event, err := protocol.DecodeTelemetry(payload, p.maxRecord)
		if err != nil {
			return nil, err
		}
		result = append(result, protocol.Delivery{Partition: p.id, Offset: offset, Event: event})
	}
	return result, nil
}

func (p *partition) rotate() error {
	if p.file != nil {
		if err := p.file.Sync(); err != nil {
			return err
		}
		if err := p.file.Close(); err != nil {
			return err
		}
	}
	path := filepath.Join(p.dir, fmt.Sprintf("%020d.log", p.nextOffset))
	f, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_RDWR|os.O_APPEND, 0o640)
	if err != nil {
		return err
	}
	p.file = f
	p.fileSize = 0
	return nil
}

func (s *Store) loadCommits() error {
	data, err := os.ReadFile(filepath.Join(s.dir, "commits.json"))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	var disk map[string]map[string]uint64
	if err := json.Unmarshal(data, &disk); err != nil {
		return err
	}
	for group, values := range disk {
		s.commits[group] = make(map[uint32]uint64)
		for raw, offset := range values {
			id, err := strconv.ParseUint(raw, 10, 32)
			if err != nil {
				return err
			}
			s.commits[group][uint32(id)] = offset
		}
	}
	return nil
}

func (s *Store) saveCommitsLocked() error {
	disk := make(map[string]map[string]uint64)
	for group, values := range s.commits {
		disk[group] = make(map[string]uint64)
		for partitionID, offset := range values {
			disk[group][strconv.FormatUint(uint64(partitionID), 10)] = offset
		}
	}
	data, err := json.Marshal(disk)
	if err != nil {
		return err
	}
	tmp := filepath.Join(s.dir, "commits.json.tmp")
	if err := os.WriteFile(tmp, data, 0o640); err != nil {
		return err
	}
	return os.Rename(tmp, filepath.Join(s.dir, "commits.json"))
}

// reclaimLocked deletes only closed segments consumed by every known group.
// The active segment is retained even when fully consumed so appends remain
// atomic and no reader can race an open-file replacement.
func (s *Store) reclaimLocked() error {
	for _, partition := range s.partitions {
		minimum := ^uint64(0)
		for _, group := range s.commits {
			next, exists := group[partition.id]
			if !exists {
				minimum = 0
				break
			}
			if next < minimum {
				minimum = next
			}
		}
		if minimum == 0 || minimum == ^uint64(0) {
			continue
		}
		partition.mu.Lock()
		active := partition.file.Name()
		eligible := make(map[string]bool)
		for offset, loc := range partition.locations {
			if loc.path != active {
				if _, exists := eligible[loc.path]; !exists {
					eligible[loc.path] = true
				}
				if offset >= minimum {
					eligible[loc.path] = false
				}
			}
		}
		for path, canDelete := range eligible {
			if !canDelete {
				continue
			}
			info, err := os.Stat(path)
			if err != nil {
				partition.mu.Unlock()
				return err
			}
			if err := os.Remove(path); err != nil {
				partition.mu.Unlock()
				return err
			}
			partition.totalBytes -= info.Size()
			for offset, loc := range partition.locations {
				if loc.path != path {
					continue
				}
				delete(partition.locations, offset)
				delete(partition.dedup, loc.sourceEventKey)
			}
		}
		partition.mu.Unlock()
	}
	return nil
}

func newEventID() string {
	var random [10]byte
	_, _ = rand.Read(random[:])
	return fmt.Sprintf("%012x%s", time.Now().UTC().UnixMilli(), hex.EncodeToString(random[:]))
}
