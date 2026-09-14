package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
)

const (
	Version         = 1
	HeaderSize      = 24
	DefaultMaxFrame = 8 << 20
)

var magic = [4]byte{'G', 'T', 'Q', '1'}
var crcTable = crc32.MakeTable(crc32.Castagnoli)

type Operation uint8

const (
	OpRegisterProducer Operation = 1
	OpAcquireCycle     Operation = 2
	OpCompleteCycle    Operation = 3
	OpPublish          Operation = 4
	OpRegisterConsumer Operation = 5
	OpHeartbeat        Operation = 6
	OpFetch            Operation = 7
	OpCommit           Operation = 8
	OpStats            Operation = 9
	OpResponse         Operation = 100
	OpError            Operation = 101
)

type Frame struct {
	Operation Operation
	Flags     uint16
	RequestID uint64
	Payload   []byte
}

func WriteFrame(w io.Writer, f Frame) error {
	if len(f.Payload) > DefaultMaxFrame {
		return fmt.Errorf("payload exceeds maximum frame size: %d", len(f.Payload))
	}
	header := make([]byte, HeaderSize)
	copy(header[:4], magic[:])
	header[4] = Version
	header[5] = byte(f.Operation)
	binary.BigEndian.PutUint16(header[6:8], f.Flags)
	binary.BigEndian.PutUint64(header[8:16], f.RequestID)
	binary.BigEndian.PutUint32(header[16:20], uint32(len(f.Payload)))
	binary.BigEndian.PutUint32(header[20:24], crc32.Checksum(f.Payload, crcTable))
	if _, err := w.Write(header); err != nil {
		return err
	}
	_, err := w.Write(f.Payload)
	return err
}

func ReadFrame(r io.Reader, maxBytes int) (Frame, error) {
	var f Frame
	if maxBytes <= 0 {
		maxBytes = DefaultMaxFrame
	}
	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return f, err
	}
	if string(header[:4]) != string(magic[:]) {
		return f, errors.New("invalid frame magic")
	}
	if header[4] != Version {
		return f, fmt.Errorf("unsupported protocol version %d", header[4])
	}
	n := int(binary.BigEndian.Uint32(header[16:20]))
	if n < 0 || n > maxBytes {
		return f, fmt.Errorf("frame payload %d exceeds limit %d", n, maxBytes)
	}
	f.Operation = Operation(header[5])
	f.Flags = binary.BigEndian.Uint16(header[6:8])
	f.RequestID = binary.BigEndian.Uint64(header[8:16])
	f.Payload = make([]byte, n)
	if _, err := io.ReadFull(r, f.Payload); err != nil {
		return Frame{}, err
	}
	want := binary.BigEndian.Uint32(header[20:24])
	if got := crc32.Checksum(f.Payload, crcTable); got != want {
		return Frame{}, fmt.Errorf("frame checksum mismatch: got %08x want %08x", got, want)
	}
	return f, nil
}
