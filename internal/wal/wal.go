package wal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
)

// WAL (Write-Ahead Log) provides durability by logging operations before applying them.
type WAL struct {
	file *os.File
	path string
}

// Entry represents a single operation in the WAL.
type Entry struct {
	Op        string
	Key       string
	Value     []byte
	Timestamp uint64
}

// Header layout (24 bytes total on disk):
// - Checksum:  4 bytes (written separately)
// - Flags:     4 bytes
// - Timestamp: 8 bytes
// - KeyLen:    4 bytes
// - ValueLen:  4 bytes
type Header struct {
	Checksum  uint32
	Flags     uint32
	Timestamp uint64
	KeyLen    uint32
	ValueLen  uint32
}

const (
	FlagDelete uint32 = 1 << 0
	HeaderSize        = 20 // Excludes checksum (4 bytes read separately)
)

// NewWAL opens or creates a WAL file at the given path.
func NewWAL(path string) (*WAL, error) {
	f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &WAL{file: f, path: path}, nil
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	return w.file.Close()
}

// Append writes an operation to the WAL.
func (w *WAL) Append(op string, key string, value []byte, timestamp uint64) error {
	flags := encodeFlags(op)
	data := encodeEntry(flags, timestamp, key, value)
	checksum := crc32.ChecksumIEEE(data)

	if err := w.writeChecksum(checksum); err != nil {
		return err
	}
	if err := w.writeData(data); err != nil {
		return err
	}

	return w.file.Sync()
}

// Replay reads all entries from the WAL and verifies their integrity.
func (w *WAL) Replay() ([]Entry, error) {
	f, err := os.OpenFile(w.path, os.O_RDONLY, 0)
	if err != nil {
		return nil, fmt.Errorf("could not open WAL for replay: %w", err)
	}
	defer f.Close()

	var entries []Entry
	headerBuf := make([]byte, HeaderSize)

	for {
		// Read and verify each entry
		entry, err := readEntry(f, headerBuf)
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// --- Encoding Helpers ---

func encodeFlags(op string) uint32 {
	if op == "DELETE" {
		return FlagDelete
	}
	return 0
}

func encodeEntry(flags uint32, timestamp uint64, key string, value []byte) []byte {
	buf := new(bytes.Buffer)

	binary.Write(buf, binary.BigEndian, flags)
	binary.Write(buf, binary.BigEndian, timestamp)
	binary.Write(buf, binary.BigEndian, uint32(len(key)))
	binary.Write(buf, binary.BigEndian, uint32(len(value)))
	buf.Write([]byte(key))
	buf.Write(value)

	return buf.Bytes()
}

// --- Write Helpers ---

func (w *WAL) writeChecksum(checksum uint32) error {
	return binary.Write(w.file, binary.BigEndian, checksum)
}

func (w *WAL) writeData(data []byte) error {
	_, err := w.file.Write(data)
	return err
}

// --- Read Helpers ---

func readEntry(f *os.File, headerBuf []byte) (Entry, error) {
	// Read checksum
	storedChecksum, err := readChecksum(f)
	if err != nil {
		return Entry{}, err
	}

	// Read header
	if err := readFull(f, headerBuf); err != nil {
		return Entry{}, err
	}
	header := parseHeader(headerBuf)

	// Read payload
	payloadBuf := make([]byte, header.KeyLen+header.ValueLen)
	if err := readFull(f, payloadBuf); err != nil {
		return Entry{}, fmt.Errorf("failed to read payload: %w", err)
	}

	// Verify checksum
	if err := verifyChecksum(storedChecksum, headerBuf, payloadBuf); err != nil {
		return Entry{}, err
	}

	return buildEntry(header, payloadBuf), nil
}

func readChecksum(f *os.File) (uint32, error) {
	var checksum uint32
	err := binary.Read(f, binary.BigEndian, &checksum)
	if err == io.EOF {
		return 0, io.EOF
	}
	if err != nil {
		return 0, fmt.Errorf("failed to read checksum: %w", err)
	}
	return checksum, nil
}

func readFull(f *os.File, buf []byte) error {
	_, err := io.ReadFull(f, buf)
	if err == io.ErrUnexpectedEOF {
		return fmt.Errorf("corruption detected: incomplete entry")
	}
	return err
}

func parseHeader(buf []byte) Header {
	return Header{
		Flags:     binary.BigEndian.Uint32(buf[0:4]),
		Timestamp: binary.BigEndian.Uint64(buf[4:12]),
		KeyLen:    binary.BigEndian.Uint32(buf[12:16]),
		ValueLen:  binary.BigEndian.Uint32(buf[16:20]),
	}
}

func verifyChecksum(stored uint32, headerBuf, payloadBuf []byte) error {
	h := crc32.NewIEEE()
	h.Write(headerBuf)
	h.Write(payloadBuf)
	computed := h.Sum32()

	if computed != stored {
		return fmt.Errorf("corruption detected: stored %d != computed %d", stored, computed)
	}
	return nil
}

func buildEntry(header Header, payloadBuf []byte) Entry {
	key := string(payloadBuf[:header.KeyLen])
	value := payloadBuf[header.KeyLen:]

	op := "SET"
	if header.Flags&FlagDelete != 0 {
		op = "DELETE"
	}

	return Entry{
		Op:        op,
		Key:       key,
		Value:     value,
		Timestamp: header.Timestamp,
	}
}
