package storage

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"

	"github.com/MahyarNemati/raft-kv/internal/raft"
)

const (
	walFile      = "wal.log"
	snapshotFile = "snapshot.bin"

	recHardState byte = 1
	recEntry     byte = 2

	recHeaderSize = 9 // length(4) + crc(4) + type(1)
	maxRecordSize = 64 << 20

	snapMagic uint32 = 0x52534e50 // "RSNP"
)

var crcTable = crc32.MakeTable(crc32.Castagnoli)

// WALOptions configures a WAL store.
type WALOptions struct {
	// NoSync skips fsync. Only for benchmarks and tests.
	NoSync bool
	Logger *slog.Logger
}

// WAL persists Raft state in a directory:
//
//	wal.log       append-only records (hard state and entries), CRC-protected
//	snapshot.bin  latest snapshot, replaced atomically via rename
//
// An appended entry at index i implicitly discards any stored entries >= i,
// so log truncation needs no separate record. Taking a snapshot rewrites the
// WAL with only the retained suffix, which is how the log is compacted.
type WAL struct {
	dir  string
	opts WALOptions

	mu   sync.Mutex
	f    *os.File
	hard raft.HardState
	base uint64 // snapshot index
	last uint64 // last stored entry index
}

// OpenWAL opens or creates a WAL store in dir.
func OpenWAL(dir string, opts WALOptions) (*WAL, error) {
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("storage: create dir: %w", err)
	}
	return &WAL{dir: dir, opts: opts}, nil
}

// Load replays the snapshot and WAL. A torn or corrupt tail, the expected
// result of a crash mid-write, is truncated away.
func (w *WAL) Load() (raft.PersistentState, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	var st raft.PersistentState

	snap, err := readSnapshot(filepath.Join(w.dir, snapshotFile))
	if err != nil {
		return st, err
	}
	st.Snapshot = snap
	if snap != nil {
		w.base = snap.Meta.Index
	}

	path := filepath.Join(w.dir, walFile)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o640)
	if err != nil {
		return st, fmt.Errorf("storage: open wal: %w", err)
	}
	hard, entries, good, err := replay(f)
	if err != nil {
		_ = f.Close()
		return st, err
	}
	if info, statErr := f.Stat(); statErr == nil && info.Size() > good {
		w.opts.Logger.Warn("truncating torn wal tail", "path", path, "offset", good, "size", info.Size())
		if err := f.Truncate(good); err != nil {
			_ = f.Close()
			return st, fmt.Errorf("storage: truncate wal: %w", err)
		}
	}
	if _, err := f.Seek(good, io.SeekStart); err != nil {
		_ = f.Close()
		return st, fmt.Errorf("storage: seek wal: %w", err)
	}

	entries = trimToSnapshot(entries, snap)
	w.f = f
	w.hard = hard
	w.last = w.base + uint64(len(entries))
	st.Hard = hard
	st.Entries = entries
	return st, nil
}

// trimToSnapshot drops entries covered by the snapshot. If the WAL disagrees
// with the snapshot at its index (a crash between writing the snapshot and
// rewriting the WAL) the whole log is discarded, mirroring Raft's rule.
func trimToSnapshot(entries []raft.Entry, snap *raft.Snapshot) []raft.Entry {
	if snap == nil || len(entries) == 0 {
		return entries
	}
	idx := snap.Meta.Index
	for i, e := range entries {
		if e.Index == idx {
			if e.Term != snap.Meta.Term {
				return nil
			}
			return entries[i+1:]
		}
		if e.Index > idx {
			if i == 0 {
				return entries
			}
			return nil
		}
	}
	return nil
}

func replay(r io.Reader) (raft.HardState, []raft.Entry, int64, error) {
	var (
		hard    raft.HardState
		entries []raft.Entry
		offset  int64
	)
	br := bufio.NewReaderSize(r, 1<<20)
	hdr := make([]byte, recHeaderSize)
	for {
		if _, err := io.ReadFull(br, hdr); err != nil {
			return hard, entries, offset, nil
		}
		n := binary.LittleEndian.Uint32(hdr[0:4])
		sum := binary.LittleEndian.Uint32(hdr[4:8])
		typ := hdr[8]
		if n > maxRecordSize {
			return hard, entries, offset, nil
		}
		payload := make([]byte, n)
		if _, err := io.ReadFull(br, payload); err != nil {
			return hard, entries, offset, nil
		}
		c := crc32.Update(crc32.Checksum([]byte{typ}, crcTable), crcTable, payload)
		if c != sum {
			return hard, entries, offset, nil
		}
		switch typ {
		case recHardState:
			hs, err := decodeHardState(payload)
			if err != nil {
				return hard, entries, offset, nil
			}
			hard = hs
		case recEntry:
			e, err := decodeEntry(payload)
			if err != nil {
				return hard, entries, offset, nil
			}
			if len(entries) > 0 {
				first := entries[0].Index
				switch {
				case e.Index < first:
					entries = entries[:0]
				case e.Index <= entries[len(entries)-1].Index:
					entries = entries[:e.Index-first]
				case e.Index != entries[len(entries)-1].Index+1:
					return hard, nil, 0, fmt.Errorf("storage: wal gap at index %d", e.Index)
				}
			}
			entries = append(entries, e)
		default:
			return hard, entries, offset, nil
		}
		offset += int64(recHeaderSize) + int64(n)
	}
}

func (w *WAL) SetHardState(hs raft.HardState) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return errors.New("storage: wal not loaded")
	}
	if err := w.writeAndSync(appendRecord(nil, recHardState, encodeHardState(hs))); err != nil {
		return err
	}
	w.hard = hs
	return nil
}

func (w *WAL) Append(entries []raft.Entry) error {
	if len(entries) == 0 {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return errors.New("storage: wal not loaded")
	}
	first := entries[0].Index
	if first <= w.base || first > w.last+1 {
		return fmt.Errorf("storage: append at %d outside (%d, %d]", first, w.base, w.last+1)
	}
	var buf []byte
	for i := range entries {
		buf = appendRecord(buf, recEntry, encodeEntry(&entries[i]))
	}
	if err := w.writeAndSync(buf); err != nil {
		return err
	}
	w.last = entries[len(entries)-1].Index
	return nil
}

func (w *WAL) SaveSnapshot(snap raft.Snapshot, entries []raft.Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return errors.New("storage: wal not loaded")
	}
	if err := writeFileAtomic(w.dir, snapshotFile, encodeSnapshot(snap), !w.opts.NoSync); err != nil {
		return fmt.Errorf("storage: write snapshot: %w", err)
	}

	buf := appendRecord(nil, recHardState, encodeHardState(w.hard))
	for i := range entries {
		buf = appendRecord(buf, recEntry, encodeEntry(&entries[i]))
	}
	if err := writeFileAtomic(w.dir, walFile, buf, !w.opts.NoSync); err != nil {
		return fmt.Errorf("storage: rewrite wal: %w", err)
	}
	f, err := os.OpenFile(filepath.Join(w.dir, walFile), os.O_RDWR|os.O_APPEND, 0o640)
	if err != nil {
		return fmt.Errorf("storage: reopen wal: %w", err)
	}
	_ = w.f.Close()
	w.f = f
	w.base = snap.Meta.Index
	w.last = w.base + uint64(len(entries))
	return nil
}

func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.f == nil {
		return nil
	}
	err := w.f.Close()
	w.f = nil
	return err
}

func (w *WAL) writeAndSync(buf []byte) error {
	if _, err := w.f.Write(buf); err != nil {
		return fmt.Errorf("storage: write wal: %w", err)
	}
	if w.opts.NoSync {
		return nil
	}
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("storage: sync wal: %w", err)
	}
	return nil
}

func writeFileAtomic(dir, name string, data []byte, sync bool) error {
	tmp, err := os.CreateTemp(dir, name+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		return err
	}
	if sync {
		if err := tmp.Sync(); err != nil {
			_ = tmp.Close()
			cleanup()
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, filepath.Join(dir, name)); err != nil {
		cleanup()
		return err
	}
	if !sync {
		return nil
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}

// ---- encoding ----

func appendRecord(buf []byte, typ byte, payload []byte) []byte {
	var hdr [recHeaderSize]byte
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	c := crc32.Update(crc32.Checksum([]byte{typ}, crcTable), crcTable, payload)
	binary.LittleEndian.PutUint32(hdr[4:8], c)
	hdr[8] = typ
	buf = append(buf, hdr[:]...)
	return append(buf, payload...)
}

func encodeHardState(hs raft.HardState) []byte {
	b := binary.LittleEndian.AppendUint64(nil, hs.Term)
	b = binary.LittleEndian.AppendUint16(b, uint16(len(hs.VotedFor)))
	return append(b, hs.VotedFor...)
}

func decodeHardState(b []byte) (raft.HardState, error) {
	if len(b) < 10 {
		return raft.HardState{}, errors.New("short hard state")
	}
	n := int(binary.LittleEndian.Uint16(b[8:10]))
	if len(b) != 10+n {
		return raft.HardState{}, errors.New("bad hard state length")
	}
	return raft.HardState{Term: binary.LittleEndian.Uint64(b[0:8]), VotedFor: raft.NodeID(b[10:])}, nil
}

func encodeEntry(e *raft.Entry) []byte {
	b := make([]byte, 0, 17+len(e.Data))
	b = binary.LittleEndian.AppendUint64(b, e.Index)
	b = binary.LittleEndian.AppendUint64(b, e.Term)
	b = append(b, byte(e.Type))
	return append(b, e.Data...)
}

func decodeEntry(b []byte) (raft.Entry, error) {
	if len(b) < 17 {
		return raft.Entry{}, errors.New("short entry")
	}
	e := raft.Entry{
		Index: binary.LittleEndian.Uint64(b[0:8]),
		Term:  binary.LittleEndian.Uint64(b[8:16]),
		Type:  raft.EntryType(b[16]),
	}
	if len(b) > 17 {
		e.Data = append([]byte(nil), b[17:]...)
	}
	return e, nil
}

func encodeSnapshot(s raft.Snapshot) []byte {
	b := make([]byte, 0, 24+len(s.Data))
	b = binary.LittleEndian.AppendUint32(b, snapMagic)
	b = binary.LittleEndian.AppendUint64(b, s.Meta.Index)
	b = binary.LittleEndian.AppendUint64(b, s.Meta.Term)
	b = binary.LittleEndian.AppendUint32(b, crc32.Checksum(s.Data, crcTable))
	return append(b, s.Data...)
}

func readSnapshot(path string) (*raft.Snapshot, error) {
	b, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("storage: read snapshot: %w", err)
	}
	if len(b) < 24 || binary.LittleEndian.Uint32(b[0:4]) != snapMagic {
		return nil, fmt.Errorf("storage: snapshot %s is malformed", path)
	}
	data := b[24:]
	if crc32.Checksum(data, crcTable) != binary.LittleEndian.Uint32(b[20:24]) {
		return nil, fmt.Errorf("storage: snapshot %s failed checksum", path)
	}
	return &raft.Snapshot{
		Meta: raft.SnapshotMeta{
			Index: binary.LittleEndian.Uint64(b[4:12]),
			Term:  binary.LittleEndian.Uint64(b[12:20]),
		},
		Data: data,
	}, nil
}
