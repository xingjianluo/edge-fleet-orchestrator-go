// Package edge simulates an edge gateway: it durably buffers telemetry and
// state changes while offline in a local log, resumes from a checkpoint after
// reconnect (or process restart), and deduplicates producer retries.
package edge

import (
	"bufio"
	"encoding/json"
	"io"
	"os"
	"path/filepath"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/pkg/fleet"
)

const (
	logFileName  = "log.jsonl"
	metaFileName = "meta.json"
	tmpSuffix    = ".tmp"
)

// Store is the durable per-gateway outgoing log.
//
// On-disk layout in dir:
//   - log.jsonl: append-only entries, one JSON record per line, never
//     rewritten. A torn final line (crash mid-write) is truncated on open;
//     committed prefixes always survive.
//   - meta.json: checkpoint + assigned seq counter + seen producer ids,
//     written atomically (temp file + fsync + rename + dir fsync) whenever
//     the checkpoint advances.
//
// Invariant: an entry is acknowledged to its producer only after it has been
// fsynced into log.jsonl; the checkpoint is advanced only after the cloud has
// applied it. A crash at any point therefore never loses an unacknowledged
// entry and never re-applies a committed one.
type Store struct {
	dir     string
	logF    *os.File
	records []logRecord // complete replay of log.jsonl
	seen    map[string]int64
	nextSeq int64
	commit  int64
}

// StoredEntry pairs the assigned log seq with an entry.
type StoredEntry struct {
	Seq int64
	fleet.Entry
}

// OpenStore opens (or creates) the durable log in dir and replays it so a
// restarted gateway resumes exactly where it stopped.
func OpenStore(dir string) (*Store, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	s := &Store{dir: dir, seen: map[string]int64{}}

	logPath := filepath.Join(dir, logFileName)
	f, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return nil, err
	}
	s.logF = f

	if err := s.replayLog(); err != nil {
		return nil, err
	}
	if err := s.loadMeta(); err != nil {
		return nil, err
	}
	// Checkpoint cannot reference a seq that the log does not contain.
	if s.commit > s.nextSeq {
		s.commit = s.nextSeq
	}
	return s, nil
}

// replayLog reads every complete JSON line, rebuilding the record slice and
// seen set. A torn final line (non-JSON/incomplete write from a crash) is
// physically truncated so later appends stay valid.
func (s *Store) replayLog() error {
	rest, err := os.ReadFile(filepath.Join(s.dir, logFileName))
	if err != nil {
		return err
	}
	validLen := 0
	originalLen := len(rest)
	for len(rest) > 0 {
		nl := indexByte(rest, '\n')
		var line []byte
		atEOF := false
		if nl < 0 {
			line, nl, atEOF = rest, len(rest), true
		} else {
			line = rest[:nl]
		}
		var rec logRecord
		if jerr := json.Unmarshal(line, &rec); jerr != nil {
			break // torn tail: stop at the valid prefix
		}
		s.records = append(s.records, rec)
		s.seen[rec.Entry.EventID] = rec.Seq
		if rec.Seq > s.nextSeq {
			s.nextSeq = rec.Seq
		}
		validLen += nl + 1
		if atEOF {
			break
		}
		rest = rest[nl+1:]
	}
	if validLen < originalLen {
		return os.Truncate(filepath.Join(s.dir, logFileName), int64(validLen))
	}
	return nil
}

func indexByte(b []byte, c byte) int {
	for i, x := range b {
		if x == c {
			return i
		}
	}
	return -1
}

// loadMeta reads meta.json if present; otherwise a fresh meta is written.
func (s *Store) loadMeta() error {
	path := filepath.Join(s.dir, metaFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			s.commit = 0
			return s.writeMeta()
		}
		return err
	}
	var m metaRecord
	if err := json.Unmarshal(data, &m); err != nil {
		return err
	}
	s.commit = m.Checkpoint
	if m.NextSeq > s.nextSeq {
		s.nextSeq = m.NextSeq
	}
	// Trust the log's own seen set (source of truth); meta seen is only a
	// diagnostic cross-check.
	return nil
}

// writeMeta atomically replaces meta.json and fsyncs the directory.
func (s *Store) writeMeta() error {
	m := metaRecord{
		Checkpoint: s.commit,
		NextSeq:    s.nextSeq,
		Seen:       s.seen,
	}
	data, err := json.MarshalIndent(&m, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	tmp := filepath.Join(s.dir, metaFileName+tmpSuffix)
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	if err := fsyncPath(tmp); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(s.dir, metaFileName)); err != nil {
		return err
	}
	return fsyncDir(s.dir)
}

// Append durably appends an entry. A repeated producer EventID is rejected
// locally with ReasonDuplicateID so producer retries never buffer twice.
func (s *Store) Append(e fleet.Entry) (StoredEntry, error) {
	if e.EventID == "" {
		return StoredEntry{}, fleet.NewError(fleet.ReasonInvalidVersion, "entry EventID must not be empty")
	}
	if _, dup := s.seen[e.EventID]; dup {
		return StoredEntry{}, fleet.NewError(fleet.ReasonDuplicateID, "event %q already buffered", e.EventID)
	}
	s.nextSeq++
	rec := logRecord{Seq: s.nextSeq, Entry: e}
	line, err := json.Marshal(&rec)
	if err != nil {
		return StoredEntry{}, err
	}
	line = append(line, '\n')
	w := bufio.NewWriter(s.logF)
	if _, err := w.Write(line); err != nil {
		return StoredEntry{}, err
	}
	if err := w.Flush(); err != nil {
		return StoredEntry{}, err
	}
	if err := s.logF.Sync(); err != nil {
		return StoredEntry{}, err
	}
	s.records = append(s.records, rec)
	s.seen[e.EventID] = rec.Seq
	return StoredEntry{Seq: rec.Seq, Entry: e}, nil
}

// Pending returns entries with seq strictly greater than the checkpoint, in
// log order. "Greater than checkpoint" is the resume position.
func (s *Store) Pending() []StoredEntry {
	out := make([]StoredEntry, 0)
	for _, rec := range s.records {
		if rec.Seq > s.commit {
			out = append(out, StoredEntry{Seq: rec.Seq, Entry: rec.Entry})
		}
	}
	return out
}

// Commit advances the checkpoint to seq and durably persists it. seq may only
// move forward; committing earlier seqs is ignored.
func (s *Store) Commit(seq int64) error {
	if seq <= s.commit {
		return nil
	}
	if seq > s.nextSeq {
		return io.ErrUnexpectedEOF // cannot commit past the log tail
	}
	s.commit = seq
	return s.writeMeta()
}

// Checkpoint returns the current resume position.
func (s *Store) Checkpoint() int64 { return s.commit }

// Close flushes and closes the log file.
func (s *Store) Close() error {
	if s.logF != nil {
		if err := s.logF.Sync(); err != nil {
			return err
		}
		return s.logF.Close()
	}
	return nil
}

// ---- on-disk formats ----

type logRecord struct {
	Seq   int64       `json:"seq"`
	Entry fleet.Entry `json:"entry"`
}

type metaRecord struct {
	Checkpoint int64            `json:"checkpoint"`
	NextSeq    int64            `json:"next_seq"`
	Seen       map[string]int64 `json:"seen"`
}

func fsyncPath(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func fsyncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
}
