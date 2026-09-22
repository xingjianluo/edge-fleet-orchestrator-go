package edge

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/pkg/fleet"
)

// TestTornLogTailTruncation simulates a crash halfway through appending a
// record: log.jsonl ends with a partial JSON line. Reopening must truncate the
// torn tail, keep all previously fsynced records, and allow new appends with
// continuing seqs.
func TestTornLogTailTruncation(t *testing.T) {
	dir := t.TempDir()
	st := mustOpen(t, dir)
	if _, err := st.Append(tele("e1", "a")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.Append(tele("e2", "b")); err != nil {
		t.Fatal(err)
	}
	if err := st.Commit(1); err != nil {
		t.Fatal(err)
	}
	if err := st.Close(); err != nil {
		t.Fatal(err)
	}

	// Append a torn (incomplete) record line by hand, as a crash would leave.
	logPath := filepath.Join(dir, logFileName)
	good, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	torn := append(append([]byte(nil), good...), []byte(`{"seq":3,"entry":{"eventid`)...)
	if err := os.WriteFile(logPath, torn, 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("input=log.jsonl with %d good bytes + %d torn bytes -> reopen", len(good), len(torn)-len(good))

	st2 := mustOpen(t, dir)
	pending := st2.Pending()
	t.Logf("after reopen: checkpoint=%d pending=%d", st2.Checkpoint(), len(pending))
	if st2.Checkpoint() != 1 {
		t.Fatalf("checkpoint=%d want 1 (meta intact)", st2.Checkpoint())
	}
	if len(pending) != 1 || pending[0].EventID != "e2" {
		t.Fatalf("pending=%+v want just e2 after torn tail truncation", pending)
	}
	// New append continues seq allocation without collision.
	pe, err := st2.Append(tele("e3", "c"))
	if err != nil || pe.Seq != 3 {
		t.Fatalf("append after recovery: %+v %v", pe, err)
	}
	if _, err := st2.Append(tele("e1", "dup")); reason(err) != fleet.ReasonDuplicateID {
		t.Fatalf("seen-set replay err=%v want duplicate_id", err)
	}
	if err := st2.Close(); err != nil {
		t.Fatal(err)
	}
	t.Log("input=e1 again after restart -> duplicate_id (dedupe set rebuilt from log)")
}
