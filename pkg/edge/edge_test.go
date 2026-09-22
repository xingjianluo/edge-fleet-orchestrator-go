package edge

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/pkg/cloud"
	"github.com/xingjianluo/edge-fleet-orchestrator-go/pkg/fleet"
)

func reason(err error) fleet.RejectReason {
	if err == nil {
		return fleet.ReasonOK
	}
	if re, ok := err.(*fleet.RejectError); ok {
		return re.Reason
	}
	return "non_structured_error"
}

func tele(id, payload string) fleet.Entry {
	return fleet.Entry{EventID: id, Kind: fleet.KindTelemetry, Telemetry: payload}
}

func state(id, key, value string, base int64) fleet.Entry {
	return fleet.Entry{EventID: id, Kind: fleet.KindState, StateKey: key, StateValue: value, BaseRev: base}
}

func newCloudDevice(t *testing.T) (*cloud.ControlPlane, fleet.Device) {
	t.Helper()
	cp := cloud.New()
	cp.RegisterTenant("t", cloud.Quota{})
	v, _ := cloud.ParseVersion("1.0.0")
	dev := fleet.Device{
		Tenant: "t", Cluster: "c", ID: "dev",
		Capabilities: fleet.Capabilities{MinPlatform: v, Features: map[string]bool{"gps": true}},
	}
	if err := cp.RegisterDevice(dev); err != nil {
		t.Fatal(err)
	}
	return cp, dev
}

func mustOpen(t *testing.T, dir string) *Store {
	t.Helper()
	s, err := OpenStore(dir)
	if err != nil {
		t.Fatalf("OpenStore(%s): %v", dir, err)
	}
	return s
}

// TestOfflineBufferAndResume: entries produced offline stay pending; after
// reconnect one flush delivers them in log order exactly once from the
// checkpoint, and a second flush sends nothing.
func TestOfflineBufferAndResume(t *testing.T) {
	cp, dev := newCloudDevice(t)
	dir := t.TempDir()
	st := mustOpen(t, dir)
	link := NewCloudLink(cp, "t")
	gw := NewGateway(dev, st, link)

	// Offline production.
	for _, e := range []fleet.Entry{tele("e1", "a"), tele("e2", "b"), state("e3", "mode", "on", 0)} {
		if err := gw.Produce(e); err != nil {
			t.Fatalf("offline produce %s: %v", e.EventID, err)
		}
	}
	if n, _ := gw.Pending(); n != 3 {
		t.Fatalf("pending offline=%d want 3", n)
	}
	t.Log("input=3 entries while offline -> pending=3, produce succeeded (durable local buffer)")

	// Flushing while offline is refused and nothing is sent.
	if _, err := gw.Flush(); err != ErrLinkDown {
		t.Fatalf("offline flush err=%v want ErrLinkDown", err)
	}
	if len(link.AttemptLog()) != 0 {
		t.Fatal("link must not be used while offline")
	}

	// Reconnect: Online(true) flushes from checkpoint 0.
	link.SetUp(true)
	if err := gw.Online(true); err != nil {
		t.Fatalf("online flush: %v", err)
	}
	if n, _ := gw.Pending(); n != 0 {
		t.Fatalf("after reconnect pending=%d want 0", n)
	}
	sh, _ := cp.Shadow("t", "dev")
	t.Logf("input=reconnect -> telemetry=%v fields=%v rev=%d", sh.Telemetry, sh.Fields, sh.Rev)
	if fmt.Sprint(sh.Telemetry) != "[a b]" || sh.Fields["mode"] != "on" {
		t.Fatalf("cloud shadow wrong: %+v", sh)
	}

	// Second flush must be a no-op: no sends at all.
	if err := gw.Online(false); err != nil {
		t.Fatal(err)
	}
	if err := gw.Online(true); err != nil {
		t.Fatal(err)
	}
	if n := len(link.AttemptLog()); n != 3 {
		t.Fatalf("attempt count=%d want 3 (checkpoint prevents retransmission)", n)
	}
	t.Log("input=second reconnect flush -> 0 sends (resume position committed)")
}

// TestProducerDuplicateRejected: the same EventID produced twice is rejected
// locally with ReasonDuplicateID before the network is involved, so retries
// never double-buffer or double-apply.
func TestProducerDuplicateRejected(t *testing.T) {
	cp, dev := newCloudDevice(t)
	st := mustOpen(t, t.TempDir())
	link := NewCloudLink(cp, "t")
	gw := NewGateway(dev, st, link)
	link.SetUp(true)
	if err := gw.Online(true); err != nil {
		t.Fatal(err)
	}
	if err := gw.Produce(tele("dup", "x")); err != nil {
		t.Fatal(err)
	}
	err := gw.Produce(tele("dup", "x-different-payload"))
	t.Logf("input=produce same EventID twice -> reason=%q detail=%v", reason(err), err)
	if reason(err) != fleet.ReasonDuplicateID {
		t.Fatalf("got %v want duplicate_id", err)
	}
	sh, _ := cp.Shadow("t", "dev")
	if len(sh.Telemetry) != 1 || sh.Telemetry[0] != "x" {
		t.Fatalf("telemetry=%v want [x] (first write wins, retry ignored)", sh.Telemetry)
	}
	if n := len(link.AttemptLog()); n != 1 {
		t.Fatalf("link attempts=%d want 1", n)
	}
}

// TestTransientFailureResumesAtCheckpoint: first send attempt fails transiently
// (jitter); no checkpoint may advance. On retry the entry is delivered once,
// and cloud-side duplicate handling makes even an external retry safe.
func TestTransientFailureResumesAtCheckpoint(t *testing.T) {
	cp, dev := newCloudDevice(t)
	st := mustOpen(t, t.TempDir())
	link := NewCloudLink(cp, "t")
	// Event e1 fails its first attempt, succeeds on retry.
	link.SetFailer(func(_ fleet.Device, e fleet.Entry, attempt int) bool {
		return e.EventID == "e1" && attempt == 0
	})
	gw := NewGateway(dev, st, link)
	link.SetUp(true)

	// Start online via gateway state; SetUp then produce.
	gw.online = true
	// e1's first send fails transiently: the error surfaces, but the entry
	// remains buffered at checkpoint 0 (nothing lost).
	err1 := gw.Produce(tele("e1", "v1"))
	t.Logf("input=produce e1 with injected first-attempt failure -> surfaced err=%v, checkpoint=%d",
		err1, gw.Checkpoint())
	if err1 == nil {
		t.Fatal("transient failure must surface to the online producer")
	}
	if gw.Checkpoint() != 0 {
		t.Fatalf("checkpoint=%d must stay 0 after a failed (unacked) send", gw.Checkpoint())
	}
	if n, _ := gw.Pending(); n != 1 {
		t.Fatalf("pending=%d want 1 (entry retained for retry)", n)
	}
	// e2's flush retries pending e1 first (now succeeds), then sends e2.
	if err := gw.Produce(tele("e2", "v2")); err != nil {
		t.Fatalf("e2/retry flush: %v", err)
	}
	sh, _ := cp.Shadow("t", "dev")
	t.Logf("cloud after flaky first send: telemetry=%v attempts=%d checkpoint=%d",
		sh.Telemetry, len(link.AttemptLog()), gw.Checkpoint())
	if fmt.Sprint(sh.Telemetry) != "[v1 v2]" {
		t.Fatalf("telemetry=%v want [v1 v2]", sh.Telemetry)
	}
	if gw.Checkpoint() != 2 {
		t.Fatalf("checkpoint=%d want 2", gw.Checkpoint())
	}
	// Exactly: e1 attempted twice (one fail), e2 once.
	var e1Tries int
	for _, a := range link.AttemptLog() {
		if a.EventID == "e1" {
			e1Tries++
		}
	}
	if e1Tries != 2 {
		t.Fatalf("e1 attempts=%d want 2 (transient retry)", e1Tries)
	}
}

// TestRestartRecovery simulates a gateway process restart: new Store and new
// Gateway over the same directory must resume from the persisted checkpoint
// with no loss and no double application.
func TestRestartRecovery(t *testing.T) {
	cp, dev := newCloudDevice(t)
	dir := t.TempDir()
	link := NewCloudLink(cp, "t")

	// Phase 1: offline buffer e1,e2; reconnect; e1,e2 commit; then offline
	// buffer e3,e4 and crash (no reconnect).
	{
		st := mustOpen(t, dir)
		gw := NewGateway(dev, st, link)
		for _, e := range []fleet.Entry{tele("e1", "1"), tele("e2", "2")} {
			if err := gw.Produce(e); err != nil {
				t.Fatal(err)
			}
		}
		link.SetUp(true)
		if err := gw.Online(true); err != nil {
			t.Fatal(err)
		}
		link.SetUp(false)
		if err := gw.Online(false); err != nil {
			t.Fatal(err)
		}
		for _, e := range []fleet.Entry{tele("e3", "3"), tele("e4", "4")} {
			if err := gw.Produce(e); err != nil {
				t.Fatal(err)
			}
		}
		if err := gw.Close(); err != nil {
			t.Fatal(err)
		}
		t.Log("phase 1: e1,e2 synced; e3,e4 buffered; gateway process exits")
	}

	// Phase 2: restart. Store replay must show exactly e3,e4 pending.
	{
		st := mustOpen(t, dir)
		gw := NewGateway(dev, st, link)
		if n, _ := gw.Pending(); n != 2 {
			t.Fatalf("after restart pending=%d want 2", n)
		}
		if cp := gw.Checkpoint(); cp != 2 {
			t.Fatalf("after restart checkpoint=%d want 2", cp)
		}
		t.Logf("phase 2: restarted gateway replays log -> pending=2 checkpoint=2 (e1,e2 not resent)")
		link.SetUp(true)
		if err := gw.Online(true); err != nil {
			t.Fatal(err)
		}
		if n, _ := gw.Pending(); n != 0 {
			t.Fatalf("after restart flush pending=%d want 0", n)
		}
		if err := gw.Close(); err != nil {
			t.Fatal(err)
		}
	}
	sh, _ := cp.Shadow("t", "dev")
	t.Logf("cloud telemetry after restart+resume: %v (rev=%d)", sh.Telemetry, sh.Rev)
	if fmt.Sprint(sh.Telemetry) != "[1 2 3 4]" {
		t.Fatalf("telemetry=%v want [1 2 3 4]: loss or duplication on restart", sh.Telemetry)
	}
	if sh.Rev != 4 {
		t.Fatalf("rev=%d want 4 (each event applied once across restart)", sh.Rev)
	}
}

// TestRestartDuringTransientRetry: cloud applies an entry but the gateway
// crashes before committing (simulated by committing nothing while cloud got
// it). On restart it resends; cloud dedup by EventID makes delivery exactly
// once effective.
func TestRestartDuringTransientRetry(t *testing.T) {
	cp, dev := newCloudDevice(t)
	dir := t.TempDir()
	// A link that applies at the cloud but reports transient failure once.
	var delivered map[string]bool
	_ = delivered
	link := NewSimLink(func(d fleet.Device, e fleet.Entry) (fleet.Ack, error) {
		// Every attempt genuinely reaches and applies (or dedups) at cloud.
		return cp.Ingest("t", d.ID, e)
	})
	link.SetFailer(func(_ fleet.Device, e fleet.Entry, attempt int) bool {
		return e.EventID == "e1" && attempt == 0 // lose the ack once
	})
	link.SetUp(true)
	{
		st := mustOpen(t, dir)
		gw := NewGateway(dev, st, link)
		gw.online = true
		_ = gw.Produce(tele("e1", "x")) // e1 applied at cloud, ack lost -> no commit
		_ = gw.Close()
		t.Log("phase 1: cloud applied e1 but ack was lost; gateway checkpoint stays 0, process exits")
	}
	{
		// Restart with a stable link: e1 resent; cloud must report duplicate.
		link.SetFailer(nil)
		st := mustOpen(t, dir)
		gw := NewGateway(dev, st, link)
		if gw.Checkpoint() != 0 {
			t.Fatalf("checkpoint=%d want 0 (commit never happened)", gw.Checkpoint())
		}
		link.SetUp(true)
		if err := gw.Online(true); err != nil {
			t.Fatal(err)
		}
		_ = gw.Close()
	}
	sh, _ := cp.Shadow("t", "dev")
	t.Logf("cloud telemetry after lost-ack restart: %v rev=%d", sh.Telemetry, sh.Rev)
	if len(sh.Telemetry) != 1 || sh.Telemetry[0] != "x" {
		t.Fatalf("telemetry=%v want [x] (at-least-once transport + cloud idempotency = exactly-once effect)", sh.Telemetry)
	}
}

// TestConcurrentProducers: many goroutines produce unique and duplicate
// EventIDs offline then online; every unique event is delivered exactly once
// and duplicates are rejected locally.
func TestConcurrentProducers(t *testing.T) {
	cp, dev := newCloudDevice(t)
	st := mustOpen(t, t.TempDir())
	link := NewCloudLink(cp, "t")
	gw := NewGateway(dev, st, link)
	link.SetUp(true)
	gw.online = true

	const unique = 40
	var wg sync.WaitGroup
	var mu sync.Mutex
	var dupRejects int
	produceOne := func(i int) {
		defer wg.Done()
		err := gw.Produce(tele(fmt.Sprintf("u%02d", i), fmt.Sprintf("p%d", i)))
		if err == nil {
			return
		}
		if reason(err) == fleet.ReasonDuplicateID {
			mu.Lock()
			dupRejects++
			mu.Unlock()
			return
		}
		t.Errorf("produce u%02d unexpected err: %v", i, err)
	}
	for i := 0; i < unique; i++ {
		wg.Add(2)
		go produceOne(i)
		go produceOne(i)
	}
	wg.Wait()
	if _, err := gw.Flush(); err != nil {
		t.Fatalf("final flush: %v", err)
	}
	sh, _ := cp.Shadow("t", "dev")
	t.Logf("input=40 unique events each produced twice concurrently -> unique applied=%d dupRejects=%d",
		len(sh.Telemetry), dupRejects)
	if len(sh.Telemetry) != unique {
		t.Fatalf("telemetry count=%d want %d (no loss, no duplication)", len(sh.Telemetry), unique)
	}
	if dupRejects != unique {
		t.Fatalf("duplicate rejections=%d want %d (exactly one loser per id pair)", dupRejects, unique)
	}
}

// TestEndToEndDeployAndSync wires cloud + deploy + edge: dispatch versions to
// a fleet, have the gateway carry state updates through an outage window, and
// verify the reported state deterministically merges including a conflict.
func TestEndToEndDeployAndSync(t *testing.T) {
	cp := cloud.New()
	cp.RegisterTenant("t", cloud.Quota{})
	v, _ := cloud.ParseVersion("2.0.0")
	dev := fleet.Device{Tenant: "t", Cluster: "c", ID: "dev",
		Capabilities: fleet.Capabilities{MinPlatform: v, Features: map[string]bool{"gps": true}}}
	if err := cp.RegisterDevice(dev); err != nil {
		t.Fatal(err)
	}
	mgr := deployMgrForTest(cp)
	ver := func(s string) fleet.Version {
		vv, err := cloud.ParseVersion(s)
		if err != nil {
			t.Fatal(err)
		}
		return vv
	}
	r1, err := mgr.Dispatch("t", "dev", fleet.DeploymentSpec{
		Version: ver("1.0.0"), MinPlatform: ver("1.0.0"), Payload: "img1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := mgr.Dispatch("t", "dev", fleet.DeploymentSpec{
		Version: ver("2.0.0"), MinPlatform: ver("1.0.0"), Payload: "img2",
	}); err != nil {
		t.Fatal(err)
	}
	// Rollback while the edge is offline: edge buffers state; after resume
	// the cloud shadow reflects the merge.
	dir := filepath.Join(t.TempDir(), "gw")
	st := mustOpen(t, dir)
	link := NewCloudLink(cp, "t")
	gw := NewGateway(dev, st, link)
	_ = gw.Produce(state("s1", "cfg", "edge-edited", 1))
	rb, err := mgr.Rollback("t", "dev", r1.Seq)
	if err != nil || rb.Status != "rolled_back" {
		t.Fatalf("rollback: %+v %v", rb, err)
	}
	cur, _ := mgr.Current("t", "dev")
	t.Logf("deploy: current restored to %s payload=%s", cur.Spec.Version, cur.Spec.Payload)

	// Conflict while offline: two devices' edits to the same key with same
	// base rev; deterministic tie -> smaller EventID. Here the gateway carries
	// a competing update relative to one applied directly at the cloud.
	if _, err := cp.Ingest("t", "dev", state("a-cloud", "cfg", "cloud-edit", 1)); err != nil {
		t.Fatal(err)
	}
	link.SetUp(true)
	if err := gw.Online(true); err != nil {
		t.Fatal(err)
	}
	sh, _ := cp.Shadow("t", "dev")
	t.Logf("end-to-end shadow: fields=%v rev=%d; rule tie(baseRev 1) -> min EventID: a-cloud < s1",
		sh.Fields, sh.Rev)
	if sh.Fields["cfg"] != "cloud-edit" {
		t.Fatalf("cfg=%q want cloud-edit (deterministic conflict, not silent last-write-wins)", sh.Fields["cfg"])
	}
}
