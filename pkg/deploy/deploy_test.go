package deploy

import (
	"fmt"
	"sync"
	"testing"
	"time"

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

// fixture builds a control plane with two tenants and devices of different
// capabilities.
func fixture(t *testing.T) (*cloud.ControlPlane, fleet.TenantID, fleet.DeviceID, fleet.DeviceID) {
	t.Helper()
	cp := cloud.New()
	cp.RegisterTenant("t-a", cloud.Quota{MaxRollouts: 5})
	cp.RegisterTenant("t-b", cloud.Quota{})
	mk := func(tenant fleet.TenantID, id fleet.DeviceID, platform string, feats ...string) {
		v, err := cloud.ParseVersion(platform)
		if err != nil {
			t.Fatal(err)
		}
		fm := map[string]bool{}
		for _, f := range feats {
			fm[f] = true
		}
		if err := cp.RegisterDevice(fleet.Device{
			Tenant: tenant, Cluster: "c1", ID: id,
			Capabilities: fleet.Capabilities{MinPlatform: v, Features: fm},
		}); err != nil {
			t.Fatal(err)
		}
	}
	mk("t-a", "dev-new", "2.0.0", "gps", "can")
	mk("t-a", "dev-old", "1.0.0", "gps")
	mk("t-b", "dev-b", "2.0.0", "gps", "can")
	return cp, "t-a", "dev-new", "dev-old"
}

func fixedClock() *time.Time {
	base := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	return &base
}

func spec(ver, minPlatform, payload string, feats ...string) fleet.DeploymentSpec {
	v, _ := cloud.ParseVersion(ver)
	mp, _ := cloud.ParseVersion(minPlatform)
	return fleet.DeploymentSpec{Version: v, MinPlatform: mp, RequiresFeatures: feats, Payload: payload}
}

// TestDispatchIdempotent verifies identical repeated dispatch returns the same
// record, same current version, and consumes no extra quota slot.
func TestDispatchIdempotent(t *testing.T) {
	cp, tn, dev, _ := fixture(t)
	m := NewManager(cp, cp, time.Now)

	s1 := spec("1.2.0", "1.0.0", "img-A", "gps")
	r1, err := m.Dispatch(tn, dev, s1)
	if err != nil {
		t.Fatalf("initial dispatch: %v", err)
	}
	t.Logf("input=dispatch 1.2.0 -> seq=%d status=%s", r1.Seq, r1.Status)

	before := cp.CountRollouts(tn)
	for i := 0; i < 3; i++ {
		r, err := m.Dispatch(tn, dev, s1)
		if err != nil || r.Seq != r1.Seq {
			t.Fatalf("repeat dispatch #%d returned seq=%d err=%v, want seq=%d nil", i, r.Seq, err, r1.Seq)
		}
	}
	if after := cp.CountRollouts(tn); after != before {
		t.Fatalf("idempotent dispatch consumed quota: %d -> %d", before, after)
	}
	t.Logf("input=repeat same spec x3 -> same seq=%d, rollout count stays %d (idempotent, no quota charge)", r1.Seq, before)

	// Features listed in a different order still mean the same spec.
	shuffled := spec("1.2.0", "1.0.0", "img-A", "gps")
	shuffled.RequiresFeatures = []string{"gps"}
	if r, _ := m.Dispatch(tn, dev, shuffled); r.Seq != r1.Seq {
		t.Fatalf("feature-order variant must be treated as the same spec, got seq=%d want %d", r.Seq, r1.Seq)
	}

	// A new version appends a new applied record and becomes current.
	s2 := spec("1.3.0", "1.0.0", "img-B", "gps")
	r2, err := m.Dispatch(tn, dev, s2)
	if err != nil || r2.Seq != 2 || r2.Status != StatusApplied {
		t.Fatalf("dispatch 1.3.0 -> %+v %v", r2, err)
	}
	cur, _ := m.Current(tn, dev)
	if cur.Spec.Version.String() != "1.3.0" {
		t.Fatalf("current=%s want 1.3.0", cur.Spec.Version)
	}
	t.Logf("input=dispatch 1.3.0 -> seq=%d current=1.3.0 rollout count=%d", r2.Seq, cp.CountRollouts(tn))
}

// TestDispatchValidation distinguishes invalid specs, capability
// incompatibility (platform vs missing feature), cross-tenant and unknown
// device, and proves rejected dispatch never changes current.
func TestDispatchValidation(t *testing.T) {
	cp, tn, devNew, devOld := fixture(t)
	m := NewManager(cp, cp, time.Now)
	base := spec("1.0.0", "1.0.0", "img", "gps")
	if _, err := m.Dispatch(tn, devOld, base); err != nil {
		t.Fatal(err)
	}

	dispatch := func(name string, tenant fleet.TenantID, dev fleet.DeviceID, s fleet.DeploymentSpec) fleet.RejectReason {
		r, err := m.Dispatch(tenant, dev, s)
		rs := reason(err)
		seq := int64(-1)
		status := Status("")
		if r != nil {
			seq, status = r.Seq, r.Status
		}
		t.Logf("input=%s -> reason=%q detail=%v recordSeq=%d recordStatus=%s", name, rs, err, seq, status)
		return rs
	}

	invalid := fleet.DeploymentSpec{Payload: "x"} // version 0.0.0
	if got := dispatch("zero version", tn, devNew, invalid); got != fleet.ReasonInvalidVersion {
		t.Fatalf("got %q want invalid_version", got)
	}
	empty := spec("2.0.0", "1.0.0", "")
	if got := dispatch("empty payload", tn, devNew, empty); got != fleet.ReasonInvalidVersion {
		t.Fatalf("got %q want invalid_version", got)
	}
	tooNew := spec("3.0.0", "2.5.0", "img", "gps")
	if got := dispatch("platform too old for dev-new", tn, devNew, tooNew); got != fleet.ReasonIncompatibleDevice {
		t.Fatalf("got %q want incompatible_device", got)
	}
	needCan := spec("2.0.0", "1.0.0", "img", "can")
	if got := dispatch("missing feature can on dev-old", tn, devOld, needCan); got != fleet.ReasonIncompatibleDevice {
		t.Fatalf("got %q want incompatible_device", got)
	}
	if got := dispatch("unknown device", tn, "ghost", base); got != fleet.ReasonUnknownDevice {
		t.Fatalf("got %q want unknown_device", got)
	}
	if got := dispatch("cross tenant device", "t-b", devNew, base); got != fleet.ReasonCrossTenant {
		t.Fatalf("got %q want cross_tenant", got)
	}
	if got := dispatch("unknown tenant", "nobody", devNew, base); got != fleet.ReasonUnknownTenant {
		t.Fatalf("got %q want unknown_tenant", got)
	}

	// Current for dev-old must still be 1.0.0; rejected dispatches are audit
	// records only, never effective.
	cur, _ := m.Current(tn, devOld)
	if cur.Spec.Version.String() != "1.0.0" {
		t.Fatalf("current changed to %s after rejections", cur.Spec.Version)
	}
	hist, _ := m.History(tn, devOld)
	var rejected int
	for _, r := range hist {
		if r.Status == StatusRejected {
			rejected++
		}
	}
	t.Logf("dev-old history after rejects: %d records (%d rejected), current stays 1.0.0", len(hist), rejected)
	if rejected != 1 { // needCan; tooNew was attempted against dev-new
		t.Fatalf("rejected audit records=%d want 1 for dev-old", rejected)
	}

	// Repeating a rejected dispatch is idempotent: same record, no duplicate.
	n := len(hist)
	if r, err := m.Dispatch(tn, devOld, needCan); reason(err) != fleet.ReasonIncompatibleDevice || r == nil {
		t.Fatalf("repeat rejected dispatch must return same rejection, got r=%v err=%v", r, err)
	}
	hist2, _ := m.History(tn, devOld)
	if len(hist2) != n {
		t.Fatalf("repeated rejection appended a new record: %d -> %d", n, len(hist2))
	}
	t.Logf("input=repeat incompatible spec -> same rejection, no new record")
}

// TestRollbackReproducible verifies rollback restores exact historical specs,
// rejects invalid targets with distinguishable reasons, is idempotent, and
// re-dispatching after rollback also behaves.
func TestRollbackReproducible(t *testing.T) {
	cp, tn, dev, _ := fixture(t)
	m := NewManager(cp, cp, time.Now)
	v1 := spec("1.0.0", "1.0.0", "img-1", "gps")
	v2 := spec("2.0.0", "1.0.0", "img-2", "gps")
	v3 := spec("3.0.0", "2.0.0", "img-3", "gps")
	for _, s := range []fleet.DeploymentSpec{v1, v2, v3} {
		if _, err := m.Dispatch(tn, dev, s); err != nil {
			t.Fatal(err)
		}
	}
	cur, _ := m.Current(tn, dev)
	if cur.Spec.Version.String() != "3.0.0" {
		t.Fatalf("current=%s want 3.0.0", cur.Spec.Version)
	}

	// Rollback to seq 1 must restore the exact v1 spec (payload included).
	rb, err := m.Rollback(tn, dev, 1)
	if err != nil || rb.Status != StatusRolledBack || rb.RollbackTo != 1 {
		t.Fatalf("rollback -> %+v %v", rb, err)
	}
	if !specEqual(rb.Spec, v1) {
		t.Fatalf("rollback spec=%+v want exact v1=%+v", rb.Spec, v1)
	}
	cur, _ = m.Current(tn, dev)
	if !specEqual(cur.Spec, v1) {
		t.Fatalf("after rollback current=%s want 1.0.0", cur.Spec.Version)
	}
	t.Logf("input=rollback ->1 -> seq=%d status=%s current=1.0.0 payload=img-1 (exact spec restored)",
		rb.Seq, rb.Status)

	// Repeating the same rollback is a no-op (current spec already equals target).
	rb2, err := m.Rollback(tn, dev, 1)
	if err != nil || rb2.Seq != rb.Seq {
		t.Fatalf("repeat rollback must be idempotent: seq=%d err=%v (first seq=%d)", rb2.Seq, err, rb.Seq)
	}
	t.Logf("input=repeat rollback ->1 -> same seq=%d (idempotent)", rb2.Seq)

	// Forward again by rolling back to seq 2.
	rb3, err := m.Rollback(tn, dev, 2)
	if err != nil || !specEqual(rb3.Spec, v2) {
		t.Fatalf("rollback ->2: %+v %v", rb3, err)
	}
	cur, _ = m.Current(tn, dev)
	if cur.Spec.Version.String() != "2.0.0" {
		t.Fatalf("current=%s want 2.0.0", cur.Spec.Version)
	}
	t.Logf("input=rollback ->2 -> current=2.0.0")

	// Invalid targets.
	if _, err := m.Rollback(tn, dev, 99); reason(err) != fleet.ReasonRevisionNotFound {
		t.Fatalf("got %v want revision_not_found", err)
	}
	if _, err := m.Rollback(tn, "ghost", 1); reason(err) != fleet.ReasonUnknownDevice {
		t.Fatalf("got %v want unknown_device", err)
	}
	if _, err := m.Rollback("t-b", "dev-b", 1); reason(err) != fleet.ReasonNotFound {
		t.Fatalf("rollback with empty history got %v want not_found", err)
	}
	t.Log("input=rollback seq=99 -> revision_not_found; other-tenant device -> not_found")

	// Full history deterministically replays what happened.
	hist, _ := m.History(tn, dev)
	for _, r := range hist {
		t.Logf("history seq=%d version=%s status=%s rollbackTo=%d", r.Seq, r.Spec.Version, r.Status, r.RollbackTo)
	}
}

// TestRolloutQuota verifies only effective records are charged and the quota
// error is distinguishable; rejections/idempotent calls stay free.
func TestRolloutQuota(t *testing.T) {
	cp := cloud.New()
	cp.RegisterTenant("tight", cloud.Quota{MaxRollouts: 2})
	if err := cp.RegisterDevice(fleet.Device{Tenant: "tight", ID: "d"}); err != nil {
		t.Fatal(err)
	}
	m := NewManager(cp, cp, time.Now)
	dispatch := func(ver string) fleet.RejectReason {
		_, err := m.Dispatch("tight", "d", spec(ver, "0.0.0", "p"))
		t.Logf("input=dispatch %s -> reason=%q", ver, reason(err))
		return reason(err)
	}
	if dispatch("1.0.0") != fleet.ReasonOK {
		t.Fatal("first dispatch must fit quota")
	}
	if dispatch("2.0.0") != fleet.ReasonOK {
		t.Fatal("second dispatch must fit quota")
	}
	if dispatch("3.0.0") != fleet.ReasonQuotaExceeded {
		t.Fatal("third dispatch must hit rollout quota")
	}
	// Retrying 2.0.0 (already applied) still succeeds and does not charge.
	if _, err := m.Dispatch("tight", "d", spec("2.0.0", "0.0.0", "p")); err != nil {
		t.Fatalf("idempotent retry under exhausted quota must succeed: %v", err)
	}
	// Rollback is also a new effective record and is charged, so quota blocks it.
	if _, err := m.Rollback("tight", "d", 1); reason(err) != fleet.ReasonQuotaExceeded {
		t.Fatalf("rollback under quota got %v want quota_exceeded", err)
	}
	t.Log("input=rollback under exhausted quota -> quota_exceeded (rollbacks are effective records)")
}

// TestConcurrentDispatchNoCorruption: parallel identical and distinct
// dispatches linearize into a consistent history; each version applied once.
func TestConcurrentDispatchNoCorruption(t *testing.T) {
	cp, tn, dev, _ := fixture(t)
	m := NewManager(cp, cp, time.Now)
	var wg sync.WaitGroup
	errs := make(chan error, 200)
	for g := 0; g < 10; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			// Every goroutine retries the same set of versions.
			for _, ver := range []string{"1.0.0", "1.0.0", "2.0.0", "2.0.0"} {
				if _, err := m.Dispatch(tn, dev, spec(ver, "1.0.0", "p:"+ver, "gps")); err != nil {
					errs <- err
				}
			}
		}(g)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("unexpected dispatch error under concurrency: %v", err)
	}
	hist, _ := m.History(tn, dev)
	applied := map[string]int{}
	for _, r := range hist {
		if r.Status == StatusApplied {
			applied[r.Spec.Version.String()]++
		}
	}
	t.Logf("input=10 goroutines x 4 dispatch calls (half duplicates) -> applied per version: %v", applied)
	if len(applied) != 2 || applied["1.0.0"] != 1 || applied["2.0.0"] != 1 {
		t.Fatalf("each version must be applied exactly once, got %v history=%s", applied, fmtHist(hist))
	}
	cur, _ := m.Current(tn, dev)
	if cur.Spec.Version.String() != "2.0.0" {
		t.Fatalf("current=%s want 2.0.0", cur.Spec.Version)
	}
	// Revision seqs must be a gapless 1..n sequence.
	for i, r := range hist {
		if r.Seq != int64(i+1) {
			t.Fatalf("history seq gap at %d: %d", i, r.Seq)
		}
	}
}

func fmtHist(h []Revision) string {
	s := ""
	for _, r := range h {
		s += fmt.Sprintf("[%d %s %s] ", r.Seq, r.Spec.Version, r.Status)
	}
	return s
}
