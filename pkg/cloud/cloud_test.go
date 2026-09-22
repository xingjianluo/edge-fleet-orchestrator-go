package cloud

import (
	"fmt"
	"sync"
	"testing"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/pkg/fleet"
)

// reason extracts the structured RejectReason so assertions distinguish
// failure causes instead of matching strings.
func reason(err error) fleet.RejectReason {
	if err == nil {
		return fleet.ReasonOK
	}
	if re, ok := err.(*fleet.RejectError); ok {
		return re.Reason
	}
	return "non_structured_error"
}

func regDevice(t *testing.T, cp *ControlPlane, tn fleet.TenantID, cl fleet.ClusterID, d fleet.DeviceID) {
	t.Helper()
	if err := cp.RegisterDevice(fleet.Device{Tenant: tn, Cluster: cl, ID: d}); err != nil {
		t.Fatalf("RegisterDevice input={tenant:%s device:%s} unexpected error: %v", tn, d, err)
	}
}

// TestTenantIsolation verifies that cross-tenant reads/ingests and unknown
// tenants/devices are rejected with distinguishable reasons.
func TestTenantIsolation(t *testing.T) {
	cp := New()
	cp.RegisterTenant("t-a", Quota{})
	cp.RegisterTenant("t-b", Quota{})
	regDevice(t, cp, "t-a", "c1", "dev-1")

	cases := []struct {
		name string
		call func() error
		want fleet.RejectReason
	}{
		{"unknown tenant lookup",
			func() error { _, e := cp.LookupDevice("t-x", "dev-1"); return e },
			fleet.ReasonUnknownTenant},
		{"cross tenant lookup",
			func() error { _, e := cp.LookupDevice("t-b", "dev-1"); return e },
			fleet.ReasonCrossTenant},
		{"unknown device lookup",
			func() error { _, e := cp.LookupDevice("t-a", "nope"); return e },
			fleet.ReasonUnknownDevice},
		{"cross tenant ingest",
			func() error {
				_, e := cp.Ingest("t-b", "dev-1", fleet.Entry{EventID: "e1", Kind: fleet.KindTelemetry, Telemetry: "x"})
				return e
			}, fleet.ReasonCrossTenant},
		{"cross tenant shadow read",
			func() error { _, e := cp.Shadow("t-b", "dev-1"); return e },
			fleet.ReasonCrossTenant},
		{"register device under other tenant's id",
			func() error {
				return cp.RegisterDevice(fleet.Device{Tenant: "t-b", ID: "dev-1"})
			}, fleet.ReasonCrossTenant},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.call()
			got := reason(err)
			t.Logf("input=%s -> reason=%q detail=%v (want=%q)", tc.name, got, err, tc.want)
			if got != tc.want {
				t.Fatalf("input=%s: got reason %q, want %q (err=%v)", tc.name, got, tc.want, err)
			}
		})
	}

	// Same device id under the same tenant is idempotent and not quota charged.
	if err := cp.RegisterDevice(fleet.Device{Tenant: "t-a", Cluster: "c1", ID: "dev-1"}); err != nil {
		t.Fatalf("idempotent re-registration failed: %v", err)
	}
	t.Log("input=re-register same (tenant,device) -> accepted idempotently")
}

// TestDeviceQuota verifies quota exhaustion is distinguishable from auth
// failures, and does not leak across tenants.
func TestDeviceQuota(t *testing.T) {
	cp := New()
	cp.RegisterTenant("small", Quota{MaxDevices: 1})
	cp.RegisterTenant("other", Quota{MaxDevices: 1})
	regDevice(t, cp, "small", "c", "d0")

	err := cp.RegisterDevice(fleet.Device{Tenant: "small", ID: "d1"})
	t.Logf("input=register 2nd device for quota-1 tenant -> reason=%q detail=%v", reason(err), err)
	if reason(err) != fleet.ReasonQuotaExceeded {
		t.Fatalf("got %v, want quota_exceeded", err)
	}
	// Other tenant is unaffected.
	regDevice(t, cp, "other", "c", "o0")
	t.Log("input=register for a different tenant -> accepted (quotas do not leak)")
}

// stateEntry is a test input descriptor.
type stateEntry struct {
	id      string
	key     string
	value   string
	baseRev int64
}

func ingestState(t *testing.T, cp *ControlPlane, tn fleet.TenantID, dev fleet.DeviceID, e stateEntry) fleet.Ack {
	t.Helper()
	ack, err := cp.Ingest(tn, dev, fleet.Entry{
		EventID: e.id, Kind: fleet.KindState, StateKey: e.key,
		StateValue: e.value, BaseRev: e.baseRev,
	})
	if err != nil {
		t.Fatalf("Ingest input=%+v unexpected error: %v", e, err)
	}
	t.Logf("ingest input={id:%s key:%s value:%s baseRev:%d} -> accepted=%v conflict=%v duplicate=%v",
		e.id, e.key, e.value, e.baseRev, ack.Accepted, ack.Conflict, ack.Duplicate)
	return ack
}

// TestOutOfOrderStateConvergence: conflict rule depends only on content
// (higher BaseRev, then smaller EventID), so two opposite arrival orders of
// the same entry set must converge to the identical shadow.
func TestOutOfOrderStateConvergence(t *testing.T) {
	entries := []stateEntry{
		{id: "A", key: "mode", value: "cloud", baseRev: 5},
		{id: "B", key: "mode", value: "edge", baseRev: 5},  // tie on BaseRev 5: smaller EventID "A" wins
		{id: "C", key: "mode", value: "fleet", baseRev: 9}, // higher BaseRev 9 wins regardless of order
		{id: "D", key: "temp", value: "hot", baseRev: 2},
	}
	_ = entries

	run := func(order []int) fleet.ShadowState {
		cp := New()
		cp.RegisterTenant("t", Quota{})
		regDevice(t, cp, "t", "c", "dev")
		for _, idx := range order {
			e := entries[idx]
			ingestState(t, cp, "t", "dev", e)
		}
		sh, err := cp.Shadow("t", "dev")
		if err != nil {
			t.Fatalf("Shadow: %v", err)
		}
		return sh
	}

	forward := run([]int{0, 1, 2, 3})
	reverse := run([]int{3, 2, 1, 0})

	t.Logf("forward-order  shadow: rev=%d fields=%v telemetry=%v", forward.Rev, forward.Fields, forward.Telemetry)
	t.Logf("reverse-order  shadow: rev=%d fields=%v telemetry=%v", reverse.Rev, reverse.Fields, reverse.Telemetry)
	if fmt.Sprint(forward.Fields) != fmt.Sprint(reverse.Fields) {
		t.Fatalf("conflict rule is order dependent: %v vs %v", forward.Fields, reverse.Fields)
	}
	if forward.Rev != reverse.Rev {
		t.Fatalf("revision is order dependent: %d vs %d", forward.Rev, reverse.Rev)
	}
	want := map[string]string{"mode": "fleet", "temp": "hot"}
	if fmt.Sprint(forward.Fields) != fmt.Sprint(want) {
		t.Fatalf("converged fields=%v want=%v (rule: max BaseRev; tie -> min EventID)", forward.Fields, want)
	}
}

// TestTelemetryDedupAndDuplicateAck: redelivering the same EventID applies
// exactly once and is acknowledged as Duplicate; order of two different
// samples is preserved by log position, never duplicated.
func TestTelemetryDedupAndDuplicateAck(t *testing.T) {
	cp := New()
	cp.RegisterTenant("t", Quota{})
	regDevice(t, cp, "t", "c", "dev")
	send := func(id, payload string) fleet.Ack {
		ack, err := cp.Ingest("t", "dev", fleet.Entry{EventID: id, Kind: fleet.KindTelemetry, Telemetry: payload})
		if err != nil {
			t.Fatalf("input={id:%s payload:%s}: %v", id, payload, err)
		}
		t.Logf("input={id:%s payload:%q} -> accepted=%v duplicate=%v", id, payload, ack.Accepted, ack.Duplicate)
		return ack
	}
	a1 := send("T1", "sample-1")
	a2 := send("T2", "sample-2")
	if !a1.Accepted || !a2.Accepted {
		t.Fatal("first deliveries must be accepted")
	}
	a1dup := send("T1", "sample-1")
	a2dup := send("T2", "sample-2")
	if !a1dup.Duplicate || !a2dup.Duplicate || a1dup.Accepted || a2dup.Accepted {
		t.Fatalf("redeliveries must be duplicate non-applying: %+v %+v", a1dup, a2dup)
	}
	sh, _ := cp.Shadow("t", "dev")
	want := []string{"sample-1", "sample-2"}
	if fmt.Sprint(sh.Telemetry) != fmt.Sprint(want) {
		t.Fatalf("telemetry log=%v want=%v (exactly-once, arrival order)", sh.Telemetry, want)
	}
	if sh.Rev != 2 {
		t.Fatalf("rev=%d want 2 (duplicates must not bump rev)", sh.Rev)
	}
}

// TestConcurrentStateUpdates: many goroutines update distinct keys plus
// competing writes to one key; no update is lost, and reads are always a
// deterministic point in the total order (no torn maps).
func TestConcurrentStateUpdates(t *testing.T) {
	cp := New()
	cp.RegisterTenant("t", Quota{})
	regDevice(t, cp, "t", "c", "dev")
	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// Distinct keys: all must land.
			id1 := fmt.Sprintf("key-%d", i)
			if _, err := cp.Ingest("t", "dev", fleet.Entry{
				EventID: id1, Kind: fleet.KindState, StateKey: id1,
				StateValue: fmt.Sprintf("v%d", i), BaseRev: 1,
			}); err != nil {
				t.Errorf("distinct key ingest %d: %v", i, err)
			}
			// Competing writes to "race" all based at rev 1: exactly one
			// deterministic winner (smallest EventID among them).
			if _, err := cp.Ingest("t", "dev", fleet.Entry{
				EventID: fmt.Sprintf("race-%02d", i), Kind: fleet.KindState, StateKey: "race",
				StateValue: fmt.Sprintf("w%d", i), BaseRev: 1,
			}); err != nil {
				t.Errorf("race ingest %d: %v", i, err)
			}
		}(i)
	}
	// Concurrent readers must never observe an inconsistent snapshot.
	stop := make(chan struct{})
	go func() {
		for {
			select {
			case <-stop:
				return
			default:
				if sh, err := cp.Shadow("t", "dev"); err == nil {
					_ = len(sh.Fields) // exercise deep copy under race detector
				}
			}
		}
	}()
	wg.Wait()
	close(stop)

	sh, _ := cp.Shadow("t", "dev")
	if len(sh.Fields) != n+1 {
		t.Fatalf("field count=%d want %d (no distinct update may be lost)", len(sh.Fields), n+1)
	}
	if sh.Fields["race"] != "w0" {
		t.Fatalf("race winner=%q want %q (tie on BaseRev -> smallest EventID race-00)", sh.Fields["race"], "w0")
	}
	t.Logf("input=50 concurrent goroutines x 2 keys -> fields=%d race winner=%q rev=%d",
		len(sh.Fields), sh.Fields["race"], sh.Rev)
}

// TestParseVersion covers malformed inputs and numeric (not lexicographic)
// ordering.
func TestParseVersion(t *testing.T) {
	v, err := ParseVersion("1.10.0")
	if err != nil || v.String() != "1.10.0" {
		t.Fatalf("ParseVersion(1.10.0)=%v %v", v, err)
	}
	if fleet.CompareVersions(mustVer(t, "1.10.0"), mustVer(t, "1.9.0")) <= 0 {
		t.Fatal("1.10.0 must be greater than 1.9.0 numerically")
	}
	for _, bad := range []string{"1.2", "1.2.x", "", "v1.2.3", "1.-1.0"} {
		if _, err := ParseVersion(bad); reason(err) != fleet.ReasonInvalidVersion {
			t.Fatalf("input=%q err=%v, want invalid_version", bad, err)
		}
		t.Logf("input=%q correctly rejected as invalid_version", bad)
	}
}

func mustVer(t *testing.T, s string) fleet.Version {
	t.Helper()
	v, err := ParseVersion(s)
	if err != nil {
		t.Fatal(err)
	}
	return v
}
