// Package deploy implements versioned fleet deployment rollout: idempotent
// dispatch, version/capability validation, per-device immutable version
// history and reproducible rollback to any historically applied revision.
package deploy

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/pkg/fleet"
)

// Status is the lifecycle state of one rollout record.
type Status string

const (
	// StatusApplied: the spec in this record is (or was) effective.
	StatusApplied Status = "applied"
	// StatusRejected: dispatch was refused (bad version or incompatible
	// device). The record is audit evidence and does not change Current.
	StatusRejected Status = "rejected"
	// StatusRolledBack: this record restores the spec of an earlier
	// StatusApplied revision (named by RollbackTo).
	StatusRolledBack Status = "rolled_back"
)

// Revision is one immutable entry in a device's version history.
type Revision struct {
	Seq        int64
	Spec       fleet.DeploymentSpec
	Status     Status
	Reason     fleet.RejectReason // populated when Status == StatusRejected
	At         time.Time
	RollbackTo int64 // nonzero only on rollback records
}

// Rollout is the per-device deployment state.
type Rollout struct {
	Tenant    fleet.TenantID
	Cluster   fleet.ClusterID
	Device    fleet.DeviceID
	Current   int64 // seq of the currently effective record; 0 = none
	Revisions []Revision
}

type deviceKey struct {
	tenant fleet.TenantID
	device fleet.DeviceID
}

// DeviceDirectory resolves and authorizes devices (implemented by *cloud.ControlPlane).
type DeviceDirectory interface {
	LookupDevice(t fleet.TenantID, dev fleet.DeviceID) (fleet.Device, error)
}

// QuotaProvider reserves one rollout-record quota slot atomically. Only
// records that change the effective version consume a slot; rejected dispatches
// and idempotent no-ops do not. Implemented by *cloud.ControlPlane.
type QuotaProvider interface {
	ReserveRollout(t fleet.TenantID) error
}

// Manager holds all rollouts. Methods are concurrency safe: one mutex
// serializes decisions, so concurrent dispatches/rollbacks are linearized and
// can never interleave into a corrupt history.
type Manager struct {
	mu    sync.Mutex
	dir   DeviceDirectory
	quota QuotaProvider
	clock func() time.Time
	roll  map[deviceKey]*Rollout
}

// NewManager creates a deployment manager. clock is injected so test timestamps
// are reproducible; pass time.Now in production.
func NewManager(dir DeviceDirectory, qp QuotaProvider, clock func() time.Time) *Manager {
	if clock == nil {
		clock = time.Now
	}
	return &Manager{
		dir:   dir,
		quota: qp,
		clock: clock,
		roll:  map[deviceKey]*Rollout{},
	}
}

// Dispatch validates and delivers a spec to one device.
//
// Failure reasons (fleet.RejectReason):
//   - ReasonInvalidVersion: malformed/empty spec version or requirements;
//   - directory reasons: ReasonUnknownTenant / ReasonCrossTenant /
//     ReasonUnknownDevice;
//   - ReasonIncompatibleDevice: device platform too old or a required feature
//     is missing;
//   - ReasonQuotaExceeded: tenant rollout-record quota exhausted.
//
// Idempotency: re-dispatching the identical spec returns the original record
// (applied or rejected) and never appends/charges a new record.
func (m *Manager) Dispatch(t fleet.TenantID, dev fleet.DeviceID, spec fleet.DeploymentSpec) (*Revision, error) {
	if err := validateSpec(spec); err != nil {
		return nil, err
	}
	d, err := m.dir.LookupDevice(t, dev)
	if err != nil {
		return nil, err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.roll[deviceKey{t, dev}]
	if r == nil {
		r = &Rollout{Tenant: t, Cluster: d.Cluster, Device: dev}
		m.roll[deviceKey{t, dev}] = r
	}

	// Idempotency over prior Dispatch records (RollbackTo == 0): return the
	// original record and its original verdict (including a rejection error).
	if i := findDispatch(r, spec); i >= 0 {
		rev := r.Revisions[i]
		if rev.Status == StatusRejected {
			return &rev, fleet.NewError(rev.Reason, "%s",
				"repeated dispatch reproduces the original rejection")
		}
		return &rev, nil
	}

	if reason, detail := checkCompatible(spec, d.Capabilities); reason != fleet.ReasonOK {
		rev := Revision{
			Seq:    int64(len(r.Revisions)) + 1,
			Spec:   spec,
			Status: StatusRejected,
			Reason: reason,
			At:     m.clock(),
		}
		r.Revisions = append(r.Revisions, rev)
		return &rev, fleet.NewError(reason, "%s", detail)
	}

	// Quota is reserved only for records that actually take effect.
	if err := m.quota.ReserveRollout(t); err != nil {
		return nil, err
	}
	rev := Revision{
		Seq:    int64(len(r.Revisions)) + 1,
		Spec:   spec,
		Status: StatusApplied,
		At:     m.clock(),
	}
	r.Revisions = append(r.Revisions, rev)
	r.Current = rev.Seq
	return &rev, nil
}

// Rollback restores the effective version to the historically applied revision
// seq. It appends a StatusRolledBack marker carrying that revision's spec, so
// the action is reproducible from history alone.
//
// Repeating a rollback (or dispatching when the target spec is already current)
// is an idempotent no-op returning the current record.
func (m *Manager) Rollback(t fleet.TenantID, dev fleet.DeviceID, seq int64) (*Revision, error) {
	if _, err := m.dir.LookupDevice(t, dev); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()

	r := m.roll[deviceKey{t, dev}]
	if r == nil || len(r.Revisions) == 0 {
		return nil, fleet.NewError(fleet.ReasonNotFound,
			"device %q has no rollout history", dev)
	}
	if seq < 1 || int(seq) > len(r.Revisions) {
		return nil, fleet.NewError(fleet.ReasonRevisionNotFound,
			"revision %d does not exist for device %q", seq, dev)
	}
	target := r.Revisions[seq-1]
	if target.Status != StatusApplied {
		return nil, fleet.NewError(fleet.ReasonInvalidVersion,
			"revision %d is %s, not an applied revision; only applied revisions can be restored",
			seq, target.Status)
	}
	// Idempotent: the target spec is already effective.
	if cur := m.currentLocked(r); cur != nil && specEqual(cur.Spec, target.Spec) {
		return cur, nil
	}

	if err := m.quota.ReserveRollout(t); err != nil {
		return nil, err
	}
	rev := Revision{
		Seq:        int64(len(r.Revisions)) + 1,
		Spec:       target.Spec,
		Status:     StatusRolledBack,
		At:         m.clock(),
		RollbackTo: seq,
	}
	r.Revisions = append(r.Revisions, rev)
	r.Current = rev.Seq
	return &rev, nil
}

// History returns a copy of the ordered, append-only revision log.
func (m *Manager) History(t fleet.TenantID, dev fleet.DeviceID) ([]Revision, error) {
	if _, err := m.dir.LookupDevice(t, dev); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.roll[deviceKey{t, dev}]
	if r == nil {
		return []Revision{}, nil
	}
	out := append([]Revision(nil), r.Revisions...)
	return out, nil
}

// Current returns the currently effective revision (an applied revision or a
// rollback marker), or ReasonNotFound when nothing was ever applied.
func (m *Manager) Current(t fleet.TenantID, dev fleet.DeviceID) (*Revision, error) {
	if _, err := m.dir.LookupDevice(t, dev); err != nil {
		return nil, err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	r := m.roll[deviceKey{t, dev}]
	if r == nil {
		return nil, fleet.NewError(fleet.ReasonNotFound, "device %q has no rollout", dev)
	}
	cur := m.currentLocked(r)
	if cur == nil {
		return nil, fleet.NewError(fleet.ReasonNotFound, "device %q has no applied revision", dev)
	}
	return cur, nil
}

func (m *Manager) currentLocked(r *Rollout) *Revision {
	if r.Current == 0 {
		return nil
	}
	rev := r.Revisions[r.Current-1]
	return &rev
}

// findDispatch returns the index of a prior Dispatch record with the same
// spec, or -1.
func findDispatch(r *Rollout, spec fleet.DeploymentSpec) int {
	for i := range r.Revisions {
		rev := &r.Revisions[i]
		if rev.RollbackTo == 0 && specEqual(rev.Spec, spec) {
			return i
		}
	}
	return -1
}

// validateSpec rejects structurally invalid specs.
func validateSpec(s fleet.DeploymentSpec) error {
	bad := func(detail string) error {
		return fleet.NewError(fleet.ReasonInvalidVersion, "%s", detail)
	}
	v := s.Version
	if v.Major == 0 && v.Minor == 0 && v.Patch == 0 {
		return bad("deployment version 0.0.0 is not a valid target")
	}
	if v.Major < 0 || v.Minor < 0 || v.Patch < 0 {
		return bad(fmt.Sprintf("deployment version %s has negative components", v))
	}
	if s.MinPlatform.Major < 0 || s.MinPlatform.Minor < 0 || s.MinPlatform.Patch < 0 {
		return bad(fmt.Sprintf("required platform %s has negative components", s.MinPlatform))
	}
	if s.Payload == "" {
		return bad("deployment payload must not be empty")
	}
	return nil
}

// checkCompatible returns the rejection reason/detail when the device cannot
// run the spec.
func checkCompatible(s fleet.DeploymentSpec, c fleet.Capabilities) (fleet.RejectReason, string) {
	if fleet.CompareVersions(c.MinPlatform, s.MinPlatform) < 0 {
		return fleet.ReasonIncompatibleDevice,
			fmt.Sprintf("device platform %s is older than required %s", c.MinPlatform, s.MinPlatform)
	}
	var missing []string
	for _, f := range s.RequiresFeatures {
		if !c.Features[f] {
			missing = append(missing, f)
		}
	}
	if len(missing) > 0 {
		sort.Strings(missing)
		return fleet.ReasonIncompatibleDevice,
			fmt.Sprintf("device is missing required features: %v", missing)
	}
	return fleet.ReasonOK, ""
}

// specEqual compares specs as values (feature requirements compared as sets,
// so order differences do not defeat idempotency).
func specEqual(a, b fleet.DeploymentSpec) bool {
	if a.Version != b.Version || a.MinPlatform != b.MinPlatform || a.Payload != b.Payload {
		return false
	}
	if len(a.RequiresFeatures) != len(b.RequiresFeatures) {
		return false
	}
	fa := append([]string(nil), a.RequiresFeatures...)
	fb := append([]string(nil), b.RequiresFeatures...)
	sort.Strings(fa)
	sort.Strings(fb)
	for i := range fa {
		if fa[i] != fb[i] {
			return false
		}
	}
	return true
}
