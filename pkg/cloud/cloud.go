// Package cloud simulates the cloud control plane: tenant/device directory,
// quotas, device shadows and the telemetry/state ingest endpoint used by the
// edge gateways.
package cloud

import (
	"strconv"
	"strings"
	"sync"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/pkg/fleet"
)

// Quota limits how many devices and how many rollout revision records one
// tenant may own. Zero fields mean "unlimited".
type Quota struct {
	MaxDevices  int
	MaxRollouts int
}

type tenant struct {
	quota Quota
}

type deviceKey struct {
	tenant fleet.TenantID
	device fleet.DeviceID
}

// fieldProvenance records the conflict-resolution metadata stored per shadow
// field. The deterministic winner is: higher BaseRev wins; ties broken by
// lexicographically smaller EventID. The rule depends only on entry content,
// never on arrival order, so any permutation of the same updates converges to
// the same shadow (including the same Rev).
type fieldProvenance struct {
	value   string
	baseRev int64
	eventID string
}

type shadow struct {
	rev       int64
	fields    map[string]fieldProvenance
	telemetry []string
	seen      map[string]bool // applied EventIDs (deduplication)
}

// ControlPlane is the in-memory simulated cloud. All public methods are safe
// for concurrent use; each method holds the single mutex for its duration, so
// every observed state is one point in a total order of completed
// operations (deterministic linearizable reads).
type ControlPlane struct {
	mu       sync.Mutex
	tenants  map[fleet.TenantID]*tenant
	devices  map[deviceKey]fleet.Device
	owner    map[fleet.DeviceID]fleet.TenantID // global index -> cross-tenant detection
	devCount map[fleet.TenantID]int
	rollouts map[fleet.TenantID]int
	shadows  map[deviceKey]*shadow
}

// New creates an empty control plane.
func New() *ControlPlane {
	return &ControlPlane{
		tenants:  map[fleet.TenantID]*tenant{},
		devices:  map[deviceKey]fleet.Device{},
		owner:    map[fleet.DeviceID]fleet.TenantID{},
		devCount: map[fleet.TenantID]int{},
		rollouts: map[fleet.TenantID]int{},
		shadows:  map[deviceKey]*shadow{},
	}
}

// RegisterTenant onboards a tenant with the given quota. Re-registering an
// existing tenant replaces its quota (bootstrap operation).
func (c *ControlPlane) RegisterTenant(t fleet.TenantID, q Quota) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.tenants[t] = &tenant{quota: q}
}

// RegisterDevice registers a device after tenant/quota checks. Registering
// the same (tenant, device) again is idempotent.
func (c *ControlPlane) RegisterDevice(d fleet.Device) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, ok := c.tenants[d.Tenant]
	if !ok {
		return fleet.NewError(fleet.ReasonUnknownTenant, "tenant %q is not registered", d.Tenant)
	}
	if owner, dup := c.owner[d.ID]; dup {
		if owner != d.Tenant {
			return fleet.NewError(fleet.ReasonCrossTenant,
				"device %q already belongs to tenant %q", d.ID, owner)
		}
		return nil // idempotent re-registration
	}
	if t.quota.MaxDevices > 0 && c.devCount[d.Tenant] >= t.quota.MaxDevices {
		return fleet.NewError(fleet.ReasonQuotaExceeded,
			"tenant %q device quota %d exhausted", d.Tenant, t.quota.MaxDevices)
	}
	c.devices[deviceKey{d.Tenant, d.ID}] = d
	c.owner[d.ID] = d.Tenant
	c.devCount[d.Tenant]++
	c.shadows[deviceKey{d.Tenant, d.ID}] = &shadow{
		fields: map[string]fieldProvenance{},
		seen:   map[string]bool{},
	}
	return nil
}

// LookupDevice authenticates a device, enforcing the tenant boundary. A
// device existing under another tenant yields ReasonCrossTenant; a globally
// unknown id yields ReasonUnknownDevice; unknown caller tenant yields
// ReasonUnknownTenant.
func (c *ControlPlane) LookupDevice(t fleet.TenantID, dev fleet.DeviceID) (fleet.Device, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.tenants[t]; !ok {
		return fleet.Device{}, fleet.NewError(fleet.ReasonUnknownTenant, "tenant %q is not registered", t)
	}
	if d, ok := c.devices[deviceKey{t, dev}]; ok {
		return d, nil
	}
	if owner, ok := c.owner[dev]; ok && owner != t {
		return fleet.Device{}, fleet.NewError(fleet.ReasonCrossTenant,
			"device %q belongs to tenant %q, not %q", dev, owner, t)
	}
	return fleet.Device{}, fleet.NewError(fleet.ReasonUnknownDevice, "device %q not found for tenant %q", dev, t)
}

// TenantQuota returns the quota of a registered tenant.
func (c *ControlPlane) TenantQuota(t fleet.TenantID) (Quota, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	tn, ok := c.tenants[t]
	if !ok {
		return Quota{}, false
	}
	return tn.quota, true
}

// CountRollouts reports how many rollout revision records a tenant owns.
func (c *ControlPlane) CountRollouts(t fleet.TenantID) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.rollouts[t]
}

// ReserveRollout atomically consumes one rollout-record quota slot. The
// deployment manager calls it exactly once per newly appended revision
// (idempotent no-op requests never consume a slot).
func (c *ControlPlane) ReserveRollout(t fleet.TenantID) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	tn, ok := c.tenants[t]
	if !ok {
		return fleet.NewError(fleet.ReasonUnknownTenant, "tenant %q is not registered", t)
	}
	if tn.quota.MaxRollouts > 0 && c.rollouts[t] >= tn.quota.MaxRollouts {
		return fleet.NewError(fleet.ReasonQuotaExceeded,
			"tenant %q rollout-record quota %d exhausted", t, tn.quota.MaxRollouts)
	}
	c.rollouts[t]++
	return nil
}

// Ingest applies one telemetry/state entry for a device.
//
// Semantics:
//   - unknown/cross-tenant access is rejected with a distinguishable reason;
//   - an EventID already applied is acknowledged as Duplicate (idempotent
//     redelivery: never applied twice);
//   - telemetry is appended once;
//   - state fields are merged by the deterministic rule (higher BaseRev, then
//     smaller EventID), independent of arrival order; losing fields are
//     reported in Ack.Conflict rather than silently overwritten;
//   - shadow Rev counts unique (non-duplicate) entries and is independent of
//     arrival order.
func (c *ControlPlane) Ingest(t fleet.TenantID, dev fleet.DeviceID, e fleet.Entry) (fleet.Ack, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.tenants[t]; !ok {
		return fleet.Ack{}, fleet.NewError(fleet.ReasonUnknownTenant, "tenant %q is not registered", t)
	}
	s, ok := c.shadows[deviceKey{t, dev}]
	if !ok {
		if owner, known := c.owner[dev]; known && owner != t {
			return fleet.Ack{}, fleet.NewError(fleet.ReasonCrossTenant,
				"device %q belongs to tenant %q, not %q", dev, owner, t)
		}
		return fleet.Ack{}, fleet.NewError(fleet.ReasonUnknownDevice, "device %q not found for tenant %q", dev, t)
	}

	ack := fleet.Ack{EventID: e.EventID}
	if s.seen[e.EventID] {
		ack.Duplicate = true
		return ack, nil
	}
	s.seen[e.EventID] = true
	// Rev counts every distinct (non-duplicate) entry processed, including
	// entries that lose the deterministic conflict. It is therefore exactly
	// the number of unique EventIDs observed, which is independent of arrival
	// order: any permutation of the same entry set converges to the same rev
	// as well as the same field values.
	s.rev++

	switch e.Kind {
	case fleet.KindTelemetry:
		s.telemetry = append(s.telemetry, e.Telemetry)
		ack.Accepted = true
	case fleet.KindState:
		incoming := fieldProvenance{value: e.StateValue, baseRev: e.BaseRev, eventID: e.EventID}
		old, exists := s.fields[e.StateKey]
		switch {
		case !exists:
			s.fields[e.StateKey] = incoming
			ack.Accepted = true
		case stateWins(incoming, old):
			s.fields[e.StateKey] = incoming
			ack.Accepted = true
		default:
			// Incoming loses the deterministic conflict: it is NOT applied;
			// surface the key instead of silently dropping it.
			ack.Conflict = append(ack.Conflict, e.StateKey)
		}
	default:
		return fleet.Ack{}, fleet.NewError(fleet.ReasonInvalidVersion,
			"entry %q has unknown kind %d", e.EventID, e.Kind)
	}
	return ack, nil
}

// stateWins reports whether incoming should replace old. Rule: higher BaseRev
// wins; equal BaseRev -> lexicographically smaller EventID wins. Pure
// function of content => order independent.
func stateWins(incoming, old fieldProvenance) bool {
	if incoming.baseRev != old.baseRev {
		return incoming.baseRev > old.baseRev
	}
	return incoming.eventID < old.eventID
}

// Shadow returns a deterministic deep-copy snapshot of the device shadow.
func (c *ControlPlane) Shadow(t fleet.TenantID, dev fleet.DeviceID) (fleet.ShadowState, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	s, ok := c.shadows[deviceKey{t, dev}]
	if !ok {
		if owner, known := c.owner[dev]; known && owner != t {
			return fleet.ShadowState{}, fleet.NewError(fleet.ReasonCrossTenant,
				"device %q belongs to tenant %q, not %q", dev, owner, t)
		}
		return fleet.ShadowState{}, fleet.NewError(fleet.ReasonUnknownDevice, "device %q not found for tenant %q", dev, t)
	}
	out := fleet.ShadowState{
		Rev:       s.rev,
		Fields:    make(map[string]string, len(s.fields)),
		Telemetry: append([]string(nil), s.telemetry...),
	}
	for k, v := range s.fields {
		out.Fields[k] = v.value
	}
	return out, nil
}

// ParseVersion parses "MAJOR.MINOR.PATCH" with exactly three non-negative
// integer segments (so 1.10.0 > 1.9.0).
func ParseVersion(s string) (fleet.Version, error) {
	parts := strings.Split(s, ".")
	if len(parts) != 3 {
		return fleet.Version{}, fleet.NewError(fleet.ReasonInvalidVersion,
			"version %q must have exactly 3 segments", s)
	}
	nums := make([]int, 3)
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return fleet.Version{}, fleet.NewError(fleet.ReasonInvalidVersion,
				"version %q segment %q is not a non-negative integer", s, p)
		}
		nums[i] = n
	}
	return fleet.Version{Major: nums[0], Minor: nums[1], Patch: nums[2]}, nil
}
