// Package fleet contains the shared domain types for the edge fleet
// orchestrator: identity, versions, capabilities, deployment specs and
// telemetry/state entries.
package fleet

import "fmt"

// TenantID identifies a tenant. Tenants are hard isolation boundaries; a
// request authenticated as one tenant can never read or mutate resources of
// another tenant.
type TenantID string

// ClusterID identifies a cluster belonging to a tenant.
type ClusterID string

// DeviceID identifies a device belonging to a cluster.
type DeviceID string

// Version is a semantic version. Values are compared as integer triples so
// that e.g. 1.10.0 > 1.9.0.
type Version struct {
	Major int
	Minor int
	Patch int
}

// String renders the version as MAJOR.MINOR.PATCH.
func (v Version) String() string { return fmt.Sprintf("%d.%d.%d", v.Major, v.Minor, v.Patch) }

// Capabilities describes what a device can run. A deployment spec whose
// requirements are not a subset of the device capabilities is incompatible.
type Capabilities struct {
	// MinPlatform is the lowest platform version the device supports
	// running. A deployment requires platform >= MinPlatform.
	MinPlatform Version
	// Features are the named features the device provides.
	Features map[string]bool
}

// DeploymentSpec is the versioned payload delivered to a device.
type DeploymentSpec struct {
	Version     Version
	MinPlatform Version
	// RequiresFeatures lists features the device must provide.
	RequiresFeatures []string
	// Payload identifies the deployment content (for logging/repro).
	Payload string
}

// EntryKind distinguishes telemetry samples from reported state fields.
type EntryKind int

const (
	KindUnknown   EntryKind = 0
	KindTelemetry EntryKind = 1
	KindState     EntryKind = 2
)

// Entry is one locally buffered item produced by a device.
type Entry struct {
	// EventID is the producer-assigned unique id. It is deduplicated
	// everywhere (local buffer and cloud), so producer retries never apply
	// twice.
	EventID string
	Kind    EntryKind
	// State entries only.
	StateKey string
	// StateValue is the new value; Telemetry entries carry their sample in
	// Telemetry.
	StateValue string
	// BaseRev is the shadow revision the state update was based on. Used by
	// the deterministic conflict rule. 0 means "no base".
	BaseRev int64
	// Telemetry is an opaque sample payload for telemetry entries.
	Telemetry string
}

// RejectReason is the closed set of machine-readable failure reasons. Every
// rejection path returns one of these so callers (and test assertions) can
// distinguish failure causes rather than parsing strings.
type RejectReason string

const (
	ReasonOK                 RejectReason = ""
	ReasonNotFound           RejectReason = "not_found"
	ReasonCrossTenant        RejectReason = "cross_tenant"
	ReasonUnknownTenant      RejectReason = "unknown_tenant"
	ReasonUnknownDevice      RejectReason = "unknown_device"
	ReasonInvalidVersion     RejectReason = "invalid_version"
	ReasonIncompatibleDevice RejectReason = "incompatible_device"
	ReasonRevisionNotFound   RejectReason = "revision_not_found"
	ReasonDuplicateID        RejectReason = "duplicate_id"
	ReasonQuotaExceeded      RejectReason = "quota_exceeded"
	ReasonConcurrentUpdate   RejectReason = "concurrent_update"
)

// RejectError carries a structured rejection reason.
type RejectError struct {
	Reason RejectReason
	Detail string
}

func (e *RejectError) Error() string {
	if e.Detail == "" {
		return string(e.Reason)
	}
	return string(e.Reason) + ": " + e.Detail
}

// NewError builds a RejectError.
func NewError(reason RejectReason, format string, args ...any) error {
	return &RejectError{Reason: reason, Detail: fmt.Sprintf(format, args...)}
}

// Device is the registered identity and static capability description.
type Device struct {
	Tenant       TenantID
	Cluster      ClusterID
	ID           DeviceID
	Capabilities Capabilities
}

// ShadowState is the cloud-side materialized state of a device.
type ShadowState struct {
	// Rev is the count of unique (EventID-deduped) entries ever applied,
	// including state entries that lost conflict resolution. It is
	// independent of arrival order.
	Rev int64
	// Fields maps state key -> value.
	Fields map[string]string
	// Telemetry is the append-only, EventID-deduped sample log in arrival
	// order.
	Telemetry []string
}

// Ack is the cloud's verdict on one ingested entry.
type Ack struct {
	EventID string
	// Accepted means "applied" (state field changed or telemetry appended).
	Accepted bool
	// Duplicate means the EventID was already applied and nothing changed.
	Duplicate bool
	// Conflict lists state keys whose value was overridden by the
	// deterministic conflict rule because another writer won.
	Conflict []string
}

// CompareVersions returns -1, 0 or 1 comparing a to b as integer triples.
func CompareVersions(a, b Version) int {
	switch {
	case a.Major != b.Major:
		if a.Major < b.Major {
			return -1
		}
		return 1
	case a.Minor != b.Minor:
		if a.Minor < b.Minor {
			return -1
		}
		return 1
	case a.Patch != b.Patch:
		if a.Patch < b.Patch {
			return -1
		}
		return 1
	}
	return 0
}
