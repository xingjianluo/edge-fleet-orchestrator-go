package edge

import (
	"sync"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/pkg/cloud"
	"github.com/xingjianluo/edge-fleet-orchestrator-go/pkg/fleet"
)

// SenderFunc applies one entry at the cloud.
type SenderFunc func(dev fleet.Device, e fleet.Entry) (fleet.Ack, error)

// Failer decides whether an individual send attempt must fail transiently
// (attempt is the 0-based attempt number for that EventID). Tests use it to
// inject "first attempt fails, retry succeeds" or repeated outage windows.
type Failer func(dev fleet.Device, e fleet.Entry, attempt int) bool

// SimLink is a controllable link: explicit up/down state plus per-attempt
// transient failure injection. It also records a deterministic attempt log so
// tests can print the exact send/ack sequence as evidence.
type SimLink struct {
	mu       sync.Mutex
	up       bool
	sender   SenderFunc
	failer   Failer
	attempts map[string]int
	log      []AttemptRecord
}

// AttemptRecord records one Send call and its outcome.
type AttemptRecord struct {
	EventID       string
	Attempt       int
	Up            bool
	TransientFail bool
	Accepted      bool
	Duplicate     bool
	Conflict      []string
}

// NewSimLink wraps a sender. It starts down; call SetUp(true) to connect.
func NewSimLink(sender SenderFunc) *SimLink {
	return &SimLink{sender: sender, attempts: map[string]int{}, up: false}
}

// NewCloudLink builds a SimLink whose sender is the cloud ingest endpoint for
// the given tenant.
func NewCloudLink(cp *cloud.ControlPlane, t fleet.TenantID) *SimLink {
	return NewSimLink(func(dev fleet.Device, e fleet.Entry) (fleet.Ack, error) {
		return cp.Ingest(t, dev.ID, e)
	})
}

// SetUp toggles connectivity.
func (l *SimLink) SetUp(up bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.up = up
}

// IsUp reports connectivity.
func (l *SimLink) IsUp() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.up
}

// SetFailer installs a transient-failure predicate.
func (l *SimLink) SetFailer(f Failer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.failer = f
}

// Send implements Link. Down or injected failure => ErrTransient so the
// gateway retains the entry at its checkpoint.
func (l *SimLink) Send(dev fleet.Device, e fleet.Entry) (fleet.Ack, error) {
	l.mu.Lock()
	up := l.up
	attempt := l.attempts[e.EventID]
	l.attempts[e.EventID] = attempt + 1
	fail := l.failer
	l.mu.Unlock()

	rec := AttemptRecord{EventID: e.EventID, Attempt: attempt, Up: up}
	defer func() {
		l.mu.Lock()
		l.log = append(l.log, rec)
		l.mu.Unlock()
	}()

	if !up || (fail != nil && fail(dev, e, attempt)) {
		rec.TransientFail = true
		return fleet.Ack{}, ErrTransient
	}
	ack, err := l.sender(dev, e)
	if err == nil {
		rec.Accepted = ack.Accepted
		rec.Duplicate = ack.Duplicate
		rec.Conflict = append([]string(nil), ack.Conflict...)
	}
	return ack, err
}

// AttemptLog returns a copy of all send attempts in call order.
func (l *SimLink) AttemptLog() []AttemptRecord {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := append([]AttemptRecord(nil), l.log...)
	return out
}
