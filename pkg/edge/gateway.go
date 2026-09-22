package edge

import (
	"errors"
	"sync"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/pkg/fleet"
)

// ErrLinkDown reports that the gateway is offline and cannot flush.
var ErrLinkDown = errors.New("edge gateway is offline")

// ErrTransient is the standard transient link failure a Link returns when the
// cloud could not be reached (or did not durably handle the entry). The
// gateway keeps such entries buffered and retries from the checkpoint.
var ErrTransient = errors.New("transient link failure")

// Link abstracts the network between a gateway and the cloud ingest
// endpoint. Tests provide controllable implementations (always-up, down/up,
// per-attempt flaky) to reproduce disconnect/reconnect scenarios.
type Link interface {
	// Send delivers one entry. A non-nil error means the entry was not
	// durably handled and must be retried later from the checkpoint.
	Send(dev fleet.Device, e fleet.Entry) (fleet.Ack, error)
}

// Gateway buffers one device's outgoing traffic. All methods are safe for
// concurrent use; a single mutex serializes produces and flushes so the
// checkpoint can never jump past a sent-but-uncommitted entry.
type Gateway struct {
	mu     sync.Mutex
	dev    fleet.Device
	store  *Store
	link   Link
	online bool
}

// NewGateway attaches a gateway for dev to a durable store. Gateways start
// offline; call Online(true) after the link is available.
func NewGateway(dev fleet.Device, store *Store, link Link) *Gateway {
	return &Gateway{dev: dev, store: store, link: link, online: false}
}

// Online sets the link state. While offline, Produce still buffers durably.
// Switching to online performs one best-effort flush from the checkpoint.
func (g *Gateway) Online(online bool) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.online = online
	if online {
		return g.flushLocked()
	}
	return nil
}

// IsOnline reports the current link state.
func (g *Gateway) IsOnline() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.online
}

// Produce accepts one device-originated entry: it is deduplicated by EventID
// locally (producer retries are rejected with ReasonDuplicateID, never
// buffered twice), durably appended, and when online an immediate flush is
// attempted. Offline buffering always succeeds once the append is durable.
func (g *Gateway) Produce(e fleet.Entry) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, err := g.store.Append(e); err != nil {
		return err // includes ReasonDuplicateID
	}
	if g.online {
		return g.flushLocked()
	}
	return nil
}

// Flush sends every pending entry strictly after the checkpoint, in log
// order, stopping at the first transient failure and committing each
// acknowledged entry before the next send. The checkpoint therefore never
// passes an entry the cloud did not durably handle.
func (g *Gateway) Flush() (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.online {
		return 0, ErrLinkDown
	}
	return g.flushCountLocked()
}

func (g *Gateway) flushLocked() error {
	if !g.online {
		return nil // buffering only; no error for the producer
	}
	_, err := g.flushCountLocked()
	return err
}

func (g *Gateway) flushCountLocked() (int, error) {
	pending := g.store.Pending()
	sent := 0
	for _, pe := range pending {
		ack, err := g.link.Send(g.dev, pe.Entry)
		_ = ack
		if err == nil {
			// Cloud handled it (applied or idempotent duplicate): advance
			// the durable checkpoint before sending anything else.
			if cerr := g.store.Commit(pe.Seq); cerr != nil {
				return sent, cerr
			}
			sent++
			continue
		}
		var rej *fleet.RejectError
		if errors.As(err, &rej) {
			// Permanent cloud verdict (e.g. device no longer registered):
			// replaying cannot change it, so commit past it to keep the
			// queue moving. The entry was not applied; the verdict reason
			// is returned to the caller and logged by tests.
			if cerr := g.store.Commit(pe.Seq); cerr != nil {
				return sent, cerr
			}
			return sent, err
		}
		// Transient: stop in log order; earlier entries are already
		// committed, resume starts exactly here on the next flush.
		return sent, err
	}
	return sent, nil
}

// Pending reports how many buffered entries are not yet acknowledged.
func (g *Gateway) Pending() (int, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.store.Pending()), nil
}

// Checkpoint returns the committed resume position.
func (g *Gateway) Checkpoint() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.store.Checkpoint()
}

// Close releases the store.
func (g *Gateway) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.store.Close()
}
