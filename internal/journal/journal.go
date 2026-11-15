// Package journal owns the bounded, numbered stream of domain facts that is
// committed together with the state those facts describe.
//
// Callers declare facts through a transaction. They never assign sequence
// numbers, trim the retention window, or publish events themselves: Begin
// stages the currently committed journal, Record appends facts to that stage,
// and Commit writes the result into the state that the store is about to
// persist. If the surrounding transaction is rejected or the write fails, the
// staged facts never become visible.
package journal

import (
	"atlas-render/internal/model"
	"time"
)

// Window is the maximum number of most recent events retained in state.
const Window = 500

// Tx is a single state transition. Recorded facts share the transition's
// commit boundary, so an entity and the event describing it either both
// become durable or neither does.
type Tx struct {
	state    model.State
	sequence int
	events   []model.Event
	reserved bool
}

// Begin stages a transaction over a private copy of the persisted state. The
// clone isolates the candidate transition: rejected or unwritable commits
// cannot mutate the journal that consumers currently observe.
func Begin(current model.State) *Tx {
	state := model.Clone(current)
	return &Tx{state: state, sequence: state.Sequence, events: state.Events}
}

// State exposes the working copy that the transaction mutates. The journal
// remains authoritative for Sequence and Events; Commit replaces both.
func (t *Tx) State() *model.State { return &t.state }

// NextID derives an identifier from the next fact sequence number. The next
// Record call in this transaction consumes that same number, keeping new
// object identifiers aligned with the event stream exactly as before.
func (t *Tx) NextID(prefix string) string {
	if t.reserved {
		panic("journal: identifier reserved before the previous fact was recorded")
	}
	t.reserved = true
	return model.ID(prefix, t.sequence+1)
}

// Record appends one domain fact at the next sequence number. Old entries are
// dropped once the retention window is exceeded; the sequence keeps growing.
func (t *Tx) Record(kind, subject string) {
	t.sequence++
	t.reserved = false
	t.events = append(t.events, model.Event{Sequence: t.sequence, Kind: kind, Subject: subject, At: time.Now().UTC()})
	if len(t.events) > Window {
		t.events = t.events[len(t.events)-Window:]
	}
}

// Commit returns the state carrying the recorded facts. It must be called only
// after the transaction callback succeeds, immediately before validation and
// durable replacement. A reserved identifier without its fact would leave a
// number consumed by no event, so that state is rejected here.
func (t *Tx) Commit() model.State {
	if t.reserved {
		panic("journal: identifier reserved without a recorded fact")
	}
	t.state.Sequence = t.sequence
	t.state.Events = t.events
	return t.state
}

// Events returns a copy of the retained event window in sequence order. It is
// the observation entry point for committed state; callers cannot mutate the
// underlying journal through the result.
func Events(state model.State) []model.Event {
	if len(state.Events) == 0 {
		return nil
	}
	start := len(state.Events) - Window
	if start < 0 {
		start = 0
	}
	return append([]model.Event(nil), state.Events[start:]...)
}

// Sequence reports the most recently committed fact number. It is the value
// consumers should treat as the journal position, independent of how many
// entries the retention window still holds.
func Sequence(state model.State) int { return state.Sequence }
