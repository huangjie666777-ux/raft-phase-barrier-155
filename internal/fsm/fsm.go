// Package fsm implements the lease-lock state machine replicated by Raft.
//
// All decisions are made purely from the committed log: the leader stamps
// each command with its decision time (Now), and replicas replay commands
// without reading their own clocks.
package fsm

import (
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"sync"
	"time"

	"github.com/hashicorp/raft"
)

// Command types carried in Raft log entries.
const (
	CmdAcquire        = "acquire"
	CmdRenew          = "renew"
	CmdRelease        = "release"
	CmdQuery          = "query"
	CmdBarrierCreate  = "barrier_create"
	CmdBarrierArrive  = "barrier_arrive"
	CmdBarrierAdvance = "barrier_advance"
	CmdBarrierQuery   = "barrier_query"
	BarrierWaiting    = "waiting"
	BarrierCompleted  = "completed"
	BarrierFailed     = "failed"
	minBarrierMembers = 2
	maxBarrierMembers = 16
)

// Command is the unit of replication. Now is the decision time written by
// the leader before proposing; replicas must use it verbatim.
type Command struct {
	Type          string    `json:"type"`
	Resource      string    `json:"resource,omitempty"`
	Holder        string    `json:"holder,omitempty"`
	Token         uint64    `json:"token,omitempty"`
	TTL           int64     `json:"ttl,omitempty"` // seconds
	Now           time.Time `json:"now"`
	Barrier       string    `json:"barrier,omitempty"`
	Participant   string    `json:"participant,omitempty"`
	Participants  []string  `json:"participants,omitempty"`
	ExpectedRound uint64    `json:"expected_round,omitempty"`
}

// Lease is an active lock on a resource.
type Lease struct {
	Holder string    `json:"holder"`
	Token  uint64    `json:"token"`
	Expiry time.Time `json:"expiry"`
}

// Registration records one participant's barrier arrival.
type Registration struct {
	Participant string `json:"participant"`
	Resource    string `json:"resource"`
	Holder      string `json:"holder"`
	Token       uint64 `json:"token"`
}

// Barrier is one named recoverable phase barrier.
type Barrier struct {
	Name          string                  `json:"name"`
	Participants  []string                `json:"participants"`
	Round         uint64                  `json:"round"`
	Status        string                  `json:"status"`
	Registrations map[string]Registration `json:"registrations"`
	FailureReason string                  `json:"failure_reason,omitempty"`
}

// BarrierView is the JSON-safe, deterministic view of a barrier.
type BarrierView struct {
	Name          string   `json:"name"`
	Round         uint64   `json:"round"`
	Status        string   `json:"status"`
	Participants  []string `json:"participants"`
	Arrived       []string `json:"arrived"`
	FailureReason string   `json:"failure_reason,omitempty"`
}

// Result is the deterministic outcome of applying a Command.
type Result struct {
	OK      bool         `json:"ok"`
	Holder  string       `json:"holder,omitempty"`
	Token   uint64       `json:"token,omitempty"`
	Expiry  int64        `json:"expiry,omitempty"` // unix seconds
	Barrier *BarrierView `json:"barrier,omitempty"`
	Err     string       `json:"err,omitempty"`
}

// FSM is the replicated lease-lock state machine.
type FSM struct {
	mu sync.RWMutex
	// leases holds currently valid (possibly not-yet-expired) leases.
	leases map[string]*Lease
	// tokenBounds is the per-resource historical token upper bound.
	// It is monotonic and never removed, even when a lock is released.
	tokenBounds map[string]uint64
	barriers    map[string]*Barrier
}

func New() *FSM {
	return &FSM{
		leases:      make(map[string]*Lease),
		tokenBounds: make(map[string]uint64),
		barriers:    make(map[string]*Barrier),
	}
}

func (f *FSM) liveLease(resource string, now time.Time) *Lease {
	l, ok := f.leases[resource]
	if !ok {
		return nil
	}
	if !now.Before(l.Expiry) {
		return nil // expired
	}
	return l
}

func normalizedParticipants(in []string) ([]string, error) {
	if len(in) < minBarrierMembers || len(in) > maxBarrierMembers {
		return nil, fmt.Errorf("barrier must have %d to %d participants", minBarrierMembers, maxBarrierMembers)
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, len(in))
	copy(out, in)
	sort.Strings(out)
	for _, id := range out {
		if id == "" {
			return nil, fmt.Errorf("participant id must not be empty")
		}
		if _, ok := seen[id]; ok {
			return nil, fmt.Errorf("duplicate participant %q", id)
		}
		seen[id] = struct{}{}
	}
	return out, nil
}

func sameParticipants(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func (f *FSM) registrationValid(reg Registration, now time.Time) error {
	l := f.liveLease(reg.Resource, now)
	if l == nil {
		return fmt.Errorf("participant %s lease on resource %q is no longer valid", reg.Participant, reg.Resource)
	}
	if l.Holder != reg.Holder || l.Token != reg.Token {
		return fmt.Errorf("participant %s lease token changed", reg.Participant)
	}
	return nil
}

func (f *FSM) validateRegistrations(b *Barrier, now time.Time) error {
	ids := make([]string, 0, len(b.Registrations))
	for id := range b.Registrations {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		if err := f.registrationValid(b.Registrations[id], now); err != nil {
			return err
		}
	}
	return nil
}

func (f *FSM) failBarrier(b *Barrier, reason string) Result {
	b.Status = BarrierFailed
	b.FailureReason = reason
	return Result{OK: false, Barrier: barrierView(b), Err: reason}
}

func barrierView(b *Barrier) *BarrierView {
	if b == nil {
		return nil
	}
	arrived := make([]string, 0, len(b.Registrations))
	for id := range b.Registrations {
		arrived = append(arrived, id)
	}
	sort.Strings(arrived)
	participants := make([]string, len(b.Participants))
	copy(participants, b.Participants)
	return &BarrierView{
		Name:          b.Name,
		Round:         b.Round,
		Status:        b.Status,
		Participants:  participants,
		Arrived:       arrived,
		FailureReason: b.FailureReason,
	}
}

func (f *FSM) leaseResult(resource string, now time.Time) Result {
	if l := f.liveLease(resource, now); l != nil {
		return Result{OK: true, Holder: l.Holder, Token: l.Token, Expiry: l.Expiry.Unix()}
	}
	return Result{OK: false}
}

func (f *FSM) applyBarrierCreate(cmd Command) Result {
	members, err := normalizedParticipants(cmd.Participants)
	if err != nil {
		return Result{OK: false, Err: err.Error()}
	}
	if existing, ok := f.barriers[cmd.Barrier]; ok {
		if !sameParticipants(existing.Participants, members) {
			return Result{OK: false, Barrier: barrierView(existing), Err: "barrier exists with a different configuration"}
		}
		return Result{OK: true, Barrier: barrierView(existing)}
	}
	b := &Barrier{
		Name:          cmd.Barrier,
		Participants:  members,
		Round:         1,
		Status:        BarrierWaiting,
		Registrations: make(map[string]Registration),
	}
	f.barriers[cmd.Barrier] = b
	return Result{OK: true, Barrier: barrierView(b)}
}

func (f *FSM) applyBarrierArrive(cmd Command) Result {
	b, ok := f.barriers[cmd.Barrier]
	if !ok {
		return Result{OK: false, Err: "unknown barrier"}
	}
	if cmd.ExpectedRound != b.Round {
		return Result{OK: false, Barrier: barrierView(b), Err: "round mismatch"}
	}
	if b.Status == BarrierWaiting {
		if err := f.validateRegistrations(b, cmd.Now); err != nil {
			return f.failBarrier(b, err.Error())
		}
	}
	reg := Registration{
		Participant: cmd.Participant,
		Resource:    cmd.Resource,
		Holder:      cmd.Holder,
		Token:       cmd.Token,
	}
	if existing, ok := b.Registrations[cmd.Participant]; ok && existing == reg {
		return Result{OK: true, Barrier: barrierView(b)}
	}
	if b.Status != BarrierWaiting {
		return Result{OK: false, Barrier: barrierView(b), Err: "barrier round is " + b.Status}
	}
	known := false
	for _, id := range b.Participants {
		if id == cmd.Participant {
			known = true
			break
		}
	}
	if !known {
		return Result{OK: false, Barrier: barrierView(b), Err: "unknown participant"}
	}
	if existing, ok := b.Registrations[cmd.Participant]; ok {
		if existing != reg {
			return Result{OK: false, Barrier: barrierView(b), Err: "participant already arrived with a different registration"}
		}
		return Result{OK: true, Barrier: barrierView(b)}
	}
	for _, existing := range b.Registrations {
		if existing.Resource == cmd.Resource {
			return Result{OK: false, Barrier: barrierView(b), Err: "resource is already registered in this round"}
		}
	}
	if err := f.registrationValid(reg, cmd.Now); err != nil {
		return f.failBarrier(b, err.Error())
	}
	b.Registrations[cmd.Participant] = reg
	if len(b.Registrations) == len(b.Participants) {
		if err := f.validateRegistrations(b, cmd.Now); err != nil {
			return f.failBarrier(b, err.Error())
		}
		b.Status = BarrierCompleted
	}
	return Result{OK: true, Barrier: barrierView(b)}
}

func (f *FSM) applyBarrierAdvance(cmd Command) Result {
	b, ok := f.barriers[cmd.Barrier]
	if !ok {
		return Result{OK: false, Err: "unknown barrier"}
	}
	if b.Status == BarrierWaiting {
		if err := f.validateRegistrations(b, cmd.Now); err != nil {
			return f.failBarrier(b, err.Error())
		}
	}
	if cmd.ExpectedRound != b.Round {
		return Result{OK: false, Barrier: barrierView(b), Err: "round mismatch"}
	}
	if b.Status == BarrierWaiting {
		return Result{OK: false, Barrier: barrierView(b), Err: "barrier has not reached a terminal state"}
	}
	b.Round++
	b.Status = BarrierWaiting
	b.FailureReason = ""
	b.Registrations = make(map[string]Registration)
	return Result{OK: true, Barrier: barrierView(b)}
}

func (f *FSM) applyBarrierQuery(cmd Command) Result {
	b, ok := f.barriers[cmd.Barrier]
	if !ok {
		return Result{OK: false, Err: "unknown barrier"}
	}
	if b.Status == BarrierWaiting {
		if err := f.validateRegistrations(b, cmd.Now); err != nil {
			return f.failBarrier(b, err.Error())
		}
	}
	return Result{OK: true, Barrier: barrierView(b)}
}

// Apply executes a committed command in log order.
func (f *FSM) Apply(log *raft.Log) interface{} {
	var cmd Command
	if err := json.Unmarshal(log.Data, &cmd); err != nil {
		return Result{Err: "corrupt command: " + err.Error()}
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	switch cmd.Type {
	case CmdQuery:
		return f.leaseResult(cmd.Resource, cmd.Now)

	case CmdAcquire:
		if l := f.liveLease(cmd.Resource, cmd.Now); l != nil {
			return Result{
				OK:     false,
				Holder: l.Holder,
				Token:  l.Token,
				Expiry: l.Expiry.Unix(),
				Err:    "resource is locked",
			}
		}
		token := f.tokenBounds[cmd.Resource] + 1
		f.tokenBounds[cmd.Resource] = token
		expiry := cmd.Now.Add(time.Duration(cmd.TTL) * time.Second)
		f.leases[cmd.Resource] = &Lease{Holder: cmd.Holder, Token: token, Expiry: expiry}
		return Result{OK: true, Holder: cmd.Holder, Token: token, Expiry: expiry.Unix()}

	case CmdRenew, CmdRelease:
		l := f.liveLease(cmd.Resource, cmd.Now)
		if l == nil {
			return Result{OK: false, Err: "no valid lease"}
		}
		if l.Holder != cmd.Holder || l.Token != cmd.Token {
			return Result{OK: false, Err: "holder or token mismatch"}
		}
		if cmd.Type == CmdRenew {
			// Recompute the deadline from the decision moment.
			l.Expiry = cmd.Now.Add(time.Duration(cmd.TTL) * time.Second)
			return Result{OK: true, Holder: l.Holder, Token: l.Token, Expiry: l.Expiry.Unix()}
		}
		// Release: drop the lease but keep the token upper bound.
		delete(f.leases, cmd.Resource)
		return Result{OK: true}

	default:
		switch cmd.Type {
		case CmdBarrierCreate:
			if cmd.Barrier == "" {
				return Result{OK: false, Err: "barrier name is required"}
			}
			return f.applyBarrierCreate(cmd)
		case CmdBarrierArrive:
			if cmd.Barrier == "" || cmd.Participant == "" || cmd.Resource == "" || cmd.Holder == "" || cmd.Token == 0 {
				return Result{OK: false, Err: "barrier, participant, resource, holder and token are required"}
			}
			return f.applyBarrierArrive(cmd)
		case CmdBarrierAdvance:
			if cmd.Barrier == "" || cmd.ExpectedRound == 0 {
				return Result{OK: false, Err: "barrier and expected_round are required"}
			}
			return f.applyBarrierAdvance(cmd)
		case CmdBarrierQuery:
			if cmd.Barrier == "" {
				return Result{OK: false, Err: "barrier name is required"}
			}
			return f.applyBarrierQuery(cmd)
		default:
			return Result{Err: fmt.Sprintf("unknown command type %q", cmd.Type)}
		}
	}
}

// Query returns the live lease for a resource as of now, or nil.
func (f *FSM) Query(resource string, now time.Time) *Lease {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if l := f.liveLease(resource, now); l != nil {
		cp := *l
		return &cp
	}
	return nil
}

// TokenBound returns the historical token upper bound for a resource.
func (f *FSM) TokenBound(resource string) uint64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.tokenBounds[resource]
}

// snapshot is the persisted form of the FSM.
type snapshot struct {
	Leases      map[string]*Lease   `json:"leases"`
	TokenBounds map[string]uint64   `json:"token_bounds"`
	Barriers    map[string]*Barrier `json:"barriers,omitempty"`
}

// Snapshot captures leases plus token upper bounds.
func (f *FSM) Snapshot() (raft.FSMSnapshot, error) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	s := snapshot{
		Leases:      make(map[string]*Lease, len(f.leases)),
		TokenBounds: make(map[string]uint64, len(f.tokenBounds)),
		Barriers:    make(map[string]*Barrier, len(f.barriers)),
	}
	for k, v := range f.leases {
		cp := *v
		s.Leases[k] = &cp
	}
	for k, v := range f.tokenBounds {
		s.TokenBounds[k] = v
	}
	for name, b := range f.barriers {
		participants := make([]string, len(b.Participants))
		copy(participants, b.Participants)
		registrations := make(map[string]Registration, len(b.Registrations))
		for id, reg := range b.Registrations {
			registrations[id] = reg
		}
		s.Barriers[name] = &Barrier{
			Name:          b.Name,
			Participants:  participants,
			Round:         b.Round,
			Status:        b.Status,
			Registrations: registrations,
			FailureReason: b.FailureReason,
		}
	}
	return &fsmSnapshot{state: s}, nil
}

// Restore replaces the FSM state from a snapshot.
func (f *FSM) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	var s snapshot
	if err := json.NewDecoder(rc).Decode(&s); err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leases = s.Leases
	if f.leases == nil {
		f.leases = make(map[string]*Lease)
	}
	f.tokenBounds = s.TokenBounds
	if f.tokenBounds == nil {
		f.tokenBounds = make(map[string]uint64)
	}
	f.barriers = s.Barriers
	if f.barriers == nil {
		f.barriers = make(map[string]*Barrier)
	}
	for _, b := range f.barriers {
		if b.Registrations == nil {
			b.Registrations = make(map[string]Registration)
		}
		if b.Participants == nil {
			b.Participants = []string{}
		}
	}
	return nil
}

type fsmSnapshot struct {
	state snapshot
}

func (s *fsmSnapshot) Persist(sink raft.SnapshotSink) error {
	err := json.NewEncoder(sink).Encode(s.state)
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s *fsmSnapshot) Release() {}
