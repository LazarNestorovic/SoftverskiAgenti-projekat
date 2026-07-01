package supervision

import (
	"sync"
	"time"
)

type restartRecord struct {
	timestamps []time.Time
}

type RestartTracker struct {
	mu      sync.Mutex
	records map[string]*restartRecord
}

type Supervisor struct {
	strategy SupervisorStrategy
	tracker  *RestartTracker
}

func NewSupervisor(strategy SupervisorStrategy) *Supervisor {
	return &Supervisor{
		strategy: strategy,
		tracker:  NewRestartTracker(),
	}
}

func (s *Supervisor) IsOneForAll() bool {
	_, ok := s.strategy.(*OneForAllStrategy)
	return ok
}

func (s *Supervisor) Handle(actorID string, err error) (Directive, time.Duration) {
	directive := s.strategy.Decide(err)
	if directive != Restart {
		return directive, 0
	} else {
		records := s.tracker.Record(actorID, s.strategy.Within())
		if records > s.strategy.MaxRestarts() {
			s.tracker.Reset(actorID)
			return Escalate, 0 // Kada saljem Escalade tada problem prosledjujem roditeljskom aktoru
		} else {
			if strategy, ok := s.strategy.(interface {
				Delay(restartCount int) time.Duration
			}); ok {
				return Restart, strategy.Delay(records)
			} else {
				return Restart, 0
			}
		}
	}
}

func NewRestartTracker() *RestartTracker {
	return &RestartTracker{
		records: make(map[string]*restartRecord),
	}
}

func (r *RestartTracker) Record(actorID string, window time.Duration) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	now := time.Now()
	cutoff := now.Add(-window)
	if record, ok := r.records[actorID]; ok {
		cutoffIndex := len(record.timestamps)
		for i, timestamp := range record.timestamps {
			if timestamp.After(cutoff) {
				cutoffIndex = i
				break
			}
		}
		record.timestamps = record.timestamps[cutoffIndex:]
		record.timestamps = append(record.timestamps, now)
		return len(record.timestamps)
	} else {
		record := restartRecord{timestamps: []time.Time{now}}
		r.records[actorID] = &record
		return 1
	}
}

func (r *RestartTracker) Reset(actorID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.records, actorID)
}
