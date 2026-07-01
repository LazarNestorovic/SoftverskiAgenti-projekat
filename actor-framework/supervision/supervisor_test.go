package supervision

import (
	"fmt"
	"testing"
	"time"
)

func TestRestartTracker(t *testing.T) {
	tracker := NewRestartTracker()

	result := tracker.Record("actor1", 50*time.Millisecond)

	if result != 1 {
		t.Errorf("ocekivano je 1 record, ali ima %d", result)
	}
}

func TestRestartTrackerWindowTimedOut(t *testing.T) {
	tracker := NewRestartTracker()

	tracker.Record("actor1", 30*time.Millisecond)
	time.Sleep(60 * time.Millisecond)
	result := tracker.Record("actor1", 30*time.Millisecond)
	if result == 2 {
		t.Errorf("RestartTracker nije izbacio prvi record nakon sto je window istekao")
	} else if result != 1 {
		t.Errorf("Ocekivano 1, ali ima %d", result)
	}
}

func TestRestartTrackerReset(t *testing.T) {
	tracker := NewRestartTracker()

	tracker.Record("actor1", 1*time.Second)
	tracker.Record("actor1", 1*time.Second)
	tracker.Record("actor1", 1*time.Second)

	tracker.Reset("actor1")

	result := tracker.Record("actor1", 1*time.Second)

	if result != 1 {
		t.Errorf("Ocekivano 1, ali ima %d", result)
	}
}

func TestSupervisorHandle_RestartNoDelay(t *testing.T) {
	strategy := NewOneForOne(2, 2*time.Second, nil)
	supervisor := NewSupervisor(strategy)

	directive, dilay := supervisor.Handle("actor1", fmt.Errorf("Greska1"))

	if directive != Restart {
		t.Errorf("Ocekivan je restart, a prosledjeno je: %v", directive)
	}

	if dilay != 0 {
		t.Errorf("Ocekivan je diley = 0, a prosledjen je dilay = %d", dilay)
	}
}

func TestSupervisorHandle_MaxRestarts(t *testing.T) {
	strategy := NewOneForOne(2, 20*time.Millisecond, nil)
	supervisor := NewSupervisor(strategy)

	supervisor.Handle("actor1", fmt.Errorf("greska1"))
	supervisor.Handle("actor1", fmt.Errorf("greska1"))
	directive, delay := supervisor.Handle("actor1", fmt.Errorf("greska1"))
	if directive != Escalate {
		t.Errorf("Ocekivan Escalade, a prosledjeno %v", directive)
	}
	if delay != 0 {
		t.Errorf("Ocekivan delay = 0, a prosledjeno delay = %d", delay)
	}
}

func TestSupervisorHandle_Resume(t *testing.T) {
	strategy := NewOneForOne(2, 20*time.Millisecond, func(err error) Directive {
		return Resume
	})

	supervisor := NewSupervisor(strategy)
	directive, delay := supervisor.Handle("actor1", fmt.Errorf("greska1"))
	if directive != Resume {
		t.Errorf("Ocekivana direktiva Resume, a prosledjena %v", directive)
	}
	if delay != 0 {
		t.Errorf("Ocekivan delay = 0, a prosledjeno delay = %d", delay)
	}
}

func TestSupervisorHandle_Backoff(t *testing.T) {
	strategy := NewExponentialBackoff(100*time.Millisecond, 2*time.Second, 2.0, 6, 10*time.Second, nil)
	supervisor := NewSupervisor(strategy)

	_, delay := supervisor.Handle("actor1", fmt.Errorf("greska1"))
	_, delay2 := supervisor.Handle("actor1", fmt.Errorf("greska1"))
	if delay2 != delay*2 {
		t.Errorf("delay je trebao biti duplo duzi, delay = %d <=> delay2 = %d", delay, delay2)
	}
}
