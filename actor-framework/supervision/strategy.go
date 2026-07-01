package supervision

import (
	"math"
	"time"
)

type Directive int

const (
	Resume Directive = iota
	Restart
	Stop
	Escalate
)

type Decider func(err error) Directive

func DefaultDecider(_ error) Directive { return Restart }

type SupervisorStrategy interface {
	Decide(err error) Directive
	MaxRestarts() int
	Within() time.Duration
}

type OneForOneStrategy struct {
	maxRestarts int
	decider     Decider
	within      time.Duration
}

type OneForAllStrategy struct {
	maxRestarts int
	decider     Decider
	within      time.Duration
}

type ExponentialBackoffStrategy struct {
	initialDelay time.Duration
	maxDelay     time.Duration
	multiplier   float64
	maxRestarts  int
	within       time.Duration
	decider      Decider
}

//----- One for One ------

func NewOneForOne(maxRestarts int, within time.Duration, decider Decider) *OneForOneStrategy {
	if decider == nil {
		decider = DefaultDecider
	}
	return &OneForOneStrategy{maxRestarts: maxRestarts, within: within, decider: decider}
}

func (o *OneForOneStrategy) Decide(err error) Directive {
	return o.decider(err)
}

func (o *OneForOneStrategy) MaxRestarts() int {
	return o.maxRestarts
}

func (o *OneForOneStrategy) Within() time.Duration {
	return o.within
}

// ----- One for All -----

func NewOneForAll(maxRestarts int, within time.Duration, decider Decider) *OneForAllStrategy {
	if decider == nil {
		decider = DefaultDecider
	}
	return &OneForAllStrategy{maxRestarts: maxRestarts, within: within, decider: decider}
}

func (o *OneForAllStrategy) Decide(err error) Directive {
	return o.decider(err)
}

func (o *OneForAllStrategy) MaxRestarts() int {
	return o.maxRestarts
}

func (o *OneForAllStrategy) Within() time.Duration {
	return o.within
}

// ----- Exponential Backoff ------

func NewExponentialBackoff(initialDelay time.Duration, maxDelay time.Duration, multiplier float64, maxRestarts int, within time.Duration, decider Decider) *ExponentialBackoffStrategy {
	if decider == nil {
		decider = DefaultDecider
	}
	return &ExponentialBackoffStrategy{initialDelay: initialDelay, maxDelay: maxDelay, multiplier: multiplier, maxRestarts: maxRestarts, within: within, decider: decider}
}

func (e *ExponentialBackoffStrategy) Decide(err error) Directive {
	return e.decider(err)
}

func (e *ExponentialBackoffStrategy) MaxRestarts() int {
	return e.maxRestarts
}

func (e *ExponentialBackoffStrategy) Within() time.Duration {
	return e.within
}

func (e *ExponentialBackoffStrategy) Delay(restartCount int) time.Duration {
	if restartCount < 1 {
		return e.initialDelay
	}
	delayFloat := float64(e.initialDelay) * (math.Pow(e.multiplier, float64(restartCount-1)))
	delay := time.Duration(delayFloat)
	if delay > e.maxDelay {
		return e.maxDelay
	}
	return delay
}
