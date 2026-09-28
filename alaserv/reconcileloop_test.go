package alaserv

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/streadway/amqp"
)

// stubConnector records every Connect() call.
type recordingConnector struct {
	calls int32
}

func (c *recordingConnector) Connect() *amqp.Connection {
	atomic.AddInt32(&c.calls, 1)
	return nil
}

// TestSetReconnectSignalKeepsReconcileLoopAlive guards the recovery chain:
// wiring a reconnect source must NOT stop the reconcile loop. SetReconnectSignal
// used to signal cmrCloser (an accidental close with a real buffered channel),
// so every later "scheduleReconcile" trigger was swallowed and the server
// could never recover from a broker-side channel/connection close.
func TestSetReconnectSignalKeepsReconcileLoopAlive(t *testing.T) {
	s := New("amqp://localhost:1/%2F")
	defer func() { _ = s.Close() }()

	rc := &recordingConnector{}
	s.SetConnector(rc)
	s.SetReconnectSignal(stubReconnectSignaler{})

	// Request a reconcile pass; a living loop must call back into the Connector.
	s.scheduleReconcile()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&rc.calls) > 0 {
			return // loop alive: reconcile ran and pulled from the Connector
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("reconcile loop is dead: SetReconnectSignal stopped it, recovery triggers are swallowed forever")
}

// TestScheduleReconcileDrainsRepeatedly ensures repeated triggers keep being
// processed (coalescing must not wedge the loop).
func TestScheduleReconcileKeepsProcessingTriggers(t *testing.T) {
	s := New("amqp://localhost:1/%2F")
	defer func() { _ = s.Close() }()

	rc := &recordingConnector{}
	s.SetConnector(rc)
	s.SetReconnectSignal(stubReconnectSignaler{})

	for i := 0; i < 3; i++ {
		s.scheduleReconcile()
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&rc.calls) >= 1 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("reconcile loop never processed the scheduled trigger")
}

type stubReconnectSignaler struct{}

func (stubReconnectSignaler) ReconnectSignal() <-chan *amqp.Connection {
	ch := make(chan *amqp.Connection) // never fires
	return ch
}

// TestRetryWithBackoffFiresDelayedTrigger guards the backoff: a failure retry
// must NOT be processed immediately (the old hot loop ran ~200 passes/sec,
// ~80MB/hour of logs), and the backoff must stay bounded.
func TestRetryWithBackoffFiresDelayedTrigger(t *testing.T) {
	s := New("amqp://localhost:1/%2F")
	defer func() { _ = s.Close() }()

	rc := &recordingConnector{}
	s.SetConnector(rc)
	s.SetReconnectSignal(stubReconnectSignaler{})

	callsBefore := atomic.LoadInt32(&rc.calls)
	s.retryAttempt = 1 // next backoff = base << 2 ... base << 1 = 1s

	s.retryWithBackoff()

	// Immediately after scheduling, the trigger must not have been processed
	// yet (it is queued by a timer ~1s out, not right away).
	time.Sleep(200 * time.Millisecond)
	if got := atomic.LoadInt32(&rc.calls); got != callsBefore {
		t.Fatalf("reconcile retried too early: connector called %d -> %d (backoff ignored)", callsBefore, got)
	}

	// The delayed trigger must eventually fire (base<<1 = 1s; allow 5s).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&rc.calls) > callsBefore {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatal("backoff timer never scheduled a reconcile retry")
}

// TestRetryBackoffBounded guards overflow clamping of the backoff after many
// consecutive failures.
func TestRetryBackoffBounded(t *testing.T) {
	s := New("amqp://localhost:1/%2F")
	defer func() { _ = s.Close() }()

	rc := &recordingConnector{}
	s.SetConnector(rc)
	s.SetReconnectSignal(stubReconnectSignaler{})

	callsBefore := atomic.LoadInt32(&rc.calls)
	s.retryAttempt = 100000 // would overflow a duration shift if unguarded
	s.retryWithBackoff()    // must not panic and must schedule within cap

	// The reconcile loop consumes the trigger from reconcileCh, so observe
	// delivery indirectly via the Connector being pulled.
	deadline := time.Now().Add(reconcileRetryMax + 5*time.Second)
	for time.Now().Before(deadline) {
		if atomic.LoadInt32(&rc.calls) > callsBefore {
			return // trigger arrived within the cap: clamping worked
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("backoff never fired within the 30s cap")
}
