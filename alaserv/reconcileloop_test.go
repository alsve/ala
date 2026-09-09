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
