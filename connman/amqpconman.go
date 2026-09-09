package connman

import (
	"sync"
	"time"

	"github.com/alsve/ala/logger"

	"github.com/streadway/amqp"
)

// NewAMQPConnectionManager creates a new instance of AMQPConnectionManager.
func NewAMQPConnectionManager(l logger.Logger, amqpURI string, opt ...Option) *AMQPConnectionManager {
	arcm := &AMQPConnectionManager{
		log:             l,
		amqpURI:         amqpURI,
		reconnectSignal: make(chan *amqp.Error, 1),
		tpReconnSignal:  make([]chan *amqp.Connection, 0, 10),
		routineCloser:   make(chan struct{}),
		cond:            sync.NewCond(&sync.Mutex{}),
		autoReconnect:   true,
	}
	if len(opt) >= 1 {
		arcm.autoReconnect = opt[0].AutoReconnect
	}

	go arcm.startConnectionMonitorRoutine()
	arcm.reconnectSignal <- nil

	return arcm
}

// AMQPClientConnectionManager manages connection AMQP protocol with default auto-reconnect feature enabled.
type AMQPConnectionManager struct {
	log     logger.Logger
	amqpURI string // amqpURI is a amqp connection string.

	conn *amqp.Connection // conn represents current active connection.

	reconnectSignal chan *amqp.Error        // reconnectSignal holds notifier channel when connection closed.
	tpReconnSignal  []chan *amqp.Connection // tpReconnSignal signal third party for reconnection.
	routineCloser   chan struct{}           // routineCloser holds channel for signal close connection monitor routine.

	autoReconnect bool // autoReconect sets whether connection manager should auto reconnect or not.

	cond *sync.Cond
}

// startConnectionMonitorRoutine starts connection monitor routine.
func (a *AMQPConnectionManager) startConnectionMonitorRoutine() {
	backoff := time.Second
	for {
		select {
		case <-a.routineCloser:
			a.log.Info("AMQPConnectionManager.startConnectionMonitorRoutine: receive close signal, closing routine")
			return
		case amErr := <-a.reconnectSignal:
			a.cond.L.Lock()
			if amErr != nil {
				a.log.Error("AMQPConnectionManager.startConnectionMonitorRoutine: %s", amErr.Error())
			}
			a.log.Info("AMQPConnectionManager.startConnectionMonitorRoutine: reconnecting to AMQP Message Broker.")
			a.conn = nil
			a.cond.L.Unlock()

			// FIX(retry-loop): previously a failed dial left cond.L locked and
			// waited for a *new* close notification that never comes, so the
			// manager stayed dead forever after a single transient failure.
			// Now: retry with bounded backoff until the broker is reachable.
			var conn *amqp.Connection
			for {
				c, err := amqp.Dial(a.amqpURI)
				if err == nil {
					conn = c
					break
				}
				a.log.Error("AMQPConnectionManager.startConnectionMonitorRoutine: dial failed: %s; retrying in %s", err.Error(), backoff)
				time.Sleep(backoff)
				if backoff < 30*time.Second {
					backoff *= 2
				}
			}
			backoff = time.Second

			if a.autoReconnect {
				// FIX(shared-close-chan): registering the same channel on every
				// connection makes streadway close it after the first
				// notification, panicing ("send on closed channel") on the
				// next connection loss. Use a per-connection receiver and
				// forward the event without dropping reconnect machinery.
				closeCh := make(chan *amqp.Error, 1)
				conn.NotifyClose(closeCh)
				go func(closer chan *amqp.Error) {
					amErr, ok := <-closer
					if !ok {
						return
					}
					select {
					case a.reconnectSignal <- amErr:
					default:
					}
				}(closeCh)
			}

			a.cond.L.Lock()
			a.conn = conn
			a.cond.L.Unlock()
			a.cond.Broadcast()

			// Coalesce bursts of queued reconnect signals so a flood of waiters
			// cannot trigger one dial per signal.
		drain:
			for {
				select {
				case <-a.reconnectSignal:
				default:
					break drain
				}
			}

			for _, ch := range a.tpReconnSignal {
				select {
				case ch <- a.conn:
				default:
				}
			}
		}
	}
}

// ReconnectSignal creates listener to reconnection.
func (a *AMQPConnectionManager) ReconnectSignal() <-chan *amqp.Connection {
	tpCh := make(chan *amqp.Connection)
	a.tpReconnSignal = append(a.tpReconnSignal, tpCh)
	return tpCh
}

// Connect connects service to AMQP message broker.
func (a *AMQPConnectionManager) Connect() *amqp.Connection {
	a.cond.L.Lock()
	defer a.cond.L.Unlock()

	for a.conn == nil {
		select {
		case a.reconnectSignal <- nil:
		default:
		}

		a.cond.Wait()
	}

	return a.conn
}

// RenewAMQPChannel renew channel from a connection.
func (a *AMQPConnectionManager) RenewAMQPChannel() (*amqp.Channel, error) {
	a.Connect()

	ch, err := a.conn.Channel()
	if err != nil {
		a.log.Error("AMQPConnectionManager.RenewAMQPChannel: ", ErrChannelCreationFailed)
		return nil, ErrChannelCreationFailed
	}

	return ch, nil
}

// Close close all connection gracefully.
func (a *AMQPConnectionManager) Close() error {
	a.routineCloser <- struct{}{}

	for _, ch := range a.tpReconnSignal {
		close(ch)
	}

	a.tpReconnSignal = nil
	err := a.conn.Close()
	return err
}
