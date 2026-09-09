package alaserv

import (
	"context"
	"fmt"
	"log"
	"time"

	uuid "github.com/satori/go.uuid"
	"github.com/streadway/amqp"
	"github.com/tidwall/spinlock"
)

// Publisher publish message to amqp message broker.
type Publisher interface {
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
}

// Replyer shall connect correlation id to listener.
type Replyer interface {
	Reply(correlationID string, data amqp.Delivery) error
}

// ConnectReconnector shall provide an active connection and reconnect signal when reconnection occured.
type ConnectReconnector interface {
	Connector
	ReconnectSignaler
}

// Connector shall provide an active connection.
type Connector interface {
	Connect() *amqp.Connection
}

// ReconnectSignaler shall provide reconnect signal when reconnection occured
type ReconnectSignaler interface {
	ReconnectSignal() <-chan *amqp.Connection
}

// HandlerFunc is entry point for receiving Delivery and make publish for the next exchange.
type HandlerFunc func(ctx context.Context, d amqp.Delivery, p Publisher) error

// New creates a new instance of Ala AMQP server.
func New(amqpURI string) *AlaServer {
	as := &AlaServer{
		amqpURI:           amqpURI,
		routeCloseSignals: newCanceler(),

		// cmrCloser coalesces close requests for the reconcile loop. Without a
		// buffered channel every send falls to the select default and Close()
		// could never actually stop the loop.
		cmrCloser: make(chan struct{}, 1),

		// FIX(nil-ctx): handler goroutines spawn contexts from a.ctx; it must
		// never be nil, otherwise any delivery handled before Start() sets it
		// panics with "cannot create context from nil parent" and crashes the
		// whole process (observed whenever a queue had a backlog at startup).
		ctx: context.Background(),

		// reconcileCh coalesces recovery triggers (connection close, consume
		// channel close, reconnect push) into serialized reconcile passes.
		reconcileCh: make(chan struct{}, 1),
	}
	go as.reconcileLoop()

	return as
}

// AlaServer is an AMQP Server.
type AlaServer struct {
	amqpURI         string
	c               Connector
	r               Replyer
	conn            *amqp.Connection
	amqpChan        *amqp.Channel // for publishing
	amqpChanConsume *amqp.Channel // for consuming

	closeSignal       context.CancelFunc
	ctx               context.Context
	routeCloseSignals *canceler

	cmrCloser       chan struct{}           // cmrCloser is connectionMonitorRoutine closer signaler.
	reconnectSignal <-chan *amqp.Connection // reconnectSignal is received when reconnection occured from Connector.

	rhs []routeHandlerSetting

	// ready will locked until all startup conduct is done.
	ready spinlock.Locker

	// nConsumePrefetch is consume prefetch count.
	// FIX(unbounded-prefetch): previously always 0 (unlimited), so the broker
	// delivered the whole queue at once and every delivery stayed unacked
	// until its handler finished. With RabbitMQ >= 3.8.15 the default
	// consumer_timeout (30 min) then CLOSES the consume channel and ala never
	// noticed: consumers silently disappeared. SetPrefetch bounds it.
	nConsumePrefetch int

	// reconcileCh coalesces recovery triggers; reconcileLoop serializes them.
	reconcileCh chan struct{}

	// registeredConn/routesRegistered track whether consumers are live on
	// the current connection+channel pair, so reconcile() never registers
	// duplicate consumers.
	registeredConn   *amqp.Connection
	routesRegistered bool
	consumeGen       int
	publishHealthy   bool
	consumeHealthy   bool

	// ownConn is a connection this server dialed itself (no Connector, or
	// Connector had no connection yet). Once the Connector provides a
	// connection, the owned one is abandoned — close it there and in Close
	// so a start never leaks an idle connection on the broker.
	ownConn *amqp.Connection

	// stopping guards Close() vs reconcile races.
	stopping bool
}

// SetPrefetch sets the consumer prefetch count (Qos) applied on every
// (re)registered consume channel. A non-zero value bounds the number of
// unacknowledged deliveries per consumer, which keeps the broker's
// delivery-acknowledgement (consumer_timeout) watchdog happy and stops
// unbounded unacked pile-ups. Call before Start.
func (a *AlaServer) SetPrefetch(n int) {
	a.ready.Lock()
	defer a.ready.Unlock()
	a.nConsumePrefetch = n
}

// SetReplyer sets replyer for connecting queue to listener.
func (a *AlaServer) SetReplyer(r Replyer) {
	a.ready.Lock()
	defer a.ready.Unlock()

	a.r = r
}

// getReplyer retrieves Replyer.
func (a *AlaServer) getReplyer() Replyer {
	if a.r == nil {
		a.ready.Lock()
		if a.r == nil { // double check like once.
			a.r = noopReplyer{}
		}
		a.ready.Unlock()
	}
	return a.r
}

// SetConnector sets connector for receiving latest active connection from connector.
func (a *AlaServer) SetConnector(c Connector) {
	a.ready.Lock()
	defer a.ready.Unlock()

	a.c = c
}

// SetReconnectSignal sets reconnect signal which give signal when reconnection occured.
func (a *AlaServer) SetReconnectSignal(c ReconnectSignaler) {
	a.ready.Lock()
	defer a.ready.Unlock()

	a.reconnectSignal = c.ReconnectSignal()

	// FIX(reconcile-loop-kill): this used to call closeConnectionMonitorRoutine(),
	// which was an accidental no-op while cmrCloser was an uninitialized nil
	// channel. Once the channel became real, this call STOPPED the reconcile
	// loop at wiring time — every later recovery trigger (connection close,
	// consume-channel close, scheduled reconcile) was then swallowed forever
	// and the server could never recover from a broker-side close.
	// _ = a.closeConnectionMonitorRoutine() — intentionally removed.
}

// closeConnectionMonitorRoutine closes connection monitor routine.
func (a *AlaServer) closeConnectionMonitorRoutine() {
	select {
	case a.cmrCloser <- struct{}{}:
	default:
	}
}

// reconcileLoop serializes all recovery triggers: reconnect pushes from the
// Connector, connection closes, and consume-channel closes.
func (a *AlaServer) reconcileLoop() {
	for {
		select {
		case <-a.cmrCloser:
			return
		case conn := <-a.reconnectSignal:
			a.reconcile(conn)
		case <-a.reconcileCh:
			a.reconcile(nil)
		}
	}
}

// scheduleReconcile requests a reconcile pass, coalescing bursts.
func (a *AlaServer) scheduleReconcile() {
	select {
	case a.reconcileCh <- struct{}{}:
	default:
	}
}

// reconcile (re)establishes channels and route registrations against the
// freshest available connection. It is the single recovery path for:
//   - Connector reconnect pushes,
//   - the current connection dying (NotifyClose),
//   - the consume channel being closed by the broker (e.g. consumer_timeout),
//   - failed (re)registrations (retried with backoff).
//
// It must never panic: transient broker outages are expected, and a panic
// takes down the whole service.
func (a *AlaServer) reconcile(pushConn *amqp.Connection) {
	a.ready.Lock()
	defer a.ready.Unlock()

	if a.stopping {
		return
	}

	// Pull the freshest connection from the Connector when available. This
	// compensates for reconnect notifications that were dropped by the
	// Connector's non-blocking push.
	conn := a.conn
	if a.c != nil {
		if pulled := a.c.Connect(); pulled != nil {
			conn = pulled
		}
	}
	if conn == nil {
		conn = a.conn
	}
	if conn == nil {
		log.Printf("alaserv: reconcile skipped, no connection available yet")
		return
	}

	// Healthy fast-path: already registered on this connection with live
	// channels. Without this, repeated triggers would stack duplicate
	// consumers on the same queues.
	if conn == a.registeredConn && a.publishHealthy && a.consumeHealthy {
		return
	}

	// Publish channel: reuse when healthy, recreate otherwise.
	if conn != a.conn || !a.publishHealthy {
		if a.amqpChan != nil && a.publishHealthy {
			_ = a.amqpChan.Close()
		}
		amqpChan, err := conn.Channel()
		if err != nil {
			log.Printf("alaserv: reconcile: failed to create publish channel: %s; retrying", err.Error())
			a.scheduleReconcile()
			return
		}
		a.amqpChan = amqpChan
		a.publishHealthy = true
	}

	// Consume channel: recreate when unhealthy, which also forces
	// re-registration of every route (consumers die with their channel).
	if conn != a.conn || !a.consumeHealthy {
		if a.amqpChanConsume != nil && a.consumeHealthy {
			a.amqpChanConsume.Close()
		}
		amqpChanConsume, err := conn.Channel()
		if err != nil {
			log.Printf("alaserv: reconcile: failed to create consume channel: %s; retrying", err.Error())
			a.scheduleReconcile()
			return
		}
		a.amqpChanConsume = amqpChanConsume
		a.amqpChanConsume.Qos(a.nConsumePrefetch, 0, false)
		a.routesRegistered = false
		a.consumeHealthy = true
	}

	a.conn = conn

	// FIX(ownConn-leak): once the Connector provides a connection, the
	// self-dialed one from Start is abandoned — close it so a start never
	// leaks an idle connection on the broker (one per Start/reload before).
	if a.ownConn != nil && a.ownConn != a.conn {
		_ = a.ownConn.Close()
		a.ownConn = nil
	}

	if !a.routesRegistered {
		a.routeCloseSignals.CancelAll()
		if err := a.startupServer(); err != nil {
			log.Printf("alaserv: reconcile: route registration incomplete: %s; retrying", err.Error())
			a.scheduleReconcile()
			return
		}
		a.routesRegistered = true
		a.registeredConn = conn
		log.Printf("alaserv: serving %d route(s) on connection %p", len(a.rhs), conn)
	}

	// Arm close watchers: if the connection or the consume channel dies
	// (network loss, broker restart, consumer_timeout, ...), re-reconcile.
	// The generation guard keeps superseded watchers inert.
	a.consumeGen++
	gen := a.consumeGen
	connClose := conn.NotifyClose(make(chan *amqp.Error, 1))
	chClose := a.amqpChanConsume.NotifyClose(make(chan *amqp.Error, 1))
	go func() {
		select {
		case <-connClose:
		case <-chClose:
		}
		a.ready.Lock()
		stale := a.consumeGen != gen || a.stopping
		a.ready.Unlock()
		if stale {
			return
		}
		a.publishHealthy = false
		a.consumeHealthy = false
		a.routesRegistered = false
		log.Printf("alaserv: connection/channel closed, scheduling reconcile")
		a.scheduleReconcile()
	}()
}

// registerRoutes declares queues, exchanges and bindings, then starts one
// handler routine per route. Returns the first error encountered, after
// attempting all routes.
func (a *AlaServer) registerRoutes() error {
	var firstErr error
	for _, rhs := range a.rhs {
		consumeCh, err := a.registerRoute(*rhs.q, rhs.ess)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			log.Printf("alaserv: failed to register route of queue %s: %s", rhs.q.Name, err.Error())
			continue
		}
		go func(rhs routeHandlerSetting) {
			a.startHandlerRoutine(consumeCh, rhs.handler)
		}(rhs)
	}
	return firstErr
}

// Route register a new route for new handler.
func (a *AlaServer) Route(q QueueSetting, ess []ExchangeSetting, handler HandlerFunc) {
	a.rhs = append(a.rhs, routeHandlerSetting{q: &q, ess: ess, handler: handler})
}

// registerRoute declares queue and needed exchange and return consume channel used for handler routine.
func (a *AlaServer) registerRoute(q QueueSetting, ess []ExchangeSetting) (<-chan amqp.Delivery, error) {
	_, err := a.amqpChan.QueueDeclare(q.Name, !q.NotDurable, q.AutoDelete, q.Exclusive, q.NoWait, q.Args)
	if err != nil {
		log.Printf("Failed to declare queue of %s: %s", q.Name, err.Error())
		return nil, err
	}

	for _, es := range ess {
		err = a.amqpChan.ExchangeDeclare(es.Name, es.kind(), !es.NotDurable, es.AutoDelete, es.Internal, es.NoWait, es.Args)
		if err != nil {
			log.Printf("Failed to declare exchange of %s: %s", es.Name, err.Error())
			return nil, err
		}

		routeKey := es.RouteKey
		if routeKey == "" {
			routeKey = q.Name
		}

		err = a.amqpChan.QueueBind(q.Name, routeKey, es.Name, es.NoWaitQueueBind, es.ArgsQueueBind)
		if err != nil {
			log.Printf("Failed to bind exchange to queue of %s: %s", es.Name, err.Error())
			return nil, err
		}
	}

	consumeCh, err := a.amqpChanConsume.Consume(q.Name, q.ConsumerKey, !q.ManualAck, q.ExclusiveConsume, false, q.NoWaitConsume, q.ArgsConsume)
	if err != nil {
		log.Printf("Failed to consume queue: %s", err.Error())
		return nil, err
	}

	return consumeCh, nil
}

// startHandlerRoutine start routine for handler to receive delivery.
func (a *AlaServer) startHandlerRoutine(consumeCh <-chan amqp.Delivery, handler HandlerFunc) {
	for delivery := range consumeCh {
		go func(d amqp.Delivery) {
			routeCtx, cancel := context.WithCancel(a.ctx)
			defer cancel()

			a.getReplyer().Reply(d.CorrelationId, d)

			// setup close routine if successfull or in case of panic.
			routineUUID := uuid.NewV4()
			ruuidStr := routineUUID.String()
			a.routeCloseSignals.Set(ruuidStr, cancel)

			err := handler(routeCtx, d, a.amqpChan)
			if err != nil {
				d.Nack(false, true)
			}

			// delete cancelFunc from singals.
			a.routeCloseSignals.Delete(ruuidStr)
		}(delivery)
	}
}

// startupServer starts server and fires up routine for each registered handler.
func (a *AlaServer) startupServer() error {
	var firstErr error
	for _, rhs := range a.rhs {
		consumeCh, err := a.registerRoute(*rhs.q, rhs.ess)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue // keep registering the remaining routes
		}
		go func(rhs routeHandlerSetting) {
			a.startHandlerRoutine(consumeCh, rhs.handler)
		}(rhs)
	}
	return firstErr
}

// Start starts a Ala AMQP Server for this service.
func (a *AlaServer) Start(ctx context.Context) {
	a.ready.Lock()

	// FIX(nil-ctx): set the server context BEFORE any route registration so
	// handler goroutines never derive from a nil parent context.
	serverCtx, cancel := context.WithCancel(ctx)
	a.ctx = serverCtx
	a.closeSignal = cancel

	if a.conn == nil {
		// Fail fast on unrecoverable configuration errors (bad URI, wrong
		// credentials), but tolerate a broker that is merely not up yet:
		// retry a few times before treating it as fatal. NOTE: intentionally
		// NOT pulling from the Connector here — Connect() may block for a long
		// time on an unreachable broker while this method holds the ready
		// lock. reconcile() adopts the Connector's connection as soon as one
		// exists and closes this self-dialed one (ownConn).
		var conn *amqp.Connection
		var dialErr error
		for attempt := 1; attempt <= 3; attempt++ {
			conn, dialErr = amqp.Dial(a.amqpURI)
			if dialErr == nil {
				break
			}
			log.Printf("alaserv: failed to dial %s (attempt %d/3): %s", a.amqpURI, attempt, dialErr.Error())
			time.Sleep(time.Duration(attempt) * 2 * time.Second)
		}
		if dialErr != nil {
			// FIX(panic-with-lock): log.Panicf used to unwind while still
			// holding the ready lock, so any later Close() on this server
			// spun forever on it. Mark the server as stopping and release
			// the lock before panicking.
			a.stopping = true
			a.ready.Unlock()
			log.Panicf("alaserv: failed to start server: %s", dialErr.Error())
		}
		a.conn = conn
		a.ownConn = conn
	}

	a.ready.Unlock()

	// Reconcile performs channel creation + route registration (idempotent).
	a.reconcile(a.conn)
	a.printLogo()

	<-serverCtx.Done()
}

// Close closes connection and release the server loop.
func (a *AlaServer) Close() error {
	a.ready.Lock()
	a.stopping = true
	closeSignal := a.closeSignal
	publishCh := a.amqpChan
	consumeCh := a.amqpChanConsume
	ownConn := a.ownConn
	conn := a.conn
	a.ready.Unlock()

	if closeSignal != nil {
		defer closeSignal()
	}

	// Close the server's own channels so every consumer registered on them
	// is unregistered on the broker immediately. The connection itself is
	// owned by the Connector (when set) and closed by its owner; without
	// this, consumers of a previous session survive Close and keep receiving
	// deliveries that nobody processes anymore.
	if consumeCh != nil {
		_ = consumeCh.Close()
	}
	if publishCh != nil {
		_ = publishCh.Close()
	}

	// Close a still-owned self-dialed connection (the Connector-owned
	// connection is closed by its owner).
	if ownConn != nil {
		_ = ownConn.Close()
	}

	a.routeCloseSignals.CancelAll()
	a.closeConnectionMonitorRoutine()

	if a.c == nil && conn != nil {
		return conn.Close()
	}

	return nil
}

// printLogo prints ala server logo for startup.
func (a *AlaServer) printLogo() {
	uri, _ := amqp.ParseURI(a.amqpURI)
	fmt.Printf(`
  __   __     __   ____  ____  ____  _  _  ____ 
 / _\ (  )   / _\ / ___)(  __)(  _ \/ )( \(  __)
/    \/ (_/\/    \\___ \ ) _)  )   /\ \/ / ) _) 
\_/\_/\____/\_/\_/(____/(____)(__\_) \__/ (____)

Starting publish/consume at vhost of: %s
`+"\n", uri.Vhost)
}

// noopReplyer is a no-op independency injection.
type noopReplyer struct{}

// Reply implements Replyer.
func (noopReplyer) Reply(correlationID string, data amqp.Delivery) error { return nil }
