package connman

import "testing"

// Regression: methods must tolerate a typed-nil receiver. ruleengine passes
// a nil *AMQPConnectionManager as an alaserv Connector on the first session
// (r.conn is assigned one line after SetConnector), and alaserv calls
// Connect() on it — which previously dereferenced a.cond on the nil
// receiver and panicked with an invalid memory address error.
func TestConnectOnTypedNilReceiver(t *testing.T) {
	var m *AMQPConnectionManager

	if conn := m.Connect(); conn != nil {
		t.Fatal("expected nil connection from typed-nil receiver")
	}

	// RenewAMQPChannel and ReconnectSignal must be safe too.
	if _, err := m.RenewAMQPChannel(); err == nil {
		t.Fatal("expected error from RenewAMQPChannel on typed-nil receiver")
	}
	if m.ReconnectSignal() != nil {
		t.Fatal("expected nil reconnect signal from typed-nil receiver")
	}
}
