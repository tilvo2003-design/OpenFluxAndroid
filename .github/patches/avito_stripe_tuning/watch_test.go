package avito

import (
	"sync/atomic"
	"testing"
	"time"
)

// A room the far side does not read gets no data: the client's reader of the
// exit's send room 0 is gone, and the exit stops writing into that room while
// the stream goes on, whole and in order, over the other lanes.
func TestNoDataIntoARoomNobodyReads(t *testing.T) {
	newFakeAvito(t, 2*time.Millisecond)
	exit, client := startStripedPair(t, fastConfig())
	room0 := exit.txWss[0].room()
	client.rxWss[0].moveTo("no-such-room") // the client stops reading room 0
	waitFor(t, 5*time.Second, "the client's reader of room 0 down", func() bool { return !client.rxWss[0].connected.Load() })
	// Its watch list reaches the exit with the client's acks: give it a stream.
	var got atomic.Int64
	done := make(chan struct{})
	go func() { defer close(done); expectInOrder(t, client, 150, 30*time.Second, &got) }()
	sendWindowed(t, exit, 150, 256<<10, &got, done)
	<-done
	before := exit.txWss[0].stats.opsSent.Load()
	got.Store(0)
	done = make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 150; i++ {
			select {
			case <-client.got:
			case <-time.After(30 * time.Second):
				t.Error("the second stream stalled")
				return
			}
		}
	}()
	for i := 0; i < 150; i++ {
		if err := exit.Send(seqFrame(i)); err != nil {
			t.Fatal(err)
		}
		time.Sleep(2 * time.Millisecond)
	}
	<-done
	if after := exit.txWss[0].stats.opsSent.Load(); after != before {
		t.Fatalf("the exit wrote %d ops into room %s, which nobody reads", after-before, room0[:8])
	}
}
