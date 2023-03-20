package activemq

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"testing"
	"time"

	"github.com/cnblvr/activemq-reconnect/failover"
	"github.com/stretchr/testify/assert"
)

func TestConnect(t *testing.T) {
	container := runContainerActiveMQ(t, "test_amq", "", "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	amq, err := New(ctx,
		WithFailover(failover.New().
			Add("localhost:"+container.portStomp).
			WithInitialReconnectDelay(time.Second).
			WithMaxReconnectDelay(time.Second*2).
			Build()),
		WithLogger(newTestLogger(t)),
	)
	if !assert.NoError(t, err) {
		t.Fatal()
	}

	defer amq.Close(ctx)

	if !checkStatus(t, amq, StatusConnected) {
		return
	}

	t.Run("connect", func(t *testing.T) {
		t.Log(amq.Status())
		t.Log(amq.Session())
		t.Log(amq.Version())
	})

	t.Run("reconnect", func(t *testing.T) {
		container.Stop()
		container.Start()

		if !checkStatus(t, amq, StatusConnected) {
			return
		}
	})

	t.Run("consume health", func(t *testing.T) {
		err = amq.Consume(ctx, (*healthQueue)(nil), func(ctx context.Context, got ConsumeMessage) error {
			return nil
		})
		assert.Error(t, err)
	})

	t.Run("send and receive", func(t *testing.T) {
		want := newRandomTestMessage()

		// initialize a receiver
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			if err := amq.Consume(ctx, (*testMessage)(nil), func(ctx context.Context, got ConsumeMessage) error {
				wg.Done()
				cancel()
				assert.Equal(t, want.Message, got.(*testMessage).Message)
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}()

		// send a message
		if err := amq.Produce(ctx, want); err != nil {
			t.Fatal(err)
		}

		wg.Wait()
	})

	cancel()
}

type healthQueue struct{}

func (m *healthQueue) QueueName() string                                     { return "health" }
func (m *healthQueue) Encode(_ context.Context, _ io.Writer) (string, error) { panic(nil) }
func (m *healthQueue) Decode(_ context.Context, _ string, _ io.Reader) error { panic(nil) }

type testMessage struct {
	Message string `json:"message"`
}

func newRandomTestMessage() *testMessage {
	return &testMessage{Message: fmt.Sprintf("%08x", rand.Uint64())}
}

func (m *testMessage) QueueName() string { return "test_queue" }

func (m *testMessage) Encode(_ context.Context, w io.Writer) (string, error) {
	return "application/json", json.NewEncoder(w).Encode(m)
}

func (m *testMessage) Decode(_ context.Context, _ string, r io.Reader) error {
	return json.NewDecoder(r).Decode(m)
}

func TestLostMessages(t *testing.T) {
	container := runContainerActiveMQ(t, "test_amq", "", "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	amq, err := New(ctx,
		WithFailover(failover.New().
			Add("localhost:"+container.portStomp).
			WithInitialReconnectDelay(time.Second).
			WithMaxReconnectDelay(time.Second*2).
			Build()),
		WithLogger(newTestLogger(t)),
	)
	if !assert.NoError(t, err) {
		t.Fatal()
	}

	defer amq.Close(ctx)

	if !checkStatus(t, amq, StatusConnected) {
		return
	}

	ctxSend, cancelSend := context.WithCancel(ctx)
	defer cancelSend()
	ctxReceiver, cancelReceiver := context.WithCancel(ctx)
	defer cancelReceiver()
	var messages sync.Map

	// initialize a receiver
	go func() {
		defer cancel()
		if err := amq.Consume(ctxReceiver, (*testMessage)(nil), func(ctx context.Context, got ConsumeMessage) error {
			messages.Delete(got.(*testMessage).Message)
			return nil
		}); err != nil {
			t.Log(err)
		}
	}()

	// initialize a sender
	go func() {
		ticker := time.NewTicker(time.Millisecond * 50)
		for {
			msg := newRandomTestMessage()
			if err := amq.Produce(ctx, msg); err != nil {
				t.Log(err)
			} else {
				messages.Store(msg.Message, struct{}{})
			}
			select {
			case <-ctxSend.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	container.Kill()
	container.Start()

	if !checkStatus(t, amq, StatusConnected) {
		return
	}

	time.Sleep(time.Second)
	cancelSend()

	for i := 0; i < 10; i++ {
		hasMessages := false
		messages.Range(func(_, _ interface{}) bool {
			hasMessages = true
			return false
		})
		if !hasMessages {
			break
		}
		<-time.NewTimer(time.Second).C
	}
}

func TestDiversify(t *testing.T) {
	container1 := runContainerActiveMQ(t, "test_amq1", "61613", "")
	portStomp2 := "62613"

	ctx := context.Background()

	amq, err := New(ctx,
		WithFailover(failover.New().
			Add("localhost:"+container1.portStomp).
			Add("localhost:"+portStomp2).
			WithRandomize(false).
			WithInitialReconnectDelay(time.Second).
			WithMaxReconnectDelay(time.Second*2).
			Build()),
		WithLogger(newTestLogger(t)),
	)
	if !assert.NoError(t, err) {
		t.Fatal()
	}

	defer amq.Close(ctx)

	if !checkStatus(t, amq, StatusConnected) {
		return
	}

	container1.Kill()

	if !checkStatus(t, amq, StatusReconnecting) {
		return
	}

	container2 := runContainerActiveMQ(t, "test_amq2", portStomp2, "")
	_ = container2

	if !checkStatus(t, amq, StatusConnected) {
		return
	}
}

func checkStatus(t *testing.T, amq *ActiveMQ, status Status) bool {
	for i := 0; i < 10; i++ {
		if amq.Status() == status {
			break
		}
		<-time.NewTimer(time.Second).C
	}
	return assert.Equal(t, status, amq.Status())
}
