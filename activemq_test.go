package activemq

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/cnblvr/activemq-reconnect/failover"
	"github.com/stretchr/testify/assert"
)

func TestConnect(t *testing.T) {
	container := runContainerActiveMQ(t, "test_amq", "", "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fo := WithFailover(failover.New().
		Add("tcp://localhost:" + container.portStomp).
		WithInitialReconnectDelay(time.Second).
		WithMaxReconnectDelay(time.Second * 2).
		Build())

	consumer, err := New(ctx,
		fo,
		WithLogger(newTestLogger(t, "consumer")),
	)
	if !assert.NoError(t, err) {
		t.Fatal()
	}
	defer consumer.Close(ctx)

	producer, err := New(ctx,
		fo,
		WithLogger(newTestLogger(t, "producer")),
	)
	if !assert.NoError(t, err) {
		t.Fatal()
	}
	defer producer.Close(ctx)

	if !checkStatus(t, consumer, StatusConnected) {
		return
	}
	if !checkStatus(t, producer, StatusConnected) {
		return
	}

	t.Run("connect", func(t *testing.T) {
		t.Log(producer.Status())
		t.Log(producer.Session())
		t.Log(producer.Version())
	})

	t.Run("reconnect", func(t *testing.T) {
		container.Stop()
		container.Start()

		if !checkStatus(t, consumer, StatusConnected) {
			return
		}
		if !checkStatus(t, producer, StatusConnected) {
			return
		}
	})

	t.Run("consume health", func(t *testing.T) {
		err = consumer.Consume(ctx, (*healthQueue)(nil), func(_ context.Context, _ ConsumeMessage, _ Headers) error {
			return nil
		})
		assert.Error(t, err)
	})

	t.Run("send and receive", func(t *testing.T) {
		want := newRandomTestMessage()

		wantHeaders := Headers{
			"multiple": "I'm not alone",
			"empty":    "",
		}
		const wantSingleHeaderKey, wantSingleHeaderValue = "reqid", "123456"

		// initialize a receiver
		var wg sync.WaitGroup
		wg.Add(1)
		go func() {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			if err := consumer.Consume(ctx, (*testMessage)(nil), func(ctx context.Context, got ConsumeMessage, headers Headers) error {
				defer wg.Done()
				defer cancel()
				assert.Equal(t, want.Message, got.(*testMessage).Message)

				for key, value := range wantHeaders {
					assert.Equal(t, value, headers[key])
				}
				assert.Equal(t, wantSingleHeaderValue, headers[wantSingleHeaderKey])
				return nil
			}); err != nil {
				t.Fatal(err)
			}
		}()

		// send a message
		if err := producer.Produce(ctx, want,
			WithHeader(wantSingleHeaderKey, wantSingleHeaderValue),
			WithHeaders(wantHeaders),
		); err != nil {
			t.Fatal(err)
		}

		wg.Wait()
	})

	t.Run("connection type", func(t *testing.T) {
		assert.Equal(t, ConnectionTypeConsumer, consumer.ConnectionType())
		assert.Equal(t, ConnectionTypeProducer, producer.ConnectionType())
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

	fo := WithFailover(failover.New().
		Add("tcp://localhost:" + container.portStomp).
		WithInitialReconnectDelay(time.Second).
		WithMaxReconnectDelay(time.Second * 2).
		Build())

	consumer, err := New(ctx,
		fo,
		WithLogger(newTestLogger(t, "consumer")),
	)
	if !assert.NoError(t, err) {
		t.Fatal()
	}
	defer consumer.Close(ctx)

	producer, err := New(ctx,
		fo,
		WithLogger(newTestLogger(t, "producer")),
	)
	if !assert.NoError(t, err) {
		t.Fatal()
	}
	defer producer.Close(ctx)

	if !checkStatus(t, consumer, StatusConnected) {
		return
	}
	if !checkStatus(t, producer, StatusConnected) {
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
		if err := consumer.Consume(ctxReceiver, (*testMessage)(nil), func(ctx context.Context, got ConsumeMessage, headers Headers) error {
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
			if err := producer.Produce(ctx, msg); err != nil {
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

	if !checkStatus(t, consumer, StatusConnected) {
		return
	}
	if !checkStatus(t, producer, StatusConnected) {
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

func TestRetries(t *testing.T) {
	container := runContainerActiveMQ(t, "test_amq", "", "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fo := WithFailover(failover.New().
		Add("tcp://localhost:" + container.portStomp).
		WithInitialReconnectDelay(time.Second).
		WithMaxReconnectDelay(time.Second * 2).
		Build())

	consumer, err := New(ctx,
		fo,
		WithLogger(newTestLogger(t, "consumer")),
	)
	if !assert.NoError(t, err) {
		t.Fatal()
	}
	defer consumer.Close(ctx)

	producer, err := New(ctx,
		fo,
		WithLogger(newTestLogger(t, "producer")),
	)
	if !assert.NoError(t, err) {
		t.Fatal()
	}
	defer producer.Close(ctx)

	if !checkStatus(t, consumer, StatusConnected) {
		return
	}
	if !checkStatus(t, producer, StatusConnected) {
		return
	}

	want := newRandomTestMessage()
	wantMaxTries, wantTryDelay := 3, time.Millisecond*500

	const wantHeaderKey, wantHeaderValue = "key", "value"

	gotMessages := new(int32)

	// initialize a receiver
	var wg sync.WaitGroup
	wg.Add(wantMaxTries + 1)
	go func(gotMessages *int32) {
		if err := consumer.Consume(ctx, (*testMessage)(nil), func(ctx context.Context, got ConsumeMessage, headers Headers) error {
			defer wg.Done()
			atomic.AddInt32(gotMessages, 1)
			assert.Equal(t, want.Message, got.(*testMessage).Message)
			assert.Equal(t, wantHeaderValue, headers[wantHeaderKey])
			return fmt.Errorf("test error")
		}); err != nil {
			t.Log(err)
		}
	}(gotMessages)

	startTime := time.Now()

	// send a message
	if err := producer.Produce(ctx, want,
		WithRetryPolicy(wantMaxTries, wantTryDelay),
		WithHeader(wantHeaderKey, wantHeaderValue),
	); err != nil {
		t.Fatal(err)
	}

	wg.Wait()
	assert.LessOrEqual(t, wantTryDelay*time.Duration(wantMaxTries), time.Since(startTime))
	assert.GreaterOrEqual(t, wantTryDelay*time.Duration(wantMaxTries)*3, time.Since(startTime))

	<-time.NewTimer(time.Second).C
	assert.Equal(t, wantMaxTries, int(atomic.LoadInt32(gotMessages))-1)
}

func TestDiversify(t *testing.T) {
	container1 := runContainerActiveMQ(t, "test_amq1", "61613", "")
	portStomp2 := "62613"

	ctx := context.Background()

	fo := WithFailover(failover.New().
		Add("tcp://localhost:" + container1.portStomp).
		Add("tcp://localhost:" + portStomp2).
		WithRandomize(false).
		WithInitialReconnectDelay(time.Second).
		WithMaxReconnectDelay(time.Second * 2).
		Build())

	consumer, err := New(ctx,
		fo,
		WithLogger(newTestLogger(t, "consumer")),
	)
	if !assert.NoError(t, err) {
		t.Fatal()
	}
	defer consumer.Close(ctx)

	producer, err := New(ctx,
		fo,
		WithLogger(newTestLogger(t, "producer")),
	)
	if !assert.NoError(t, err) {
		t.Fatal()
	}
	defer producer.Close(ctx)

	if !checkStatus(t, consumer, StatusConnected) {
		return
	}
	if !checkStatus(t, producer, StatusConnected) {
		return
	}

	container1.Kill()

	if !checkStatus(t, consumer, StatusReconnecting) {
		return
	}
	if !checkStatus(t, producer, StatusReconnecting) {
		return
	}

	container2 := runContainerActiveMQ(t, "test_amq2", portStomp2, "")
	_ = container2

	if !checkStatus(t, consumer, StatusConnected) {
		return
	}
	if !checkStatus(t, producer, StatusConnected) {
		return
	}
}

func TestPersistency(t *testing.T) {
	container := runContainerActiveMQ(t, "test_amq", "", "")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	fo := WithFailover(failover.New().
		Add("tcp://localhost:" + container.portStomp).
		WithInitialReconnectDelay(time.Second).
		WithMaxReconnectDelay(time.Second * 2).
		Build())

	consumer, err := New(ctx,
		fo,
		WithLogger(newTestLogger(t, "consumer")),
	)
	if !assert.NoError(t, err) {
		t.Fatal()
	}
	defer consumer.Close(ctx)

	producer, err := New(ctx,
		fo,
		WithLogger(newTestLogger(t, "producer")),
	)
	if !assert.NoError(t, err) {
		t.Fatal()
	}
	defer producer.Close(ctx)

	if !checkStatus(t, consumer, StatusConnected) {
		return
	}
	if !checkStatus(t, producer, StatusConnected) {
		return
	}

	const wantAmountMessages = 100

	ctxSend, cancelSend := context.WithCancel(ctx)
	defer cancelSend()
	ctxReceiver, cancelReceiver := context.WithCancel(ctx)
	defer cancelReceiver()
	var messages sync.Map

	var (
		sentMessages, receivedMessages int
	)

	// initialize a sender
	go func() {
		defer cancelSend()
		ticker := time.NewTicker(time.Millisecond * 50)
		for i := 0; i < wantAmountMessages; i++ {
			msg := newRandomTestMessage()
			if err := producer.Produce(ctx, msg); err != nil {
				t.Log(err)
				i--
			} else {
				messages.Store(msg.Message, struct{}{})
				sentMessages++
			}
			select {
			case <-ctxSend.Done():
				return
			case <-ticker.C:
			}
		}
	}()

	// initialize a receiver
	go func() {
		time.Sleep(time.Second)
		if err := consumer.Consume(ctxReceiver, (*testMessage)(nil), func(ctx context.Context, got ConsumeMessage, headers Headers) error {
			messages.Delete(got.(*testMessage).Message)
			receivedMessages++
			return nil
		}); err != nil {
			t.Log(err)
		}
	}()

	time.Sleep(time.Second * 2)
	container.Kill()
	container.Start()

	if !checkStatus(t, consumer, StatusConnected) {
		return
	}
	if !checkStatus(t, producer, StatusConnected) {
		return
	}

	<-ctxSend.Done()
	time.Sleep(time.Microsecond * 900)
	cancelReceiver()

	t.Logf("sent: %d; received: %d; lost: %d", sentMessages, receivedMessages, sentMessages-receivedMessages)
	assert.GreaterOrEqual(t, 10, sentMessages-receivedMessages)
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
