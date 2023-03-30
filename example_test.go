package activemq_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os/signal"
	"syscall"
	"time"

	"github.com/cnblvr/activemq-reconnect"
)

func ExampleNew() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	amq, err := activemq.New(ctx,
		activemq.WithFailoverStr("failover:(tcp://localhost:61613,tcp://localhost:62613)?randomize=false"),
		//activemq.WithQueueSuffix("suffix"),
		activemq.WithConnectionType(activemq.ConnectionTypeProducer),
	)
	if err != nil {
		panic(err)
	}

	defer amq.Close(ctx)

	if err := amq.WaitConnection(time.Second * 10); err != nil {
		panic(err)
	}
}

var (
	// activemq.WithConnectionType(activemq.ConnectionTypeConsumer)
	amqConsumer activemq.ActiveMQ

	// activemq.WithConnectionType(activemq.ConnectionTypeProducer)
	amqProducer activemq.ActiveMQ
)

func ExampleActiveMQ_Produce() {
	ctx := context.Background()

	// ... initialize ActiveMQ
	// activemq.WithConnectionType(activemq.ConnectionTypeProducer)

	if err := amqProducer.Produce(ctx, &MyMessage{Text: "send it!"}, activemq.WithRetryPolicy(10, time.Second*5)); err != nil {
		panic(err)
	}
}

func ExampleActiveMQ_Consume_graceful_shutdown() {
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// ... initialize ActiveMQ
	// activemq.WithConnectionType(activemq.ConnectionTypeConsumer)

	go func() {
		if err := amqConsumer.Consume(ctx, (*MyMessage)(nil), func(_ context.Context, message activemq.ConsumeMessage, _ activemq.Headers) error {
			msg := message.(*MyMessage)
			if msg.Text == "send it!" {
				fmt.Print("awesome!")
			}
			return nil
		}); err != nil {
			panic(err)
		}
	}()
}

func ExampleActiveMQ_Consume_once() {
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(time.Hour))
	defer cancel()

	// ... initialize ActiveMQ
	// activemq.WithConnectionType(activemq.ConnectionTypeConsumer)

	if err := amqConsumer.Consume(ctx, (*MyMessage)(nil), func(_ context.Context, message activemq.ConsumeMessage, _ activemq.Headers) error {
		msg := message.(*MyMessage)
		if msg.Text == "send it!" {
			fmt.Print("awesome!")
		}
		return nil
	}); err != nil {
		panic(err)
	}
}

func ExampleWithHeader() {
	go func() {
		if err := amqConsumer.Consume(context.Background(), (*MyMessage)(nil), func(_ context.Context, message activemq.ConsumeMessage, headers activemq.Headers) error {
			if v, _ := headers.Get("with"); v == "header" {
				fmt.Print("awesome!")
			}
			return nil
		}); err != nil {
			panic(err)
		}
	}()

	if err := amqProducer.Produce(context.Background(), &MyMessage{Text: "send it!"}, activemq.WithHeader("with", "header")); err != nil {
		panic(err)
	}
}

type MyMessage struct {
	Text string
}

func (v *MyMessage) QueueName() string { return "my_queue" }

func (v *MyMessage) Encode(_ context.Context, w io.Writer) (string, error) {
	return "application/json", json.NewEncoder(w).Encode(v)
}

func (v *MyMessage) Decode(_ context.Context, _ string, r io.Reader) error {
	return json.NewDecoder(r).Decode(v)
}
