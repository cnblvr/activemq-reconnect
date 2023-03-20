package activemq

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"time"

	"github.com/go-stomp/stomp/v3"
)

type ConsumeMessage interface {
	QueueName() string
	Decode(ctx context.Context, contentType string, r io.Reader) error
}

type ConsumeHandler func(ctx context.Context, message ConsumeMessage) error

func newConsumeMessage(messageNil ConsumeMessage) ConsumeMessage {
	return reflect.New(reflect.TypeOf(messageNil).Elem()).Interface().(ConsumeMessage)
}

type subscriptionInfo struct {
	queueName string
}

func (amq *ActiveMQ) Consume(ctx context.Context, messageNil ConsumeMessage, executeFn ConsumeHandler) error {
	queueName := newConsumeMessage(messageNil).QueueName()
	if queueName == amq.options.healthQueueNameOption {
		return fmt.Errorf("the queue name can't be used as %q", queueName)
	}
	queueName = amq.withQueueSuffix(queueName)

	amq.subscriptionsInfo.Store(queueName, subscriptionInfo{
		queueName: queueName,
	})
	err := amq.subscribe(ctx, queueName)
	if err != nil {
		return err
	}
	for {
		for status := amq.getStatus(); status != StatusConnected; status = amq.getStatus() {
			select {
			case <-ctx.Done():
				go amq.unsubscribe(ctx, queueName)
				return nil
			default:
			}
			if status == StatusClosed {
				go amq.unsubscribe(ctx, queueName)
				return nil
			}
			timer := time.NewTimer(time.Millisecond * 100)
			<-timer.C
		}
		sub, ok := amq.getSubscription(queueName)
		if !ok {
			return fmt.Errorf("subscription %q not found", queueName)
		}
		select {
		case <-ctx.Done():
			go amq.unsubscribe(ctx, queueName)
			return nil
		case stompMessage := <-sub.C:
			if stompMessage == nil {
				continue
			}
			if stompMessage.Err != nil {
				amq.log.Warnf("queue %q has error in message: %v", queueName, stompMessage.Err)
				continue
			}
			amq.log.Debugf("receive message %q", sub.Destination())
			buf := bytes.NewBuffer(stompMessage.Body)
			message := newConsumeMessage(messageNil)
			if err := message.Decode(ctx, stompMessage.ContentType, buf); err != nil {
				amq.log.Errorf("message %q decode error: %v", queueName, err)
				continue
			}
			err := executeFn(ctx, message)
			if err != nil {
				nackErr := amq.conn.Nack(stompMessage)
				if nackErr != nil {
					amq.log.Errorf("message %q Nack error: %v", queueName, err)
				}
				continue
			}
			ackErr := amq.conn.Ack(stompMessage)
			if ackErr != nil {
				amq.log.Errorf("message %q Ack error: %v", queueName, err)
			}
			continue
		}
	}
}

func (amq *ActiveMQ) getSubscription(queueName string) (*stomp.Subscription, bool) {
	amq.subscriptionsMx.Lock()
	defer amq.subscriptionsMx.Unlock()

	subAny, ok := amq.subscriptions.Load(queueName)
	if !ok {
		return nil, false
	}
	return subAny.(*stomp.Subscription), true
}

func (amq *ActiveMQ) subscribe(ctx context.Context, queueName string) error {
	amq.subscriptionsMx.Lock()
	defer amq.subscriptionsMx.Unlock()

	if _, ok := amq.subscriptions.Load(queueName); ok {
		return fmt.Errorf("subscription %s already exists", queueName)
	}

	sub, err := amq.conn.Subscribe(queueName, stomp.AckAuto) //TODO ack
	if err != nil {
		return err
	}

	amq.subscriptions.Store(queueName, sub)

	return nil
}

func (amq *ActiveMQ) unsubscribe(ctx context.Context, queueName string) error {
	amq.subscriptionsMx.Lock()
	subAny, ok := amq.subscriptions.LoadAndDelete(queueName)
	amq.subscriptionsInfo.Delete(queueName)
	amq.subscriptionsMx.Unlock()
	if !ok {
		return nil
	}

	if err := subAny.(*stomp.Subscription).Unsubscribe(); err != nil {
		return err
	}

	return nil
}
