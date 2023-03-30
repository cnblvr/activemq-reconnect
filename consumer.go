package activemq

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"
)

type ConsumeMessage interface {
	QueueName() string
	Decode(ctx context.Context, contentType string, r io.Reader) error
}

type ConsumeHandler func(ctx context.Context, message ConsumeMessage, headers Headers) error

func newConsumeMessage(messageNil ConsumeMessage) ConsumeMessage {
	return reflect.New(reflect.TypeOf(messageNil).Elem()).Interface().(ConsumeMessage)
}

type subscriptionInfo struct {
	queueName string
}

func (amq *ActiveMQ) Consume(ctx context.Context, messageNil ConsumeMessage, executeFn ConsumeHandler) error {
	if !amq.setConnectionType(ConnectionTypeConsumer) {
		return fmt.Errorf("this connection (%s) is not for consumption. Use a different connection", amq.getConnectionType())
	}
	queueName := newConsumeMessage(messageNil).QueueName()
	if queueName == amq.options.healthQueueName {
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
			udHeaders := make(Headers)
			for i := 0; i < stompMessage.Header.Len(); i++ {
				key, value := stompMessage.Header.GetAt(i)
				if !strings.HasPrefix(key, userDefinedPrefix) {
					continue
				}
				udHeaders.Set(strings.TrimPrefix(key, userDefinedPrefix), value)
			}
			err := executeFn(ctx, message, udHeaders)
			if err != nil {
				if !amq.retryMessage(stompMessage, message, udHeaders) {
					nackErr := amq.conn.Nack(stompMessage)
					if nackErr != nil {
						amq.log.Errorf("message %q Nack error: %v. Orig error: %v", queueName, nackErr, err)
					} else {
						amq.log.Debugf("message %q nacked by error: %v", queueName, err)
					}
					continue
				}
				// ack message because it was resent
			}
			if stompMessage.ShouldAck() {
				ackErr := amq.conn.Ack(stompMessage)
				if ackErr != nil {
					amq.log.Errorf("message %q Ack error: %v", queueName, ackErr)
				} else {
					amq.log.Debugf("message %q acked", queueName)
				}
			}
			continue
		}
	}
}

const (
	userDefinedPrefix       = "##"
	packagePrefix           = "#"
	retryNoHeader           = packagePrefix + "retry_no"
	amqScheduledDelayHeader = "AMQ_SCHEDULED_DELAY"
)

func (amq *ActiveMQ) retryMessage(stompMessage *stomp.Message, message ConsumeMessage, udHeaders Headers) bool {
	var rp retryPolicy
	if h := stompMessage.Header.Get(retryPolicyHeader); len(h) != 0 {
		if err := rp.UnmarshalText([]byte(h)); err != nil {
			amq.log.Errorf("parse %q header error: %v", retryPolicyHeader, err)
			return false
		}
	}
	if rp.maxTries <= 0 {
		return false
	}

	var retryNo int
	if retryNoStr, ok := stompMessage.Header.Contains(retryNoHeader); ok {
		var err error
		if retryNo, err = strconv.Atoi(retryNoStr); err != nil {
			amq.log.Errorf("message %q has incorrect value of header %s", stompMessage.Destination, retryNoHeader)
		}
	}

	if retryNo != 0 && retryNo >= rp.maxTries {
		amq.log.Errorf("maximum retry %d/%d for message %q", retryNo, rp.maxTries, stompMessage.Destination)
		return false
	}

	retryNo++

	sendOpts := make([]func(*frame.Frame) error, 0, stompMessage.Header.Len())
	for key, value := range udHeaders {
		sendOpts = append(sendOpts, stomp.SendOpt.Header(userDefinedPrefix+key, value))
	}
	sendOpts = append(sendOpts, stomp.SendOpt.Header(retryNoHeader, strconv.Itoa(retryNo)))
	sendOpts = append(sendOpts, stomp.SendOpt.Header(amqScheduledDelayHeader, strconv.FormatInt(rp.tryDelay.Milliseconds(), 10)))
	if err := amq.conn.Send(stompMessage.Destination, stompMessage.ContentType, stompMessage.Body, sendOpts...); err != nil {
		amq.log.Errorf("failed to resend message %q", stompMessage.Destination)
		return false
	}
	amq.log.Debugf("message %q retried %d/%d for %s", stompMessage.Destination, retryNo, rp.maxTries, rp.tryDelay.String())

	return true
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

	sub, err := amq.conn.Subscribe(queueName, stomp.AckClientIndividual) //TODO ack
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

type Headers map[string]string

func (h Headers) Get(key string) (string, bool) {
	v, ok := h[key]
	return v, ok
}

func (h Headers) Set(key, value string) {
	h[key] = value
}
