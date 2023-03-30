package activemq

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"io"
	"time"

	"github.com/go-stomp/stomp/v3"
	"github.com/go-stomp/stomp/v3/frame"
)

type ProduceMessage interface {
	QueueName() string
	Encode(ctx context.Context, w io.Writer) (contentType string, err error)
}

const (
	retryPolicyHeader = userDefinedPrefix + "retry_policy"
)

func (amq *ActiveMQ) Produce(ctx context.Context, message ProduceMessage, opts ...ProduceOption) error {
	if !amq.setConnectionType(ConnectionTypeProducer) {
		return fmt.Errorf("this connection (%s) is not for production. Use a different connection", amq.getConnectionType())
	}
	if status := amq.getStatus(); status != StatusConnected {
		return fmt.Errorf("ActiveMQ has the status %s", status.String())
	}

	o := &produceOptions{
		Headers: make(map[string]string),
	}
	for _, opt := range opts {
		opt.apply(o)
	}

	buf := bytes.NewBuffer(nil)
	contentType, err := message.Encode(ctx, buf)
	if err != nil {
		return err
	}

	sendOpts := make([]func(*frame.Frame) error, 0, len(o.Headers))
	for key, value := range o.Headers {
		sendOpts = append(sendOpts, stomp.SendOpt.Header(key, value))
	}
	queueName := amq.withQueueSuffix(message.QueueName())
	if err := amq.conn.Send(queueName, contentType, buf.Bytes(), sendOpts...); err != nil {
		return err
	}
	amq.log.Debugf("send message %q", queueName)

	return nil
}

type ProduceOption interface {
	apply(*produceOptions)
}

type produceOptions struct {
	Headers map[string]string
}

// ProduceOption :: RetryPolicy

func WithRetryPolicy(maxTries int, tryDelay time.Duration) ProduceOption {
	return retryPolicy{
		maxTries: maxTries,
		tryDelay: tryDelay,
	}
}

type retryPolicy struct {
	maxTries int
	tryDelay time.Duration
}

func (v retryPolicy) MarshalText() ([]byte, error) {
	var buf [16]byte
	binary.LittleEndian.PutUint64(buf[0:8], uint64(v.maxTries))
	binary.LittleEndian.PutUint64(buf[8:16], uint64(v.tryDelay))
	return []byte(base64.StdEncoding.EncodeToString(buf[:])), nil
}

func (v *retryPolicy) UnmarshalText(text []byte) error {
	bts, err := base64.StdEncoding.DecodeString(string(text))
	if err != nil {
		return err
	}
	if len(bts) != 16 {
		return fmt.Errorf("incorrect length %d", len(bts))
	}
	*v = retryPolicy{
		maxTries: int(binary.LittleEndian.Uint64(bts[0:8])),
		tryDelay: time.Duration(binary.LittleEndian.Uint64(bts[8:16])),
	}
	return nil
}

func (v retryPolicy) apply(o *produceOptions) {
	if text, err := v.MarshalText(); err == nil {
		o.Headers[retryPolicyHeader] = string(text)
	}
}

// ProduceOption :: Header

func WithHeader(key, value string) ProduceOption {
	return headerProduceOption{
		key:   key,
		value: value,
	}
}

type headerProduceOption struct {
	key   string
	value string
}

func (v headerProduceOption) apply(o *produceOptions) {
	o.Headers[userDefinedPrefix+v.key] = v.value
}

// ProduceOption :: Headers

func WithHeaders(h Headers) ProduceOption {
	return headersProduceOption(h)
}

type headersProduceOption Headers

func (v headersProduceOption) apply(o *produceOptions) {
	for key, value := range v {
		o.Headers[userDefinedPrefix+key] = value
	}
}
