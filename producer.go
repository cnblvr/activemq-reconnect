package activemq

import (
	"bytes"
	"context"
	"fmt"
	"io"
)

type ProduceMessage interface {
	QueueName() string
	Encode(ctx context.Context, w io.Writer) (contentType string, err error)
}

func (amq *ActiveMQ) Produce(ctx context.Context, message ProduceMessage) error {
	if status := amq.getStatus(); status != StatusConnected {
		return fmt.Errorf("ActiveMQ has the status %s", status.String())
	}
	buf := bytes.NewBuffer(nil)
	contentType, err := message.Encode(ctx, buf)
	if err != nil {
		return err
	}
	queueName := amq.withQueueSuffix(message.QueueName())
	if err := amq.conn.Send(queueName, contentType, buf.Bytes()); err != nil {
		return err
	}
	amq.log.Debugf("send message %q", queueName)
	return nil
}
