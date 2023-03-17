package activemq

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cnblvr/activemq-reconnect/failover"
	"github.com/go-stomp/stomp/v3"
)

type ActiveMQ struct {
	url    string
	conn   *stomp.Conn
	health *stomp.Subscription

	status            uint32
	reconnectMx       sync.Mutex
	subscriptions     sync.Map
	subscriptionsInfo sync.Map
	subscriptionsMx   sync.Mutex

	*options
}

func New(ctx context.Context, opts ...Option) (*ActiveMQ, error) {
	amq := &ActiveMQ{}
	amq.setStatus(StatusConnecting)

	amq.options = &options{
		log:                   &emptyLogger{},
		healthQueueNameOption: "health",
	}
	for _, opt := range opts {
		opt.apply(amq.options)
	}

	if amq.options.healthQueueNameOption == "" {
		return nil, fmt.Errorf("health queue name is empty")
	}

	// parse the failoverStr or clone the failover from options
	if len(amq.options.failover.URLs) == 0 {
		fo, err := failover.Parse(amq.options.failoverStr)
		if err != nil {
			return nil, err
		}
		amq.failover = fo
	} else {
		amq.failover = amq.options.failover.Clone()
	}

	err := amq.attemptToReconnect(ctx, failover.InfiniteReconnectAttempts(amq.failover.StartupMaxReconnectAttempts))
	if err != nil {
		return nil, err
	}
	amq.setStatus(StatusConnected)

	go amq.checkHealth(ctx)

	return amq, nil
}

func (amq *ActiveMQ) WaitConnection(d time.Duration) error {
	deadline := time.NewTimer(d)
	ticker := time.NewTicker(time.Millisecond * 80)
	for {
		if amq.getStatus() == StatusConnected {
			return nil
		}
		select {
		case <-deadline.C:
			return fmt.Errorf("failed to connect to ActiveMQ within %s", d)
		case <-ticker.C:
		}
	}
}

type Status uint32

const (
	StatusUnknown Status = iota
	StatusConnecting
	StatusConnected
	StatusReconnecting
	StatusClosed
)

const _statusString = "UnknownConnectingConnectedReconnectingClosed"

var _statusMap = map[Status]string{
	StatusUnknown:      _statusString[0:7],
	StatusConnecting:   _statusString[7:17],
	StatusConnected:    _statusString[17:26],
	StatusReconnecting: _statusString[26:38],
	StatusClosed:       _statusString[38:44],
}

func (s Status) String() string {
	str, ok := _statusMap[s]
	if !ok {
		return _statusMap[StatusUnknown]
	}
	return str
}

func (amq *ActiveMQ) Status() Status { return amq.getStatus() }

func (amq *ActiveMQ) getStatus() Status  { return Status(atomic.LoadUint32(&amq.status)) }
func (amq *ActiveMQ) setStatus(s Status) { atomic.StoreUint32(&amq.status, uint32(s)) }

func (amq *ActiveMQ) checkHealth(ctx context.Context) {
	healthTicker := time.NewTicker(time.Millisecond * 100)
	for {
		select {
		case <-ctx.Done():
			return
		case <-healthTicker.C:
			if amq.getStatus() == StatusClosed {
				return
			}
			if amq.health.Active() {
				continue
			}
			amq.setStatus(StatusReconnecting)

			amq.log.Debugf("reconnecting because %q queue is inactive...", amq.health.Destination())

			amq.reconnectMx.Lock()

			err := amq.attemptToReconnect(ctx, failover.InfiniteReconnectAttempts(amq.failover.MaxReconnectAttempts()))
			switch err {
			case nil:
			case context.Canceled, context.DeadlineExceeded:
				amq.reconnectMx.Unlock()
				amq.Close(ctx)
				return
			default:
				amq.log.Errorf("error: %v", err)
				amq.reconnectMx.Unlock()
				amq.Close(ctx)
				return
			}

			amq.reconnectMx.Unlock()
			amq.setStatus(StatusConnected)
			amq.log.Debugf("reconnect successfully")
		}
	}
}

func (amq *ActiveMQ) attemptToReconnect(ctx context.Context, maxAttempts int) error {
	var attempts int
	reconnectTimer := time.NewTimer(0)
	for attempts < maxAttempts {
		attempts++
		delay := amq.failover.MaxReconnectDelay()
		if attempts == 1 {
			delay = amq.failover.InitialReconnectDelay()
		}

		err := amq.reconnect(ctx)
		if err == nil {
			amq.log.Debugf("successfully reconnected after %d attempts", attempts)
			return nil
		}

		amq.log.Warnf("unable to complete reconnect: %v; retrying in %s", err, delay)
		reconnectTimer.Reset(delay)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-reconnectTimer.C:
		}
	}
	return fmt.Errorf("the maximum number of attempts (%d) has been reached", maxAttempts)
}

func (amq *ActiveMQ) reconnect(ctx context.Context) error {
	if err := amq.close(ctx); err != nil {
		amq.log.Warnf("close unsuccessfully: %v", err)
	}

	var conn *stomp.Conn
	var err error

	for iter := amq.failover.FirstURL(); iter.HasURL(); iter.Next() {
		url := iter.URL()
		connOpts := []func(*stomp.Conn) error{
			stomp.ConnOpt.HeartBeat(0, 0),
			stomp.ConnOpt.Logger(&stompLogger{amq.log}),
		}
		if len(amq.options.login) > 0 {
			connOpts = append(connOpts, stomp.ConnOpt.Login(amq.options.login, amq.options.passcode))
		}
		conn, err = amq.dialContext(ctx, url.URL, connOpts...)
		if err != nil {
			amq.log.Warnf("connect to %q error: %v", url.URL, err)
		} else {
			amq.url = url.URL
			break
		}
	}
	if err != nil {
		return fmt.Errorf("all servers failed on reconnect. Last error: %v", err)
	}
	amq.log.Debugf("connected to %q", amq.url)

	amq.conn = conn

	if err := amq.subscribeHealth(); err != nil {
		return err
	}

	// resubscribe
	amq.subscriptions = sync.Map{}
	var subscribeErr error
	amq.subscriptionsInfo.Range(func(queueName, subInfoAny interface{}) bool {
		subInfo := subInfoAny.(subscriptionInfo)
		if err := amq.subscribe(ctx, subInfo.queueName); err != nil {
			subscribeErr = fmt.Errorf("failed to subscribe to %q queue: %v", subInfo.queueName, err)
			return false
		}
		return true
	})
	if subscribeErr != nil {
		return subscribeErr
	}

	return nil
}

func (amq *ActiveMQ) subscribeHealth() error {
	queueName := amq.withQueueSuffix(amq.options.healthQueueNameOption)
	sub, err := amq.conn.Subscribe(queueName, stomp.AckAuto)
	if err != nil {
		return fmt.Errorf("failed to subscribe to %q: %v", queueName, err)
	}
	amq.health = sub
	return nil
}

func (amq *ActiveMQ) dialContext(ctx context.Context, addr string, opts ...func(*stomp.Conn) error) (*stomp.Conn, error) {
	dialer := net.Dialer{}
	c, err := dialer.DialContext(ctx, "tcp", addr)
	if err != nil {
		return nil, err
	}

	host, _, err := net.SplitHostPort(c.RemoteAddr().String())
	if err != nil {
		c.Close()
		return nil, err
	}

	// Add option to set host and make it the first option in list,
	// so that if host has been explicitly specified it will override.
	opts = append([]func(*stomp.Conn) error{stomp.ConnOpt.Host(host)}, opts...)

	return stomp.Connect(c, opts...)
}

func (amq *ActiveMQ) Close(ctx context.Context) error {
	amq.reconnectMx.Lock()
	defer amq.reconnectMx.Unlock()
	err := amq.close(ctx)
	amq.setStatus(StatusClosed)
	return err
}

func (amq *ActiveMQ) close(ctx context.Context) error {
	amq.subscriptions.Range(func(queueName, subAny interface{}) bool {
		sub := subAny.(*stomp.Subscription)
		if err := sub.Unsubscribe(); err != nil {
			amq.log.Debugf("failed to subscribe to %q queue: %v", sub.Destination(), err)
			return false
		}
		return true
	})
	amq.subscriptions = sync.Map{}
	if amq.health != nil && amq.health.Active() {
		if err := amq.health.Unsubscribe(); err != nil {
			amq.log.Debugf("failed to unsubscribe from %q queue: %v", amq.health.Destination(), err)
		}
		amq.health = nil
	}
	if amq.conn != nil {
		if err := amq.conn.Disconnect(); err != nil {
			amq.conn.MustDisconnect()
			return err
		}
		amq.log.Debugf("connection closed")
	}
	amq.conn = nil
	return nil
}

func (amq *ActiveMQ) withQueueSuffix(queueName string) string {
	if len(amq.options.queueSuffix) == 0 {
		return queueName
	}
	return fmt.Sprintf("%s_%s", queueName, amq.options.queueSuffix)
}
