package activemq

import "github.com/cnblvr/activemq-reconnect/failover"

type Option interface {
	apply(*options)
}

type options struct {
	failover        failover.Failover
	failoverStr     string
	login, passcode string
	log             Logger
	healthQueueName string
	queueSuffix     string
}

// options :: failover object

func WithFailover(failover failover.Failover) Option { return failoverOption{failover: failover} }

type failoverOption struct {
	failover failover.Failover
}

func (v failoverOption) apply(o *options) { o.failover = v.failover }

// options :: failover string

func WithFailoverStr(s string) Option { return failoverStrOption{failoverStr: s} }

type failoverStrOption struct {
	failoverStr string
}

func (v failoverStrOption) apply(o *options) { o.failoverStr = v.failoverStr }

// options :: auth

func WithAuth(login, passcode string) Option { return authOption{login: login, passcode: passcode} }

type authOption struct {
	login    string
	passcode string
}

func (v authOption) apply(o *options) { o.login, o.passcode = v.login, v.passcode }

// options :: logger

func WithLogger(logger Logger) Option { return loggerOption{logger: logger} }

type loggerOption struct {
	logger Logger
}

func (v loggerOption) apply(o *options) { o.log = v.logger }

// options :: health queue name

func WithHealthQueueName(name string) Option { return healthQueueNameOption(name) }

type healthQueueNameOption string

func (v healthQueueNameOption) apply(o *options) { o.healthQueueName = string(v) }

// options :: queue name suffix

func WithQueueSuffix(suffix string) Option { return queueSuffixOption(suffix) }

type queueSuffixOption string

func (v queueSuffixOption) apply(o *options) { o.queueSuffix = string(v) }
