package activemq

import "github.com/go-stomp/stomp/v3"

type Logger interface {
	Debugf(format string, args ...interface{})
	Infof(format string, args ...interface{})
	Warnf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// emptyLogger

type emptyLogger struct{}

func (*emptyLogger) Debugf(string, ...interface{}) {}
func (*emptyLogger) Infof(string, ...interface{})  {}
func (*emptyLogger) Warnf(string, ...interface{})  {}
func (*emptyLogger) Errorf(string, ...interface{}) {}

// stompLogger

type stompLogger struct {
	Logger
}

var _ stomp.Logger = (*stompLogger)(nil)

func (l *stompLogger) Debug(format string)                         { l.Logger.Debugf(format) }
func (l *stompLogger) Info(format string)                          { l.Logger.Infof(format) }
func (l *stompLogger) Warning(format string)                       { l.Logger.Warnf(format) }
func (l *stompLogger) Warningf(format string, args ...interface{}) { l.Logger.Warnf(format, args...) }
func (l *stompLogger) Error(format string)                         { l.Logger.Errorf(format) }
