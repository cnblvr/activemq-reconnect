package activemq

import (
	"fmt"
	"testing"
	"time"
)

func newTestLogger(t *testing.T, prefix string) *logger {
	return &logger{
		t:         t,
		prefixLog: prefix,
		startTime: time.Now(),
	}
}

type logger struct {
	t         *testing.T
	prefixLog string
	startTime time.Time
}

func (l *logger) prefix(level string) string {
	duration := time.Since(l.startTime)
	seconds := duration.Truncate(time.Second)
	microseconds := (duration - seconds).Truncate(time.Microsecond)
	prefix := l.prefixLog
	if prefix != "" {
		prefix += " "
	}
	return fmt.Sprintf("%s[%s] %s.%06d ", prefix, level, seconds, microseconds/1000)
}

func (l *logger) Debugf(format string, args ...interface{}) {
	l.t.Logf(l.prefix("DBG")+format, args...)
}

func (l *logger) Infof(format string, args ...interface{}) {
	l.t.Logf(l.prefix("INF")+format, args...)
}

func (l *logger) Warnf(format string, args ...interface{}) {
	l.t.Logf(l.prefix("WRN")+format, args...)
}

func (l *logger) Errorf(format string, args ...interface{}) {
	l.t.Logf(l.prefix("ERR")+format, args...)
}
