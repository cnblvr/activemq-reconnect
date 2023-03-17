package failover

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_truncateMillisecond(t *testing.T) {
	testCases := []struct {
		name  string
		input time.Duration
		want  int
	}{
		{name: "negative", input: -time.Millisecond * 80, want: 0},
		{name: "zero", input: 0, want: 0},

		{name: "1ns", input: time.Nanosecond, want: 1},
		{name: "1us", input: time.Microsecond, want: 1},
		{name: "999us999ns", input: time.Millisecond - time.Nanosecond, want: 1},
		{name: "1ms", input: time.Millisecond, want: 1},

		{name: "1ms1ns", input: time.Millisecond + time.Nanosecond, want: 2},
		{name: "2ms", input: 2 * time.Millisecond, want: 2},

		{name: "2ms1ns", input: 2*time.Millisecond + time.Nanosecond, want: 3},
		{name: "10min", input: 10 * time.Minute, want: 600000},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			got := truncateMillisecond(test.input)
			assert.Equal(t, test.want, got)
		})
	}
}
