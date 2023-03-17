package failover

import (
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	testCases := []struct {
		name    string
		input   string
		want    Failover
		wantErr bool
	}{
		{name: "empty", input: "", wantErr: true},
		{name: "udp", input: "udp://localhost:61616", wantErr: true},
		{name: "invalid ip", input: "400.12.13.14:61616", wantErr: true},
		{name: "host port", input: "example.com", wantErr: true},
		{name: "without port", input: "tcp://example.com", wantErr: true},
		{name: "with path", input: "tcp://localhost:61616/activemq/everything/etc", wantErr: true},
		{name: "scheme host port",
			input: "tcp://localhost:61616",
			want:  New().Add("tcp://localhost:61616").Build(),
		},
		{name: "trailing slash",
			input: "tcp://localhost:61616/",
			want:  New().Add("tcp://localhost:61616").Build(),
		},
		{name: "failover prefix",
			input: "failover:tcp://localhost:61616",
			want:  New().Add("tcp://localhost:61616").Build(),
		},

		{name: "brackets/wrong1", input: ")tcp://localhost:61616", wantErr: true},
		{name: "brackets/wrong2", input: "(tcp://localhost:61616", wantErr: true},
		{name: "brackets/wrong3", input: "tcp://localhost:61616(tcp://localhost:61617)", wantErr: true},
		{name: "brackets/wrong4", input: "tcp://localhost:61616,(tcp://localhost:61617)", wantErr: true},
		{name: "list",
			input: "tcp://localhost:61616,tcp://localhost:61617,tcp://localhost:61618",
			want:  New().Add("tcp://localhost:61616").Add("tcp://localhost:61617").Add("tcp://localhost:61618").Build(),
		},
		{name: "list/empty space",
			input: "tcp://localhost:61616,,tcp://localhost:61618",
			want:  New().Add("tcp://localhost:61616").Add("tcp://localhost:61618").Build(),
		},
		{name: "brackets",
			input: "(tcp://localhost:61616)",
			want:  New().Add("tcp://localhost:61616").Build(),
		},
		{name: "brackets/list",
			input: "(tcp://localhost:61616,tcp://localhost:61617)",
			want:  New().Add("tcp://localhost:61616").Add("tcp://localhost:61617").Build(),
		},

		{name: "options/question", input: "(tcp://localhost:61616)randomize=false", wantErr: true},
		{name: "options/emtpy",
			input: "(tcp://localhost:61616)?",
			want:  New().Add("tcp://localhost:61616").Build(),
		},

		{name: "options/initialReconnectDelay/without value", input: "(tcp://localhost:61616)?initialReconnectDelay", wantErr: true},
		{name: "options/initialReconnectDelay",
			input: "(tcp://localhost:61616)?initialReconnectDelay=40",
			want:  New().Add("tcp://localhost:61616").WithInitialReconnectDelay(time.Millisecond * 40).Build(),
		},
		{name: "options/initialReconnectDelay/default",
			input: "(tcp://localhost:61616)?initialReconnectDelay=10",
			// initialReconnectDelay=10, this is the default value
			want: New().Add("tcp://localhost:61616").Build(),
		},

		{name: "options/maxReconnectDelay/without value", input: "(tcp://localhost:61616)?maxReconnectDelay", wantErr: true},
		{name: "options/maxReconnectDelay",
			input: "(tcp://localhost:61616)?maxReconnectDelay=60000",
			want:  New().Add("tcp://localhost:61616").WithMaxReconnectDelay(time.Millisecond * 60000).Build(),
		},
		{name: "options/maxReconnectDelay/default",
			input: "(tcp://localhost:61616)?maxReconnectDelay=30000",
			// maxReconnectDelay=30000, this is the default value
			want: New().Add("tcp://localhost:61616").Build(),
		},

		{name: "options/randomize/without value", input: "(tcp://localhost:61616)?randomize", wantErr: true},
		{name: "options/randomize",
			input: "(tcp://localhost:61616)?randomize=false",
			want:  New().Add("tcp://localhost:61616").WithRandomize(false).Build(),
		},
		{name: "options/randomize/default",
			input: "(tcp://localhost:61616)?randomize=true",
			// randomize=true, this is the default value
			want: New().Add("tcp://localhost:61616").Build(),
		},

		{name: "options/maxReconnectAttempts/without value", input: "(tcp://localhost:61616)?maxReconnectAttempts", wantErr: true},
		{name: "options/maxReconnectAttempts/big negative", input: "(tcp://localhost:61616)?maxReconnectAttempts=-3", wantErr: true},
		{name: "options/maxReconnectAttempts", input: "(tcp://localhost:61616)?maxReconnectAttempts=123", wantErr: true},
		{name: "options/maxReconnectAttempts/default",
			input: "(tcp://localhost:61616)?maxReconnectAttempts=-1",
			// maxReconnectAttempts=-1, this is the default value
			want: New().Add("tcp://localhost:61616").Build(),
		},

		{name: "options/startupMaxReconnectAttempts/without value", input: "(tcp://localhost:61616)?startupMaxReconnectAttempts", wantErr: true},
		{name: "options/startupMaxReconnectAttempts/big negative", input: "(tcp://localhost:61616)?startupMaxReconnectAttempts=-3", wantErr: true},
		{name: "options/startupMaxReconnectAttempts",
			input: "(tcp://localhost:61616)?startupMaxReconnectAttempts=123",
			want:  New().Add("tcp://localhost:61616").WithStartupMaxReconnectAttempts(123).Build(),
		},
		{name: "options/startupMaxReconnectAttempts/default",
			input: "(tcp://localhost:61616)?startupMaxReconnectAttempts=-1",
			// startupMaxReconnectAttempts=-1, this is the default value
			want: New().Add("tcp://localhost:61616").Build(),
		},
	}
	for _, test := range testCases {
		t.Run(test.name, func(t *testing.T) {
			t.Logf("input: %q", test.input)
			got, err := Parse(test.input)
			if test.wantErr {
				if !assert.Error(t, err) {
					t.Logf("got object: %+v", got)
				} else {
					t.Logf("error: %v", err)
				}
				return
			} else {
				if !assert.NoError(t, err) {
					t.Logf("error: %v", err)
					return
				}
			}
			assert.Equal(t, test.want, got)
		})
	}
}

func TestFailover_FirstURL(t *testing.T) {
	const (
		firstURL  = "tcp://localhost:61616"
		secondURL = "tcp://localhost:61617"
		thirdURL  = "tcp://localhost:61618"
	)

	t.Run("one url", func(t *testing.T) {
		t.Run("sequential", func(t *testing.T) {
			fo := New().Add(firstURL).WithRandomize(false).Build()

			iter := fo.FirstURL()
			// first url
			assert.Equal(t, firstURL, iter.URL().URL)
			assert.True(t, iter.HasURL())
			// next
			iter.Next()
			assert.False(t, iter.HasURL())
		})

		t.Run("random", func(t *testing.T) {
			fo := New().Add(firstURL).Build()

			iter := fo.FirstURL()
			// first url
			assert.Equal(t, firstURL, iter.URL().URL)
			assert.True(t, iter.HasURL())
			// next
			iter.Next()
			assert.False(t, iter.HasURL())
		})
	})

	t.Run("list of url", func(t *testing.T) {
		t.Run("sequential", func(t *testing.T) {
			fo := New().Add(firstURL).Add(secondURL).Add(thirdURL).WithRandomize(false).Build()

			iter := fo.FirstURL()
			// first url
			assert.Equal(t, firstURL, iter.URL().URL)
			assert.True(t, iter.HasURL())
			// next
			iter.Next()
			// second url
			assert.Equal(t, secondURL, iter.URL().URL)
			assert.True(t, iter.HasURL())
			// next
			iter.Next()
			// third url
			assert.Equal(t, thirdURL, iter.URL().URL)
			assert.True(t, iter.HasURL())
			// next
			iter.Next()
			assert.False(t, iter.HasURL())
		})

		t.Run("random", func(t *testing.T) {
			fo := New().Add(firstURL).Add(secondURL).Add(thirdURL).Build()
			got, want := []string(nil), []string{firstURL, secondURL, thirdURL}
			for i := 0; i < 50; i++ {
				got = make([]string, 0, 3)
				for iter := fo.FirstURL(); iter.HasURL(); iter.Next() {
					got = append(got, iter.URL().URL)
				}
				t.Logf("%+v", got)
				if !reflect.DeepEqual(got, want) {
					break
				}
			}
			assert.NotEqual(t, want, got)
		})
	})
}
