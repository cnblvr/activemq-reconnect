package failover

import "time"

type Builder interface {
	Add(url string /*, opts ...WithOption*/) Builder

	// WithInitialReconnectDelay initializes Options.InitialReconnectDelayMs.
	WithInitialReconnectDelay(delay time.Duration) Builder

	// WithMaxReconnectDelay initializes Options.MaxReconnectDelayMs.
	WithMaxReconnectDelay(delay time.Duration) Builder

	// WithRandomize initializes Options.Randomize.
	WithRandomize(v bool) Builder

	// WithMaxReconnectAttempts initializes Options.MaxReconnectAttempts.
	//WithMaxReconnectAttempts(attempts int) Builder

	// WithStartupMaxReconnectAttempts initializes Options.StartupMaxReconnectAttempts.
	WithStartupMaxReconnectAttempts(attempts int) Builder

	Build() Failover
}

func New() Builder {
	return &builder{
		fo: Failover{
			Options: defaultOptions(),
		},
	}
}

type builder struct {
	fo Failover
}

func (b *builder) Add(s string /*, opts ...WithQwerty*/) Builder {
	u := URL{
		URL:        s,
		OptionsURL: defaultOptionsURL(),
	}

	// TODO

	b.fo.URLs = append(b.fo.URLs, u)
	return b
}

func (b *builder) WithInitialReconnectDelay(delay time.Duration) Builder {
	b.fo.Options.InitialReconnectDelayMs = truncateMillisecond(delay)
	return b
}

func (b *builder) WithMaxReconnectDelay(delay time.Duration) Builder {
	b.fo.Options.MaxReconnectDelayMs = truncateMillisecond(delay)
	return b
}

func (b *builder) WithRandomize(v bool) Builder {
	b.fo.Options.Randomize = v
	return b
}

//func (b *builder) WithMaxReconnectAttempts(attempts int) Builder {
//	b.fo.Options.MaxReconnectAttempts = attempts
//	return b
//}

func (b *builder) WithStartupMaxReconnectAttempts(attempts int) Builder {
	b.fo.Options.StartupMaxReconnectAttempts = attempts
	return b
}

func (b *builder) Build() Failover {
	return b.fo
}

// (-inf; 0ms0us0ns] = 0ms
// [1ns; 1ms0us0ns] = 1ms
// [1ms0us1ns; 2ms0us0ns] = 2ms
// ... etc
func truncateMillisecond(delay time.Duration) int {
	if delay <= 0 {
		return 0
	}
	ms := delay.Truncate(time.Millisecond)
	if delay-ms > 0 {
		ms += time.Millisecond
	}
	return int(ms / time.Millisecond)
}
