package failover

import (
	crand "crypto/rand"
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// https://activemq.apache.org/failover-transport-reference.html
type Failover struct {
	URLs []URL
	Options
}

type URL struct {
	URL string
	OptionsURL
}

type Options struct {
	NestedOptionsURL OptionsURL

	// InitialReconnectDelay returns the delay (in ms) before the first reconnect attempt.
	//
	// Default value: 10.
	InitialReconnectDelayMs int

	// MaxReconnectDelay returns the maximum delay (in ms) between the second and subsequent reconnect attempts.
	//
	// Default value: 30000.
	MaxReconnectDelayMs int

	// If true, choose a URI at random from the list to use for reconnect.
	//
	// Default value: true.
	//
	// The Failover transport chooses a URI at random by default. This effectively load-balances clients over multiple
	// brokers. However, to have a client connect to a primary first and only connect to a secondary backup broker when
	// the primary is unavailable, set randomize=false.
	Randomize bool

	maxReconnectAttempts int

	// A value of -1 denotes that the number of connection attempts at startup should be unlimited. A value of >=0
	// denotes the number of reconnect attempts at startup that will be made after which an error is sent back to the
	// client when the client makes a subsequent reconnect attempt. Note: once successfully connected the
	// Options.MaxReconnectAttempts option prevails.
	//
	// Default value: -1.
	StartupMaxReconnectAttempts int
}

func (o Options) InitialReconnectDelay() time.Duration {
	return time.Millisecond * time.Duration(o.InitialReconnectDelayMs)
}

func (o Options) MaxReconnectDelay() time.Duration {
	return time.Millisecond * time.Duration(o.MaxReconnectDelayMs)
}

// MaxReconnectAttempts returns maximum reconnect attempts.
// Value is -1, retry forever.
// 0 means disables re-connection, e.g: just try to connect once.
// a value >0 denotes the maximum number of reconnect attempts before an error is sent back to the client.
//
// Default value: -1
func (o Options) MaxReconnectAttempts() int {
	return o.maxReconnectAttempts
}

func InfiniteReconnectAttempts(option int) int {
	if option == -1 {
		return math.MinInt64
	}
	return option
}

type OptionsURL struct {
	ReadTimeout int
}

func defaultOptions() Options {
	return Options{
		NestedOptionsURL:            defaultOptionsURL(),
		InitialReconnectDelayMs:     10,
		MaxReconnectDelayMs:         30000,
		Randomize:                   true,
		maxReconnectAttempts:        -1,
		StartupMaxReconnectAttempts: -1,
	}
}

func defaultOptionsURL() OptionsURL {
	return OptionsURL{}
}

func (fo *Failover) Clone() Failover {
	var out Failover
	for _, u := range fo.URLs {
		out.URLs = append(out.URLs, u)
	}
	out.Options = fo.Options
	return out
}

func Parse(s string) (Failover, error) {
	fo := Failover{
		Options: defaultOptions(),
	}

	s = strings.TrimPrefix(s, "failover:")

	var sOptions string

	if strings.HasPrefix(s, "(") {
		s = s[1:] // trim prefix "("

		if i := strings.Index(s, ")"); i < 0 {
			return Failover{}, fmt.Errorf("parse failover error: invalid brackets")
		} else {
			// cut ")" and get urls and options
			s, sOptions = s[:i], s[i+1:]
		}
	}

	// parse urls
	for _, part := range strings.Split(s, ",") {
		if part == "" {
			continue
		}
		u, err := parseURL(part)
		if err != nil {
			return Failover{}, fmt.Errorf("parse failover error: %v", err)
		}
		fo.URLs = append(fo.URLs, u)
	}

	// parse options
	if len(sOptions) > 1 && !strings.HasPrefix(sOptions, "?") {
		return Failover{}, fmt.Errorf("parse failover error: the block of options which after the closed bracket must contain a question mark")
	}
	sOptions = strings.TrimPrefix(sOptions, "?")
	options, err := url.ParseQuery(sOptions)
	if err != nil {
		return Failover{}, fmt.Errorf("parse failover error: %v", &url.Error{Op: "parse", URL: sOptions, Err: err})
	}
	if err := parseOptions(&fo.Options, options); err != nil {
		return Failover{}, fmt.Errorf("parse failover error: %v", err)
	}

	if len(fo.URLs) == 0 {
		return Failover{}, fmt.Errorf("parse failover error: urls are not defined")
	}

	return fo, nil
}

func parseURL(s string) (URL, error) {
	u, err := url.Parse(s)
	if err != nil {
		return URL{}, err
	}

	switch u.Scheme {
	case "":
		u.Scheme = "tcp"
	case "tcp":
	default:
		return URL{}, &url.Error{Op: "parse", URL: s, Err: fmt.Errorf("scheme is not tcp")}
	}

	if len(u.Hostname()) == 0 {
		return URL{}, &url.Error{Op: "parse", URL: s, Err: fmt.Errorf("hostname is empty")}
	}
	if len(u.Port()) == 0 {
		return URL{}, &url.Error{Op: "parse", URL: s, Err: fmt.Errorf("port is empty")}
	}
	switch u.Path {
	case "/", "":
	default:
		return URL{}, &url.Error{Op: "parse", URL: s, Err: fmt.Errorf("path is not allowed")}
	}

	out := URL{
		URL:        fmt.Sprintf("%s://%s:%s", u.Scheme, u.Hostname(), u.Port()),
		OptionsURL: defaultOptionsURL(),
	}

	if err := parseOptionsURL(&out.OptionsURL, u.Query()); err != nil {
		return URL{}, &url.Error{Op: "parse", URL: s, Err: err}
	}

	return out, nil
}

func parseOptions(opts *Options, query url.Values) error {
	if opts == nil {
		return fmt.Errorf("opts is nil")
	}
	if len(query) == 0 {
		return nil
	}

	if err := parseIntOption(&opts.InitialReconnectDelayMs, "initialReconnectDelay", query); err != nil {
		return err
	}

	if err := parseIntOption(&opts.MaxReconnectDelayMs, "maxReconnectDelay", query); err != nil {
		return err
	}

	if err := parseBoolOption(&opts.Randomize, "randomize", query); err != nil {
		return err
	}

	if err := parseIntOption(&opts.maxReconnectAttempts, "maxReconnectAttempts", query); err != nil {
		return err
	}
	if opts.maxReconnectAttempts != -1 {
		return fmt.Errorf("parse option \"maxReconnectAttempts\": deny a value other than -1")
	}

	if err := parseIntOption(&opts.StartupMaxReconnectAttempts, "startupMaxReconnectAttempts", query); err != nil {
		return err
	}
	if opts.StartupMaxReconnectAttempts < -1 {
		return fmt.Errorf("parse option \"startupMaxReconnectAttempts\": minimum value is -1")
	}

	// TODO other options

	if err := parseOptionsURL(&opts.NestedOptionsURL, query); err != nil {
		return err
	}

	// TODO the remaining

	return nil
}

func parseOptionsURL(opts *OptionsURL, query url.Values) error {
	if opts == nil {
		return fmt.Errorf("opts is nil")
	}
	if len(query) == 0 {
		return nil
	}

	// TODO other options

	return nil
}

func parseBoolOption(option *bool, name string, query url.Values) error {
	if _, ok := query[name]; !ok {
		return nil
	}
	value := query.Get(name)
	if len(value) == 0 {
		return fmt.Errorf("parse option %s: empty value", name)
	}
	b, err := strconv.ParseBool(value)
	if err != nil {
		return fmt.Errorf("parse option %s: %v", name, err)
	}
	*option = b
	query.Del(name)
	return nil
}

// TODO combine
func parseIntOption(option *int, name string, query url.Values) error {
	if _, ok := query[name]; !ok {
		return nil
	}
	value := query.Get(name)
	if len(value) == 0 {
		return fmt.Errorf("parse option %s: empty value", name)
	}
	i, err := strconv.Atoi(value)
	if err != nil {
		return fmt.Errorf("parse option %s: %v", name, err)
	}
	*option = i
	query.Del(name)
	return nil
}

type URLGetter interface {
	HasURL() bool
	Next()
	URL() URL
}

func (fo *Failover) FirstURL() URLGetter {
	if fo.Randomize {
		return newRandomURLGetter(fo)
	}
	return &sequentialURLGetter{
		fo:    fo,
		index: 0,
	}
}

func newSequentialURLGetter(fo *Failover) *sequentialURLGetter {
	return &sequentialURLGetter{fo: fo}
}

type sequentialURLGetter struct {
	fo    *Failover
	index int
}

func (g *sequentialURLGetter) HasURL() bool {
	return g.index < len(g.fo.URLs)
}

func (g *sequentialURLGetter) Next() {
	g.index++
}

func (g *sequentialURLGetter) URL() URL {
	// TODO create function merge options and nested options
	//      and clone for URL
	// TODO return URL, bool
	return g.fo.URLs[g.index]
}

func newRandomURLGetter(fo *Failover) *randomURLGetter {
	g := &randomURLGetter{
		fo:      fo,
		indices: make([]int, len(fo.URLs)),
	}

	var seed int64
	var buf [8]byte
	if _, err := io.ReadFull(crand.Reader, buf[:]); err != nil {
		seed = time.Now().UnixNano()
	} else {
		seed = int64(binary.LittleEndian.Uint64(buf[:]))
	}

	for idx := 0; idx < len(fo.URLs); idx++ {
		g.indices[idx] = idx
	}
	rand.New(rand.NewSource(seed)).Shuffle(len(g.indices), func(i, j int) {
		g.indices[i], g.indices[j] = g.indices[j], g.indices[i]
	})

	return g
}

type randomURLGetter struct {
	fo      *Failover
	indices []int
}

func (g *randomURLGetter) HasURL() bool {
	return len(g.indices) > 0
}

func (g *randomURLGetter) Next() {
	g.indices = g.indices[1:]
}

func (g *randomURLGetter) URL() URL {
	// TODO create function merge options and nested options
	//      and clone for URL
	// TODO return URL, bool
	return g.fo.URLs[g.indices[0]]
}
