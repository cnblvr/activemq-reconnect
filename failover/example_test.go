package failover_test

import "github.com/cnblvr/activemq-reconnect/failover"

func ExampleNew() {
	failover.New().
		Add("tcp://localhost:61616").
		Add("tcp://localhost:61617").
		WithRandomize(true).
		Build()
}

func ExampleFailover_FirstURL() {
	var fo failover.Failover
	for iter := fo.FirstURL(); iter.HasURL(); iter.Next() {
		url := iter.URL()
		_ = url
	}
}
