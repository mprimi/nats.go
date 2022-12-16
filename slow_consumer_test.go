// Copyright 2022 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package nats

import (
	"runtime"
	"strings"
	"testing"
	"time"
)

// Shows how a client that has slow or idle consumers can rapidly blow up in memory footprint.
// With parameters as-is, this will swell the memory usage to 60GB in just a few seconds.
func TestSlowConsumersOOM(t *testing.T) {

	const (
		// SubscribeSync then never consume
		IdleSyncConsumer = iota
		// Subscribe and take 1 second to process each message
		SlowAsyncConsumer = iota
		KiB               = 1024
		MiB               = KiB * 1024
		GiB               = MiB * 1024
	)

	// Rather than starting an in-process server for this test, point to an external one.
	// This avoids confounding of server vs client memory usage.
	const server = "nats://localhost:4222"

	// Memory grows linearly, proportionately to the product of these 2:
	const numSubscriptions = 5_000 // The more subs, the faster memory grows
	const msgSize = 10 * KiB

	// Either of these, exhibit the same behavior: rapid and unbounded memory growth
	const slowConsumerMode = IdleSyncConsumer
	//const slowConsumerMode = SlowAsyncConsumer

	// These have little impact
	const numConnections = 10
	const subjectLength = 10
	const numMsg = 100_000 // Will probably crash before sending 1000

	subject := strings.Repeat("s", subjectLength)

	// Error handler that mutes the (expected) flood of ErrSlowConsumer
	handleSubError := func(conn *Conn, s *Subscription, err error) {
		if err == ErrSlowConsumer {
			// noop
		} else {
			t.Logf("%v", err)
		}
	}

	// Create connections
	connections := make([]*Conn, numConnections)
	for i := 0; i < numConnections; i++ {
		nc, err := Connect(server, ErrorHandler(handleSubError))
		if err != nil {
			t.Fatalf("failed to connect: %v", err)
		}
		connections[i] = nc
	}
	defer func() {
		for _, nc := range connections {
			if nc != nil {
				nc.Close()
			}
		}
	}()

	slowMessageHandler := func(msg *Msg) {
		time.Sleep(1 * time.Second)
	}

	// Create subscriptions, round-robin over established connections
	subscriptions := make([]*Subscription, numSubscriptions)
	for i := 0; i < numSubscriptions; i++ {
		nc := connections[i%numConnections]
		var sub *Subscription
		var err error
		switch slowConsumerMode {
		case IdleSyncConsumer:
			sub, err = nc.SubscribeSync(subject)
		case SlowAsyncConsumer:
			sub, err = nc.Subscribe(subject, slowMessageHandler)
		}
		if err != nil {
			t.Fatalf("failed to subscribe: %v", err)
		}
		subscriptions[i] = sub
	}
	defer func() {
		for _, sub := range subscriptions {
			if sub != nil {
				_ = sub.Unsubscribe()
			}
		}
	}()

	// Create connection for publisher
	nc, err := Connect(server)
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer nc.Close()

	ticker := time.NewTicker(1 * time.Second)

	data := make([]byte, msgSize)
	memStats := runtime.MemStats{}
	for i := 0; i < numMsg; i++ {
		err := nc.Publish(subject, data)
		if err != nil {
			t.Fatalf("failed to publish: %v", err)
		}

		select {
		case <-ticker.C:
			runtime.ReadMemStats(&memStats)
			t.Logf(
				"Published %d messages (%d MiB), runtime mem: %d GiB",
				i,
				(i*msgSize)/MiB,
				memStats.Alloc/GiB,
			)
		default:
			// Continue publishing
		}
	}
}
