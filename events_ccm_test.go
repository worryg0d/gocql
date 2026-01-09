//go:build ccm
// +build ccm

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gocql

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/cassandra-gocql-driver/v2/internal/ccm"
	"github.com/stretchr/testify/require"
)

type schemaChangesTestListener struct {
	KeyspaceCreatedEvents []KeyspaceCreatedEvent
	KeyspaceUpdatedEvents []KeyspaceUpdatedEvent
	KeyspaceDroppedEvents []KeyspaceDroppedEvent
}

// AggregateCreated implements [SchemaChangeListener].
func (s *schemaChangesTestListener) AggregateCreated(event AggregateCreatedEvent) {
	panic("unimplemented")
}

// AggregateDropped implements [SchemaChangeListener].
func (s *schemaChangesTestListener) AggregateDropped(event AggregateDroppedEvent) {
	panic("unimplemented")
}

// AggregateUpdated implements [SchemaChangeListener].
func (s *schemaChangesTestListener) AggregateUpdated(event AggregateUpdatedEvent) {
	panic("unimplemented")
}

// FunctionCreated implements [SchemaChangeListener].
func (s *schemaChangesTestListener) FunctionCreated(event FunctionCreatedEvent) {
	panic("unimplemented")
}

// FunctionDropped implements [SchemaChangeListener].
func (s *schemaChangesTestListener) FunctionDropped(event FunctionDroppedEvent) {
	panic("unimplemented")
}

// FunctionUpdated implements [SchemaChangeListener].
func (s *schemaChangesTestListener) FunctionUpdated(event FunctionUpdatedEvent) {
	panic("unimplemented")
}

// KeyspaceCreated implements [SchemaChangeListener].
func (s *schemaChangesTestListener) KeyspaceCreated(event KeyspaceCreatedEvent) {
	(*s).KeyspaceCreatedEvents = append(s.KeyspaceCreatedEvents, event)
}

// KeyspaceDropped implements [SchemaChangeListener].
func (s *schemaChangesTestListener) KeyspaceDropped(event KeyspaceDroppedEvent) {
	(*s).KeyspaceDroppedEvents = append(s.KeyspaceDroppedEvents, event)
}

// KeyspaceUpdated implements [SchemaChangeListener].
func (s *schemaChangesTestListener) KeyspaceUpdated(event KeyspaceUpdatedEvent) {
	(*s).KeyspaceUpdatedEvents = append(s.KeyspaceUpdatedEvents, event)
}

// TableCreated implements [SchemaChangeListener].
func (s *schemaChangesTestListener) TableCreated(event TableCreatedEvent) {
	panic("unimplemented")
}

// TableDropped implements [SchemaChangeListener].
func (s *schemaChangesTestListener) TableDropped(event TableDroppedEvent) {
	panic("unimplemented")
}

// TableUpdated implements [SchemaChangeListener].
func (s *schemaChangesTestListener) TableUpdated(event TableUpdatedEvent) {
	panic("unimplemented")
}

// UserTypeCreated implements [SchemaChangeListener].
func (s *schemaChangesTestListener) UserTypeCreated(event UserTypeCreatedEvent) {
	panic("unimplemented")
}

// UserTypeDropped implements [SchemaChangeListener].
func (s *schemaChangesTestListener) UserTypeDropped(event UserTypeDroppedEvent) {
	panic("unimplemented")
}

// UserTypeUpdated implements [SchemaChangeListener].
func (s *schemaChangesTestListener) UserTypeUpdated(event UserTypeUpdatedEvent) {
	panic("unimplemented")
}

func (s *schemaChangesTestListener) clear() {
	s.KeyspaceCreatedEvents = nil
	s.KeyspaceDroppedEvents = nil
	s.KeyspaceUpdatedEvents = nil
}

func TestTopologyEvents_Keyspace(t *testing.T) {
	ccm.DoWithin(t, func(meta *ccm.ClusterMetadata) {
		listener := &schemaChangesTestListener{}

		session := createSession(t, func(config *ClusterConfig) {
			config.Hosts = []string{meta.Hosts[0].Addr}
			config.Events.SchemaUpdateListener = listener
			config.Events.DisableTopologyEvents = false
		})
		defer session.Close()

		time.Sleep(2 * time.Second)

		ks := randomNameWithPrefix("gocql_integration_tests_events_")

		listener.clear()

		err := session.Query(fmt.Sprintf(`CREATE KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}`, ks)).Exec()
		require.NoError(t, err, "Expected no error creating keyspace")

		require.Eventually(t, func() bool {
			return len(listener.KeyspaceCreatedEvents) > 0
		}, time.Second*5, time.Millisecond*100, "Expected keyspace created event to be received")

		// Verify that the listener received the keyspace created event
		require.Equal(t, ks, listener.KeyspaceCreatedEvents[0].Keyspace.Name, "Expected keyspace created event to have correct keyspace name")
		require.Contains(t, listener.KeyspaceCreatedEvents[0].Keyspace.StrategyClass, "SimpleStrategy", "Expected keyspace created event to have correct replication class")
		require.Equal(t, "1", listener.KeyspaceCreatedEvents[0].Keyspace.StrategyOptions["replication_factor"], "Expected keyspace created event to have correct replication factor")

		listener.clear()

		err = session.Query(fmt.Sprintf(`ALTER KEYSPACE %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '2'}`, ks)).Exec()
		require.NoError(t, err, "Expected no error updating keyspace")

		// Verify that the listener received the keyspace updated event
		// and that the old and new metadata are correct

		require.Eventually(t, func() bool {
			return len(listener.KeyspaceUpdatedEvents) > 0
		}, time.Second*5, time.Millisecond*100, "Expected keyspace updated event to be received")

		// Old metadata
		require.Equal(t, ks, listener.KeyspaceUpdatedEvents[0].OldKeyspace.Name, "Expected keyspace updated event to have correct keyspace name")
		require.Contains(t, listener.KeyspaceUpdatedEvents[0].OldKeyspace.StrategyClass, "SimpleStrategy", "Expected keyspace created event to have correct replication class")
		require.Equal(t, "1", listener.KeyspaceUpdatedEvents[0].OldKeyspace.StrategyOptions["replication_factor"], "Expected keyspace updated event to have correct old replication factor")

		// New metadata
		require.Equal(t, ks, listener.KeyspaceUpdatedEvents[0].NewKeyspace.Name, "Expected keyspace updated event to have correct keyspace name")
		require.Contains(t, listener.KeyspaceUpdatedEvents[0].NewKeyspace.StrategyClass, "SimpleStrategy", "Expected keyspace updated event to have correct replication class")
		require.Equal(t, "2", listener.KeyspaceUpdatedEvents[0].NewKeyspace.StrategyOptions["replication_factor"], "Expected keyspace updated event to have correct new replication factor")

		time.Sleep(2 * time.Second)

		listener.clear()

		err = session.Query(fmt.Sprintf(`DROP KEYSPACE %s`, ks)).
			RetryPolicy(&SimpleRetryPolicy{}).
			Consistency(All).
			Exec()
		require.NoError(t, err, "Expected no error dropping keyspace")

		require.Eventually(t, func() bool {
			t.Logf("Checking for drop events, count: %d", len(listener.KeyspaceDroppedEvents))
			return len(listener.KeyspaceDroppedEvents) > 0
		}, time.Second*60, time.Millisecond*100, "Expected keyspace dropped event to be received")

		// Verify that the listener received the keyspace dropped event
		require.Equal(t, ks, listener.KeyspaceDroppedEvents[0].Keyspace.Name, "Expected keyspace dropped event to have correct keyspace name")
	})
}

func randomNameWithPrefix(prefix string) string {
	return prefix + strings.ToLower(randomText(10))
}
