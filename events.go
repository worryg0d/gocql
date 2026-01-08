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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package gocql

import (
	"fmt"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type eventDebouncer struct {
	name   string
	timer  *time.Timer
	mu     sync.Mutex
	events []frame

	callback func([]frame)
	quit     chan struct{}

	logger StructuredLogger
}

func newEventDebouncer(name string, eventHandler func([]frame), logger StructuredLogger) *eventDebouncer {
	e := &eventDebouncer{
		name:     name,
		quit:     make(chan struct{}),
		timer:    time.NewTimer(eventDebounceTime),
		callback: eventHandler,
		logger:   logger,
	}
	e.timer.Stop()
	go e.flusher()

	return e
}

func (e *eventDebouncer) stop() {
	e.quit <- struct{}{} // sync with flusher
	close(e.quit)
}

func (e *eventDebouncer) flusher() {
	for {
		select {
		case <-e.timer.C:
			e.mu.Lock()
			e.flush()
			e.mu.Unlock()
		case <-e.quit:
			return
		}
	}
}

const (
	eventBufferSize   = 1000
	eventDebounceTime = 1 * time.Second
)

// flush must be called with mu locked
func (e *eventDebouncer) flush() {
	if len(e.events) == 0 {
		return
	}

	// if the flush interval is faster than the callback then we will end up calling
	// the callback multiple times, probably a bad idea. In this case we could drop
	// frames?
	go e.callback(e.events)
	e.events = make([]frame, 0, eventBufferSize)
}

func (e *eventDebouncer) debounce(frame frame) {
	e.mu.Lock()
	e.timer.Reset(eventDebounceTime)

	// TODO: probably need a warning to track if this threshold is too low
	if len(e.events) < eventBufferSize {
		e.events = append(e.events, frame)
	} else {
		e.logger.Warning("Event buffer full, dropping event frame.",
			NewLogFieldString("event_name", e.name), NewLogFieldStringer("frame", frame))
	}

	e.mu.Unlock()
}

func (s *Session) handleEvent(framer *framer) {
	frame, err := framer.parseFrame()
	if err != nil {
		s.logger.Error("Unable to parse event frame.", NewLogFieldError("err", err))
		return
	}

	s.logger.Debug("Handling event frame.", NewLogFieldStringer("frame", frame))

	switch f := frame.(type) {
	case *schemaChangeKeyspace, *schemaChangeFunction,
		*schemaChangeTable, *schemaChangeAggregate, *schemaChangeType:

		s.schemaEvents.debounce(frame)
	case *topologyChangeEventFrame, *statusChangeEventFrame:
		s.nodeEvents.debounce(frame)
	default:
		s.logger.Error("Invalid event frame.",
			NewLogFieldString("frame_type", fmt.Sprintf("%T", f)), NewLogFieldStringer("frame", f))
	}
}

func (s *Session) handleSchemaEvent(frames []frame) {
	// TODO: debounce events
	for _, frame := range frames {
		switch f := frame.(type) {
		case *schemaChangeKeyspace:
			s.handleKeyspaceChange(f)
		case *schemaChangeTable:
			s.handleTableChange(f)
		case *schemaChangeAggregate:
			s.handleAggregateChange(f)
		case *schemaChangeFunction:
			s.handleFunctionChange(f)
		case *schemaChangeType:
			s.handleTypeChange(f)
		}
	}
}

func (s *Session) handleKeyspaceChange(frame *schemaChangeKeyspace) {
	keyspace := frame.keyspace

	if s.schemaUpdateListener == nil {
		s.schemaDescriber.clearSchema(keyspace)
		s.control.awaitSchemaAgreement()
		s.policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: keyspace, Change: frame.change})
		return
	}

	var oldKeyspaceMeta *KeyspaceMetadata
	var err error

	if frame.change == SchemaChangeUpdated {
		oldKeyspaceMeta, err = s.schemaDescriber.getSchema(frame.keyspace)
		if err != nil {
			s.logger.Error("Unable to get old keyspace metadata for updated keyspace.",
				NewLogFieldString("keyspace", frame.keyspace), NewLogFieldError("err", err))
			return
		}
	}

	// Clear the schema cache to force re-fetching updated schema

	s.schemaDescriber.clearSchema(keyspace)
	s.control.awaitSchemaAgreement()
	keyspaceMeta, err := s.schemaDescriber.getSchema(keyspace)
	if err != nil {
		s.logger.Error("Unable to get new keyspace metadata for updated keyspace.",
			NewLogFieldString("keyspace", keyspace), NewLogFieldError("err", err))
		return
	}

	// TODO: Don't like having 2 places where we call KeyspaceChanged
	s.policy.KeyspaceChanged(KeyspaceUpdateEvent{Keyspace: keyspace, Change: frame.change})

	switch frame.change {
	case SchemaChangeCreated:
		s.schemaUpdateListener.KeyspaceCreated(KeyspaceCreatedEvent{KeyspaceMetadata: keyspaceMeta})
	case SchemaChangeUpdated:
		s.schemaUpdateListener.KeyspaceUpdated(KeyspaceUpdatedEvent{OldKeyspaceMetadata: oldKeyspaceMeta, NewKeyspaceMetadata: keyspaceMeta})
	case SchemaChangeDropped:
		s.schemaUpdateListener.KeyspaceDropped(KeyspaceDroppedEvent{KeyspaceMetadata: keyspaceMeta})
	}
}

func (s *Session) handleTableChange(frame *schemaChangeTable) {
	keyspace := frame.keyspace
	if s.schemaUpdateListener == nil {
		s.schemaDescriber.clearSchema(keyspace)
		return
	}

	var oldTable *TableMetadata
	var err error

	if frame.change == SchemaChangeUpdated {
		oldTable, err = s.schemaDescriber.getTableSchema(keyspace, frame.object)
		if err != nil {
			s.logger.Error("Unable to get old table metadata for updated table.",
				NewLogFieldString("keyspace", keyspace), NewLogFieldString("table", frame.object), NewLogFieldError("err", err))
			return
		}
	}

	// Clear the schema cache to force re-fetching updated schema
	s.schemaDescriber.clearSchema(keyspace)
	newTable, err := s.schemaDescriber.getTableSchema(keyspace, frame.object)
	if err != nil {
		s.logger.Error("Unable to get new table metadata for updated table.",
			NewLogFieldString("keyspace", keyspace), NewLogFieldString("table", frame.object), NewLogFieldError("err", err))
		return
	}

	switch frame.change {
	case SchemaChangeCreated:
		s.schemaUpdateListener.TableCreated(TableCreatedEvent{Table: newTable})
	case SchemaChangeUpdated:
		s.schemaUpdateListener.TableUpdated(TableUpdatedEvent{OldTable: oldTable, NewTable: newTable})
	case SchemaChangeDropped:
		s.schemaUpdateListener.TableDropped(TableDroppedEvent{Table: newTable})
	}
}

func (s *Session) handleAggregateChange(frame *schemaChangeAggregate) {
	if s.schemaUpdateListener == nil {
		s.schemaDescriber.clearSchema(frame.keyspace)
		return
	}

	keyspace := frame.keyspace

	var oldAggregateMeta *AggregateMetadata
	var err error

	if frame.change == SchemaChangeUpdated {
		oldAggregateMeta, err = s.schemaDescriber.getAggregateSchema(frame.keyspace, frame.name)
		if err != nil {
			s.logger.Error("Unable to get old aggregate metadata for updated aggregate.",
				NewLogFieldString("keyspace", keyspace), NewLogFieldString("aggregate", frame.name), NewLogFieldError("err", err))
			return
		}
	}

	s.schemaDescriber.clearSchema(keyspace)
	newAggregate, err := s.schemaDescriber.getAggregateSchema(frame.keyspace, frame.name)
	if err != nil {
		s.logger.Error("Unable to get new aggregate metadata for updated aggregate.",
			NewLogFieldString("keyspace", keyspace), NewLogFieldString("aggregate", frame.name), NewLogFieldError("err", err))
		return
	}

	switch frame.change {
	case SchemaChangeCreated:
		s.schemaUpdateListener.AggregateCreated(AggregateCreatedEvent{Aggregate: newAggregate})
	case SchemaChangeUpdated:
		s.schemaUpdateListener.AggregateUpdated(AggregateUpdatedEvent{OldAggregate: oldAggregateMeta, NewAggregate: newAggregate})
	case SchemaChangeDropped:
		s.schemaUpdateListener.AggregateDropped(AggregateDroppedEvent{Aggregate: newAggregate})
	}
}

func (s *Session) handleTypeChange(frame *schemaChangeType) {
	if s.schemaUpdateListener == nil {
		s.schemaDescriber.clearSchema(frame.keyspace)
		return
	}

	keyspace := frame.keyspace

	var oldTypeMeta *UserTypeMetadata
	var err error

	if frame.change == SchemaChangeUpdated {
		oldTypeMeta, err = s.schemaDescriber.getUserTypeSchema(frame.keyspace, frame.object)
		if err != nil {
			s.logger.Error("Unable to get old type metadata for updated type.",
				NewLogFieldString("keyspace", keyspace), NewLogFieldString("type", frame.object), NewLogFieldError("err", err))
			return
		}
	}

	s.schemaDescriber.clearSchema(keyspace)
	newType, err := s.schemaDescriber.getUserTypeSchema(frame.keyspace, frame.object)
	if err != nil {
		s.logger.Error("Unable to get new type metadata for updated type.",
			NewLogFieldString("keyspace", keyspace), NewLogFieldString("type", frame.object), NewLogFieldError("err", err))
		return
	}

	switch frame.change {
	case SchemaChangeCreated:
		s.schemaUpdateListener.UserTypeCreated(UserTypeCreatedEvent{Type: newType})
	case SchemaChangeUpdated:
		s.schemaUpdateListener.UserTypeUpdated(UserTypeUpdatedEvent{OldType: oldTypeMeta, NewType: newType})
	case SchemaChangeDropped:
		s.schemaUpdateListener.UserTypeDropped(UserTypeDroppedEvent{Type: newType})
	}
}

func (s *Session) handleFunctionChange(frame *schemaChangeFunction) {
	if s.schemaUpdateListener == nil {
		s.schemaDescriber.clearSchema(frame.keyspace)
		return
	}

	keyspace := frame.keyspace

	var oldFunction *FunctionMetadata
	var err error

	if frame.change == SchemaChangeUpdated {
		oldFunction, err = s.schemaDescriber.getFunctionSchema(frame.keyspace, frame.name)
		if err != nil {
			s.logger.Error("Unable to get old function metadata for updated function.",
				NewLogFieldString("keyspace", keyspace), NewLogFieldString("function", frame.name), NewLogFieldError("err", err))
			return
		}
	}

	s.schemaDescriber.clearSchema(keyspace)
	newFunction, err := s.schemaDescriber.getFunctionSchema(frame.keyspace, frame.name)
	if err != nil {
		s.logger.Error("Unable to get new function metadata for updated function.",
			NewLogFieldString("keyspace", keyspace), NewLogFieldString("function", frame.name), NewLogFieldError("err", err))
		return
	}

	switch frame.change {
	case SchemaChangeCreated:
		s.schemaUpdateListener.FunctionCreated(FunctionCreatedEvent{Function: newFunction})
	case SchemaChangeUpdated:
		s.schemaUpdateListener.FunctionUpdated(FunctionUpdatedEvent{OldFunction: oldFunction, NewFunction: newFunction})
	case SchemaChangeDropped:
		s.schemaUpdateListener.FunctionDropped(FunctionDroppedEvent{Function: newFunction})
	}
}

// handleNodeEvent handles inbound status and topology change events.
//
// Status events are debounced by host IP; only the latest event is processed.
//
// Topology events are debounced by performing a single full topology refresh
// whenever any topology event comes in.
//
// Processing topology change events before status change events ensures
// that a NEW_NODE event is not dropped in favor of a newer UP event (which
// would itself be dropped/ignored, as the node is not yet known).
func (s *Session) handleNodeEvent(frames []frame) {
	type nodeEvent struct {
		change string
		host   net.IP
		port   int
	}

	topologyEventReceived := false
	// status change events
	sEvents := make(map[string]*nodeEvent)

	for _, frame := range frames {
		switch f := frame.(type) {
		case *topologyChangeEventFrame:
			s.logger.Info("Received topology change event.",
				NewLogFieldString("frame", strings.Join([]string{f.change, "->", f.host.String(), ":", strconv.Itoa(f.port)}, "")))
			topologyEventReceived = true
		case *statusChangeEventFrame:
			event, ok := sEvents[f.host.String()]
			if !ok {
				event = &nodeEvent{change: f.change, host: f.host, port: f.port}
				sEvents[f.host.String()] = event
			}
			event.change = f.change
		}
	}

	if topologyEventReceived && !s.cfg.Events.DisableTopologyEvents {
		s.debounceRingRefresh()
	}

	for _, f := range sEvents {
		s.logger.Info("Dispatching status change event.",
			NewLogFieldString("frame", strings.Join([]string{f.change, "->", f.host.String(), ":", strconv.Itoa(f.port)}, "")))

		// ignore events we received if they were disabled
		// see https://github.com/apache/cassandra-gocql-driver/issues/1591
		switch f.change {
		case "UP":
			if !s.cfg.Events.DisableNodeStatusEvents {
				s.handleNodeUp(f.host, f.port)
			}
		case "DOWN":
			if !s.cfg.Events.DisableNodeStatusEvents {
				s.handleNodeDown(f.host, f.port)
			}
		}
	}
}

func (s *Session) handleNodeUp(eventIp net.IP, eventPort int) {
	s.logger.Info("Node is UP.",
		NewLogFieldStringer("event_ip", eventIp), NewLogFieldInt("event_port", eventPort))

	host, ok := s.ring.getHostByIP(eventIp.String())
	if !ok {
		s.debounceRingRefresh()
		return
	}

	if s.cfg.filterHost(host) {
		return
	}

	if d := host.Version().nodeUpDelay(); d > 0 {
		time.Sleep(d)
	}
	s.startPoolFill(host)

	// if s.nodeStateListener != nil {
	// 	s.nodeStateListener.OnNodeUp(host)
	// }
}

func (s *Session) startPoolFill(host *HostInfo) {
	// we let the pool call handleNodeConnected to change the host state
	s.pool.addHost(host)
	s.policy.AddHost(host)
}

func (s *Session) handleNodeConnected(host *HostInfo) {
	s.logger.Debug("Pool connected to node.",
		NewLogFieldIP("host_addr", host.ConnectAddress()), NewLogFieldInt("port", host.Port()), NewLogFieldString("host_id", host.HostID()))

	host.setState(NodeUp)

	if !s.cfg.filterHost(host) {
		s.policy.HostUp(host)
	}

	// if s.nodeStateListener != nil {
	// 	s.nodeStateListener.OnNodeConnected(host)
	// }
}

func (s *Session) handleNodeDown(ip net.IP, port int) {
	s.logger.Warning("Node is DOWN.",
		NewLogFieldIP("host_addr", ip), NewLogFieldInt("port", port))

	host, ok := s.ring.getHostByIP(ip.String())
	if ok {
		host.setState(NodeDown)
		if s.cfg.filterHost(host) {
			return
		}

		s.policy.HostDown(host)
		hostID := host.HostID()
		s.pool.removeHost(hostID)
	}

	// if s.nodeStateListener != nil {
	// 	// TODO: if host is nil it means that we didn't have that host in the ring, do we have to handle this?
	// 	if host == nil {
	// 		// Expecting this never throws an error as the IP is valid
	// 		host, _ = NewHostInfoFromAddrPort(ip, port)
	// 	}
	// 	s.nodeStateListener.OnNodeDown(host)
	// }
}

// type NodeStateListener interface {
// 	// Triggered when a node UP status event is received
// 	OnNodeUp(host *HostInfo)

// 	// Triggered when a node DOWN status event is received
// 	OnNodeDown(host *HostInfo)

// 	// Triggered when a node has been connected to successfully
// 	OnNodeConnected(host *HostInfo)
// }

type SchemaChangeListener interface {
	// Triggered when a keyspace update event is received
	KeyspaceCreated(event KeyspaceCreatedEvent)
	KeyspaceUpdated(event KeyspaceUpdatedEvent)
	KeyspaceDropped(event KeyspaceDroppedEvent)

	TableCreated(event TableCreatedEvent)
	TableUpdated(event TableUpdatedEvent)
	TableDropped(event TableDroppedEvent)

	AggregateCreated(event AggregateCreatedEvent)
	AggregateUpdated(event AggregateUpdatedEvent)
	AggregateDropped(event AggregateDroppedEvent)

	UserTypeCreated(event UserTypeCreatedEvent)
	UserTypeUpdated(event UserTypeUpdatedEvent)
	UserTypeDropped(event UserTypeDroppedEvent)

	FunctionCreated(event FunctionCreatedEvent)
	FunctionUpdated(event FunctionUpdatedEvent)
	FunctionDropped(event FunctionDroppedEvent)
}

const (
	SchemaChangeCreated = "CREATED"
	SchemaChangeUpdated = "UPDATED"
	SchemaChangeDropped = "DROPPED"
)

// KeyspaceCreatedEvent represents a keyspace creation event.
// It contains the metadata of the created keyspace.
type KeyspaceCreatedEvent struct {
	KeyspaceMetadata *KeyspaceMetadata
}

// KeyspaceUpdatedEvent represents a keyspace update event.
// It contains the old and new metadata of the updated keyspace.
type KeyspaceUpdatedEvent struct {
	OldKeyspaceMetadata *KeyspaceMetadata
	NewKeyspaceMetadata *KeyspaceMetadata
}

// KeyspaceDroppedEvent represents a keyspace drop event.
// It contains the metadata of the dropped keyspace.
type KeyspaceDroppedEvent struct {
	KeyspaceMetadata *KeyspaceMetadata
}

// TableCreatedEvent represents a table creation event.
// It contains the metadata of the created table.
type TableCreatedEvent struct {
	Table *TableMetadata
}

// TableUpdatedEvent represents a table update event.
// It contains the old and new metadata of the updated table.
type TableUpdatedEvent struct {
	OldTable *TableMetadata
	NewTable *TableMetadata
}

// TableDroppedEvent represents a table drop event.
// It contains the metadata of the dropped table.
type TableDroppedEvent struct {
	Table *TableMetadata
}

// AggregateCreatedEvent represents an aggregate creation event.
// It contains the metadata of the created aggregate.
type AggregateCreatedEvent struct {
	Aggregate *AggregateMetadata
}

// AggregateUpdatedEvent represents an aggregate update event.
// It contains the old and new metadata of the updated aggregate.
type AggregateUpdatedEvent struct {
	OldAggregate *AggregateMetadata
	NewAggregate *AggregateMetadata
}

// AggregateDroppedEvent represents an aggregate drop event.
// It contains the metadata of the dropped aggregate.
type AggregateDroppedEvent struct {
	Aggregate *AggregateMetadata
}

// UserTypeCreatedEvent represents a user-defined type creation event.
// It contains the metadata of the created user-defined type.
type UserTypeCreatedEvent struct {
	Type *UserTypeMetadata
}

// UserTypeUpdatedEvent represents a user-defined type update event.
// It contains the old and new metadata of the updated user-defined type.
type UserTypeUpdatedEvent struct {
	OldType *UserTypeMetadata
	NewType *UserTypeMetadata
}

// UserTypeDroppedEvent represents a user-defined type drop event.
// It contains the metadata of the dropped user-defined type.
type UserTypeDroppedEvent struct {
	Type *UserTypeMetadata
}

// FunctionCreatedEvent represents a function creation event.
// It contains the metadata of the created function.
type FunctionCreatedEvent struct {
	Function *FunctionMetadata
}

// FunctionUpdatedEvent represents a function update event.
// It contains the old and new metadata of the updated function.
type FunctionUpdatedEvent struct {
	OldFunction *FunctionMetadata
	NewFunction *FunctionMetadata
}

// FunctionDroppedEvent represents a function drop event.
// It contains the metadata of the dropped function.
type FunctionDroppedEvent struct {
	Function *FunctionMetadata
}
