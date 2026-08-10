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
	"sync"
)

// AuthRegistry is the interface for a registry of authenticators to support authentication negotiation between the client and the server (CEP-50).
type AuthRegistry interface {
	// Returns the list of all registered authenticators.
	Authenticators() []NegotiableAuthenticator

	// Registers a new authenticator.
	//
	// authenticator is the NegotiableAuthenticator to register.
	Register(authenticator NegotiableAuthenticator)

	// Returns the authenticator for the given class name.
	// If the authenticator is not found, returns false.
	AuthenticatorFor(className string) (NegotiableAuthenticator, bool)
}

// The default implementation of the AuthRegistry interface.
type defaultAuthRegistry struct {
	mu sync.RWMutex
	// Map of Java class name to authenticator.
	byClassName map[string]NegotiableAuthenticator
	// List of all registered authenticators. Used for iteration.
	all []NegotiableAuthenticator
}

// Creates a new default implementation of the AuthRegistry interface.
func NewDefaultAuthRegistry() AuthRegistry {
	return &defaultAuthRegistry{
		byClassName: make(map[string]NegotiableAuthenticator),
		all:         make([]NegotiableAuthenticator, 0),
	}
}

func (r *defaultAuthRegistry) Register(authenticator NegotiableAuthenticator) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.mustRegisterLocked(authenticator)
}

// Registers the authenticator with the given class name.
// Panics if the authenticator is already registered.
// Must be called with the lock held.
func (r *defaultAuthRegistry) mustRegisterLocked(authenticator NegotiableAuthenticator) {
	className := authenticator.ClassName()
	if _, ok := r.byClassName[className]; ok {
		panic(fmt.Sprintf("gocql: authenticator %s already registered", className))
	}
	r.byClassName[className] = authenticator
	r.all = append(r.all, authenticator)
}

// Implements the AuthRegistry interface. Returns a copy of the list of all registered authenticators.
func (r *defaultAuthRegistry) Authenticators() []NegotiableAuthenticator {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return append([]NegotiableAuthenticator(nil), r.all...)
}

// Returns the authenticator for the given class name.
// If the authenticator is not found, returns false.
func (r *defaultAuthRegistry) AuthenticatorFor(className string) (NegotiableAuthenticator, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	authenticator, ok := r.byClassName[className]
	return authenticator, ok
}
