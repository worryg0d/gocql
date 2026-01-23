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
/*
 * Content before git sha 34fdeebefcbf183ed7f916f931aa0586fdaa1b40
 * Copyright (c) 2016, The Gocql authors,
 * provided under the BSD-3-Clause License.
 * See the NOTICE file distributed with this work for additional information.
 */

package ccm

import (
	"bufio"
	"bytes"
	"fmt"
	"os/exec"
	"runtime"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func execCmd(args ...string) (*bytes.Buffer, error) {
	execName := "ccm"
	if runtime.GOOS == "windows" {
		args = append([]string{"/c", execName}, args...)
		execName = "cmd.exe"
	}
	cmd := exec.Command(execName, args...)
	stdout := &bytes.Buffer{}
	cmd.Stdout = stdout
	cmd.Stderr = &bytes.Buffer{}
	if err := cmd.Run(); err != nil {
		return nil, fmt.Errorf("Failed to execute command: [ %s ], err: %w, stderr: %s", cmd.String(), err, cmd.Stderr.(*bytes.Buffer).String())
	}

	return stdout, nil
}

func AllUp() error {
	status, err := Status()
	if err != nil {
		return err
	}

	for _, host := range status {
		if !host.State.IsUp() {
			if err := NodeUp(host.Name); err != nil {
				return err
			}
		}
	}

	return nil
}

func NodeUp(node string) error {
	args := []string{node, "start", "--wait-for-binary-proto"}
	if runtime.GOOS == "windows" {
		args = append(args, "--quiet-windows")
	}
	_, err := execCmd(args...)
	return err
}

func NodeDown(node string) error {
	_, err := execCmd(node, "stop")
	return err
}

type Host struct {
	State NodeState
	Addr  string
	Name  string
}

type NodeState int

func (n NodeState) String() string {
	if n == NodeStateUp {
		return "UP"
	} else if n == NodeStateDown {
		return "DOWN"
	} else {
		return fmt.Sprintf("UNKNOWN_STATE_%d", n)
	}
}

func (n NodeState) IsUp() bool {
	return n == NodeStateUp
}

const (
	NodeStateUp NodeState = iota
	NodeStateDown
)

func Status() (map[string]Host, error) {
	// TODO: parse into struct to manipulate
	out, err := execCmd("status", "-v")
	if err != nil {
		return nil, err
	}

	const (
		stateCluster = iota
		stateCommas
		stateNode
		stateOption
	)

	nodes := make(map[string]Host)
	// didnt really want to write a full state machine parser
	state := stateCluster
	sc := bufio.NewScanner(out)

	var host Host

	for sc.Scan() {
		switch state {
		case stateCluster:
			text := sc.Text()
			if !strings.HasPrefix(text, "Cluster:") {
				return nil, fmt.Errorf("expected 'Cluster:' got %q", text)
			}
			state = stateCommas
		case stateCommas:
			text := sc.Text()
			if !strings.HasPrefix(text, "-") {
				return nil, fmt.Errorf("expected commas got %q", text)
			}
			state = stateNode
		case stateNode:
			// assume nodes start with node
			text := sc.Text()
			if !strings.HasPrefix(text, "node") {
				return nil, fmt.Errorf("expected 'node' got %q", text)
			}
			line := strings.Split(text, ":")
			host.Name = line[0]

			nodeState := strings.TrimSpace(line[1])
			switch nodeState {
			case "UP":
				host.State = NodeStateUp
			case "DOWN":
				host.State = NodeStateDown
			default:
				return nil, fmt.Errorf("unknown node state from ccm: %q", nodeState)
			}

			state = stateOption
		case stateOption:
			text := sc.Text()
			if text == "" {
				state = stateNode
				nodes[host.Name] = host
				host = Host{}
				continue
			}

			line := strings.Split(strings.TrimSpace(text), "=")
			k, v := line[0], line[1]
			if k == "binary" {
				// could check errors
				// ('127.0.0.1', 9042)
				v = v[2:] // (''
				if i := strings.IndexByte(v, '\''); i < 0 {
					return nil, fmt.Errorf("invalid binary v=%q", v)
				} else {
					host.Addr = v[:i]
					// dont need port
				}
			}
		default:
			return nil, fmt.Errorf("unexpected state: %q", state)
		}
	}

	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("unable to parse ccm status: %v", err)
	}

	return nodes, nil
}

func Hosts() ([]Host, error) {
	status, err := Status()
	if err != nil {
		return nil, err
	}

	hosts := make([]Host, 0, len(status))
	for _, host := range status {
		hosts = append(hosts, host)
	}

	return hosts, nil
}

// Runs ccm start --wait-for-binary-proto to start the cluster or stopped nodes if any
func StartAll() error {
	_, err := execCmd("start", "--wait-for-binary-proto")
	return err
}

func DownAll() error {
	_, err := execCmd("stop")
	return err
}

type ClusterMetadata struct {
	Name  string
	Hosts []Host
}

func CurrentCluster() (*ClusterMetadata, error) {
	err := AllUp()
	if err != nil {
		return nil, err
	}

	hosts, err := Hosts()
	if err != nil {
		return nil, err
	}

	if len(hosts) < 1 {
		return nil, fmt.Errorf("no nodes in cluster")
	}

	cluster := &ClusterMetadata{
		Name:  hosts[0].Name,
		Hosts: hosts,
	}

	return cluster, nil
}

// DoWhenAllNodesUp runs the provided function while ensuring that all nodes are up
// before and after its execution.
func DoWhenAllNodesUp(t testing.TB, fn func(metadata *ClusterMetadata)) {
	t.Helper()

	defer func() {
		t.Log("Starting nodes of the CCM cluster...")
		err := StartAll()
		require.NoError(t, err)
		t.Log("All nodes started.")

		// Wait for all nodes to be up

		err = AllUp()
		require.NoError(t, err)
	}()

	metadata, err := CurrentCluster()
	require.NoError(t, err)

	fn(metadata)
}
