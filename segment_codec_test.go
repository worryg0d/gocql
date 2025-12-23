//go:build all || unit
// +build all unit

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
	"bytes"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testMockedCompressor struct {
	// this is an error its methods should return
	expectedError error

	// invalidateDecodedDataLength allows to simulate data decoding invalidation
	invalidateDecodedDataLength bool
}

func (m testMockedCompressor) Name() string {
	return "testMockedCompressor"
}

func (m testMockedCompressor) AppendCompressed(_, src []byte) ([]byte, error) {
	if m.expectedError != nil {
		return nil, m.expectedError
	}
	return src, nil
}

func (m testMockedCompressor) AppendDecompressed(_, src []byte, decompressedLength uint32) ([]byte, error) {
	if m.expectedError != nil {
		return nil, m.expectedError
	}

	// simulating invalid size of decoded data
	if m.invalidateDecodedDataLength {
		return src[:decompressedLength-1], nil
	}

	return src, nil
}

func (m testMockedCompressor) AppendCompressedWithLength(dst, src []byte) ([]byte, error) {
	panic("testMockedCompressor.AppendCompressedWithLength is not implemented")
}

func (m testMockedCompressor) AppendDecompressedWithLength(dst, src []byte) ([]byte, error) {
	panic("testMockedCompressor.AppendDecompressedWithLength is not implemented")
}

func Test_readUncompressedFrame(t *testing.T) {
	tests := []struct {
		name        string
		modifyFrame func([]byte) []byte
		expectedErr string
	}{
		{
			name: "header crc24 mismatch",
			modifyFrame: func(frame []byte) []byte {
				// simulating some crc invalidation
				frame[0] = 255
				return frame
			},
			expectedErr: "gocql: crc24 mismatch in segment header",
		},
		{
			name: "body crc32 mismatch",
			modifyFrame: func(frame []byte) []byte {
				// simulating body crc32 mismatch
				frame[len(frame)-1] = 255
				return frame
			},
			expectedErr: "gocql: payload crc32 mismatch in segment payload",
		},
		{
			name: "invalid frame length",
			modifyFrame: func(frame []byte) []byte {
				// simulating body length invalidation
				frame = frame[:7]
				return frame
			},
			expectedErr: "gocql: failed to read uncompressed segment payload",
		},
		{
			name: "cannot read body checksum",
			modifyFrame: func(frame []byte) []byte {
				// simulating body length invalidation
				frame = frame[:len(frame)-4]
				return frame
			},
			expectedErr: "gocql: failed to read segment payload crc32",
		},
		{
			name:        "success",
			modifyFrame: nil,
			expectedErr: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			framer := newFramer(nil, protoVersion5, GlobalTypes)
			req := writeQueryFrame{
				statement: "SELECT * FROM system.local",
				params: queryParams{
					consistency: Quorum,
					keyspace:    "gocql_test",
				},
			}

			err := req.buildFrame(framer, 128)
			require.NoError(t, err)

			segmentCodec := newSegmentCodec(nil)
			frame, err := segmentCodec.encode(framer.buf, true)
			require.NoError(t, err)

			if tt.modifyFrame != nil {
				frame = tt.modifyFrame(frame)
			}

			readFrame, isSelfContained, err := segmentCodec.decode(bytes.NewReader(frame))

			if tt.expectedErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedErr)
			} else {
				require.NoError(t, err)
				assert.True(t, isSelfContained)
				assert.Equal(t, framer.buf, readFrame)
			}
		})
	}
}

func Test_readCompressedFrame(t *testing.T) {
	tests := []struct {
		name string
		// modifyFrameFn is useful for simulating frame data invalidation
		modifyFrameFn func([]byte) []byte
		compressor    testMockedCompressor

		// expectedErrorMsg is an error message that should be returned by Error() method.
		// We need this to understand which of fmt.Errorf() is returned
		expectedErrorMsg string
	}{
		{
			name: "header crc24 mismatch",
			modifyFrameFn: func(frame []byte) []byte {
				// simulating some crc invalidation
				frame[0] = 255
				return frame
			},
			expectedErrorMsg: "gocql: crc24 mismatch in segment header",
		},
		{
			name: "body crc32 mismatch",
			modifyFrameFn: func(frame []byte) []byte {
				// simulating body crc32 mismatch
				frame[len(frame)-1] = 255
				return frame
			},
			expectedErrorMsg: "gocql: payload crc32 mismatch in segment payload",
		},
		{
			name: "invalid frame length",
			modifyFrameFn: func(frame []byte) []byte {
				// simulating body length invalidation
				return frame[:12]
			},
			expectedErrorMsg: "gocql: failed to read compressed segment payload",
		},
		{
			name: "cannot read body checksum",
			modifyFrameFn: func(frame []byte) []byte {
				// simulating body length invalidation
				return frame[:len(frame)-4]
			},
			expectedErrorMsg: "gocql: failed to read segment payload crc32",
		},
		{
			name:          "failed to encode payload",
			modifyFrameFn: nil,
			compressor: testMockedCompressor{
				expectedError: errors.New("failed to encode payload"),
			},
			expectedErrorMsg: "failed to encode payload",
		},
		{
			name:          "failed to decode payload",
			modifyFrameFn: nil,
			compressor: testMockedCompressor{
				expectedError: errors.New("failed to decode payload"),
			},
			expectedErrorMsg: "failed to decode payload",
		},
		{
			name:          "length mismatch after decompressing",
			modifyFrameFn: nil,
			compressor: testMockedCompressor{
				invalidateDecodedDataLength: true,
			},
			expectedErrorMsg: "gocql: length mismatch after payload decompressing",
		},
		{
			name:             "success",
			modifyFrameFn:    nil,
			expectedErrorMsg: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			framer := newFramer(nil, protoVersion5, GlobalTypes)
			req := writeQueryFrame{
				statement: "SELECT * FROM system.local",
				params: queryParams{
					consistency: Quorum,
					keyspace:    "gocql_test",
				},
			}

			err := req.buildFrame(framer, 128)
			require.NoError(t, err)

			segmentCodec1 := newSegmentCodec(testMockedCompressor{})
			frame, err := segmentCodec1.encode(framer.buf, true)
			require.NoError(t, err)

			if tt.modifyFrameFn != nil {
				frame = tt.modifyFrameFn(frame)
			}

			segmentCodec2 := newSegmentCodec(tt.compressor)
			readFrame, selfContained, err := segmentCodec2.decode(bytes.NewReader(frame))

			switch {
			case tt.expectedErrorMsg != "":
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expectedErrorMsg)
			case tt.compressor.expectedError != nil:
				require.ErrorIs(t, err, tt.compressor.expectedError)
			default:
				require.NoError(t, err)
				assert.True(t, selfContained)
				assert.Equal(t, framer.buf, readFrame)
			}
		})
	}
}
