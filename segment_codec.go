// segment_codec.go

package gocql

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	maxSegmentPayloadSize = 1<<17 - 1

	compressedHeaderSize   = 3
	uncompressedHeaderSize = 5

	crc24Size = 3
	crc32Size = 4
)

// segmentHeader represents the header information of a segment.
type segmentHeader struct {
	// payload length is the length of the segment payload
	payloadLength int
	// uncompressedPayloadLength is the length of the uncompressed payload (only for compressed segments)
	uncompressedPayloadLength int
	// indicates whether the segment contains only completed frames
	isSelfContained bool
	// checksum of the header
	crc24 uint32
}

func (segment *segmentHeader) String() string {
	return fmt.Sprintf("segmentHeader(len=%d, uncompressedLen=%d, isSelfContained=%v, crc24=%d)",
		segment.payloadLength,
		segment.uncompressedPayloadLength,
		segment.isSelfContained,
		segment.crc24)
}

type segmentCodec struct {
	compressor Compressor
	compressed bool
}

func newSegmentCodec(compressor Compressor) *segmentCodec {
	return &segmentCodec{
		compressed: compressor != nil,
		compressor: compressor,
	}
}

func (sc *segmentCodec) encode(payload []byte, isSelfContained bool) ([]byte, error) {
	if len(payload) > maxSegmentPayloadSize {
		return nil, fmt.Errorf("gocql: payload length (%d) exceeds maximum segment size of %d", len(payload), maxSegmentPayloadSize)
	}

	if sc.compressed {
		return sc.encodeCompressedSegment(payload, isSelfContained)
	}
	return sc.encodeUncompressedSegment(payload, isSelfContained)
}

func (sc *segmentCodec) encodeCompressedSegment(payload []byte, isSelfContained bool) ([]byte, error) {
	uncompressedLen := len(payload)

	compressed, err := sc.compressor.AppendCompressed(nil, payload)
	if err != nil {
		return nil, err
	}

	compressedLen := len(compressed)

	// If compression is not worth it, we should send uncompressed data
	// following the next logic:
	if uncompressedLen < compressedLen {
		compressed = payload
		compressedLen = uncompressedLen
		uncompressedLen = 0
	}

	combined := uint64(compressedLen) | uint64(uncompressedLen)<<17
	if isSelfContained {
		combined |= 1 << 34
	}

	segment := make([]byte, 0, compressedHeaderSize+crc24Size+compressedLen+crc32Size)

	var headerBuf [8]byte
	binary.LittleEndian.PutUint64(headerBuf[:], combined)
	segment = append(segment, headerBuf[:5]...)

	headerChecksum := Crc24(segment[:5])
	segment = append(segment,
		byte(headerChecksum),
		byte(headerChecksum>>8),
		byte(headerChecksum>>16),
	)

	segment = append(segment, compressed...)

	payloadChecksum := Crc32(compressed)
	binary.LittleEndian.PutUint32(headerBuf[:], payloadChecksum)
	segment = append(segment, headerBuf[:4]...)

	return segment, nil
}

func (sc *segmentCodec) encodeUncompressedSegment(payload []byte, isSelfContained bool) ([]byte, error) {
	payloadLen := len(payload)

	headerInt := uint32(payloadLen)
	if isSelfContained {
		headerInt |= 1 << 17
	}

	segment := make([]byte, 0, uncompressedHeaderSize+crc24Size+payloadLen+crc32Size)
	segment = append(segment,
		byte(headerInt),
		byte(headerInt>>8),
		byte(headerInt>>16),
	)

	crc := Crc24(segment[:3])
	segment = append(segment,
		byte(crc),
		byte(crc>>8),
		byte(crc>>16),
	)

	segment = append(segment, payload...)

	payloadCRC32 := Crc32(payload)
	var buf [4]byte
	binary.LittleEndian.PutUint32(buf[:], payloadCRC32)
	segment = append(segment, buf[:]...)

	return segment, nil
}

func (sc *segmentCodec) decode(r io.Reader) ([]byte, bool, error) {
	if sc.compressed {
		return sc.decodeCompressedSegment(r)
	}
	return sc.decodeUncompressedSegment(r)
}

func (sc *segmentCodec) decodeCompressedSegment(r io.Reader) ([]byte, bool, error) {
	header, err := sc.decodeCompressedSegmentHeader(r)
	if err != nil {
		return nil, false, fmt.Errorf("gocql: failed to read compressed segment header, err: %w", err)
	}

	compressedPayload, err := sc.decodePayload(r, header)
	if err != nil {
		return nil, false, fmt.Errorf("gocql: failed to read compressed segment payload, err: %w", err)
	}

	var uncompressedPayload []byte
	if header.uncompressedPayloadLength > 0 {
		uncompressedPayload, err = sc.compressor.AppendDecompressed(nil, compressedPayload, uint32(header.uncompressedPayloadLength))
		if err != nil {
			return nil, false, err
		}
		// Verify that the decompressed length matches the expected length
		if uint32(len(uncompressedPayload)) != uint32(header.uncompressedPayloadLength) {
			return nil, false, fmt.Errorf("gocql: length mismatch after payload decompressing, got %d, expected %d", len(uncompressedPayload), header.uncompressedPayloadLength)
		}
	} else {
		// in case when the segment was not compressed because compression was not worth it
		uncompressedPayload = compressedPayload
	}

	return uncompressedPayload, header.isSelfContained, nil
}

func (sc *segmentCodec) decodeUncompressedSegment(r io.Reader) ([]byte, bool, error) {
	header, err := sc.decodeUncompressedSegmentHeader(r)
	if err != nil {
		return nil, false, fmt.Errorf("gocql: failed to read uncompressed segment header, err: %w", err)
	}

	payload, err := sc.decodePayload(r, header)
	if err != nil {
		return nil, false, fmt.Errorf("gocql: failed to read uncompressed segment payload, err: %w", err)
	}

	return payload, header.isSelfContained, nil
}

// verifySegmentHeaderChecksum verifies the CRC24 checksum of the segment header.
func (sc *segmentCodec) verifySegmentHeaderChecksum(data []byte, expected uint32) error {
	computed := Crc24(data)
	if computed != expected {
		return fmt.Errorf("gocql: crc24 mismatch in segment header: expected %d, got %d", expected, computed)
	}
	return nil
}

// verifySegmentPayloadChecksum verifies the CRC32 checksum of the segment payload.
func (sc *segmentCodec) verifySegmentPayloadChecksum(data []byte, expected uint32) error {
	computed := Crc32(data)
	if computed != expected {
		return fmt.Errorf("gocql: payload crc32 mismatch in segment payload: expected %d, got %d", expected, computed)
	}
	return nil
}

// decodeCompressedSegmentHeader reads and verifies the header of a compressed segment from the given reader.
func (sc *segmentCodec) decodeCompressedSegmentHeader(r io.Reader) (*segmentHeader, error) {
	var headerBuf [8]byte // TODO: potentially optimize allocation, could be stored in segmentCodec and reused if the codec is a specific for each Conn

	if _, err := io.ReadFull(r, headerBuf[:8]); err != nil {
		return nil, err
	}

	readHeaderChecksum := uint32(headerBuf[5]) | uint32(headerBuf[6])<<8 | uint32(headerBuf[7])<<16
	err := sc.verifySegmentHeaderChecksum(headerBuf[:5], readHeaderChecksum)
	if err != nil {
		return nil, err
	}

	compressedLen := uint32(headerBuf[0]) | uint32(headerBuf[1])<<8 | uint32(headerBuf[2]&0x1)<<16
	uncompressedLen := (uint32(headerBuf[2]) >> 1) | uint32(headerBuf[3])<<7 | uint32(headerBuf[4]&0b11)<<15
	selfContained := (headerBuf[4] & 0b100) != 0

	return &segmentHeader{
		payloadLength:             int(compressedLen),
		uncompressedPayloadLength: int(uncompressedLen),
		isSelfContained:           selfContained,
	}, nil
}

// decodeUncompressedSegmentHeader reads and verifies the header of an uncompressed segment from the given reader.
func (sc *segmentCodec) decodeUncompressedSegmentHeader(r io.Reader) (*segmentHeader, error) {
	var header [6]byte

	if _, err := io.ReadFull(r, header[:]); err != nil {
		return nil, err
	}

	readHeaderCRC24 := uint32(header[3]) | uint32(header[4])<<8 | uint32(header[5])<<16
	err := sc.verifySegmentHeaderChecksum(header[:3], readHeaderCRC24)
	if err != nil {
		return nil, err
	}

	headerInt := uint32(header[0]) | uint32(header[1])<<8 | uint32(header[2])<<16
	payloadLen := int(headerInt & maxSegmentPayloadSize)
	isSelfContained := (headerInt & (1 << 17)) != 0

	return &segmentHeader{
		payloadLength:   payloadLen,
		isSelfContained: isSelfContained,
	}, nil
}

// decodePayload reads and verifies the payload of a segment from the given reader.
func (sc *segmentCodec) decodePayload(r io.Reader, header *segmentHeader) ([]byte, error) {
	payload := make([]byte, header.payloadLength)
	if _, err := io.ReadFull(r, payload); err != nil {
		return nil, err
	}

	var crcBuf [4]byte
	if _, err := io.ReadFull(r, crcBuf[:]); err != nil {
		return nil, fmt.Errorf("gocql: failed to read segment payload crc32, err: %w", err)
	}

	readPayloadCRC32 := binary.LittleEndian.Uint32(crcBuf[:])
	err := sc.verifySegmentPayloadChecksum(payload, readPayloadCRC32)
	if err != nil {
		return nil, err
	}

	return payload, nil
}
