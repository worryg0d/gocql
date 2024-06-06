package gocql

import (
	"encoding/binary"
	"fmt"
	"github.com/golang/snappy"
	"github.com/pierrec/lz4/v4"
)

type Compressor interface {
	Name() string
	Encode(data []byte) ([]byte, error)
	Decode(data []byte) ([]byte, error)
	DecodeSized(data []byte, size uint32) ([]byte, error)
}

// SnappyCompressor implements the Compressor interface and can be used to
// compress incoming and outgoing frames. The snappy compression algorithm
// aims for very high speeds and reasonable compression.
type SnappyCompressor struct{}

func (s SnappyCompressor) Name() string {
	return "snappy"
}

func (s SnappyCompressor) Encode(data []byte) ([]byte, error) {
	return snappy.Encode(nil, data), nil
}

func (s SnappyCompressor) Decode(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}

func (s SnappyCompressor) DecodeSized(data []byte, size uint32) ([]byte, error) {
	buf := make([]byte, size)
	return snappy.Decode(buf, data)
}

type LZ4Compressor struct{}

func (s LZ4Compressor) Name() string {
	return "lz4"
}

func (s LZ4Compressor) Encode(data []byte) ([]byte, error) {
	buf := make([]byte, lz4.CompressBlockBound(len(data)+4))
	var compressor lz4.Compressor
	n, err := compressor.CompressBlock(data, buf[4:])
	// According to lz4.CompressBlock doc, it doesn't fail as long as the dst
	// buffer length is at least lz4.CompressBlockBound(len(data))) bytes, but
	// we check for error anyway just to be thorough.
	if err != nil {
		return nil, err
	}
	binary.BigEndian.PutUint32(buf, uint32(len(data)))
	return buf[:n+4], nil
}

func (s LZ4Compressor) Decode(data []byte) ([]byte, error) {
	if len(data) < 4 {
		return nil, fmt.Errorf("cassandra lz4 block size should be >4, got=%d", len(data))
	}
	uncompressedLength := binary.BigEndian.Uint32(data)
	if uncompressedLength == 0 {
		return nil, nil
	}
	buf := make([]byte, uncompressedLength)
	n, err := lz4.UncompressBlock(data[4:], buf)
	return buf[:n], err
}

func (s LZ4Compressor) DecodeSized(data []byte, size uint32) ([]byte, error) {
	buf := make([]byte, size)
	_, err := lz4.UncompressBlock(data, buf)
	if err != nil {
		return nil, err
	}

	return buf, nil
}
