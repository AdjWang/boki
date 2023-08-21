package common

import (
	"bytes"
	"io"

	"github.com/golang/snappy"
)

// DEBUG
func CompressData(uncompressed []byte) []byte {
	// return snappy.Encode(nil, uncompressed)
	return uncompressed
}

// DEBUG
func DecompressData(compressed []byte) ([]byte, error) {
	// return snappy.Decode(nil, compressed)
	return compressed, nil
}

// DEBUG
func DecompressReader(compressed []byte) (io.Reader, error) {
	uncompressed, err := snappy.Decode(nil, compressed)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(uncompressed), nil
}
