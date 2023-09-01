package common

import (
	"encoding/base64"

	"github.com/golang/snappy"
)

// Version1 is used by boki's statestore
// Version2 is used by asyncstore
// Separate them due to the difference on aux index so we can control
// whether to compress on demand.

func CompressData1(uncompressed []byte) []byte {
	compressed := snappy.Encode(nil, uncompressed)
	encoded := base64.StdEncoding.EncodeToString(compressed)
	return []byte(encoded)
}
func DecompressData1(compressed []byte) ([]byte, error) {
	decoded, err := base64.StdEncoding.DecodeString(string(compressed))
	if err != nil {
		return nil, err
	}
	return snappy.Decode(nil, decoded)
}

func CompressData2(uncompressed []byte) []byte {
	// compressed := snappy.Encode(nil, uncompressed)
	// encoded := base64.StdEncoding.EncodeToString(compressed)
	// return []byte(encoded)
	return uncompressed
}
func DecompressData2(compressed []byte) ([]byte, error) {
	// decoded, err := base64.StdEncoding.DecodeString(string(compressed))
	// if err != nil {
	// 	return nil, err
	// }
	// return snappy.Decode(nil, decoded)
	return compressed, nil
}
