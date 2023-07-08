package common

import (
	"bytes"
	"io"
)

// func CompressData(uncompressed []byte) []byte {
// 	return snappy.Encode(nil, uncompressed)
// }

// func DecompressReader(compressed []byte) (io.Reader, error) {
// 	uncompressed, err := snappy.Decode(nil, compressed)
// 	if err != nil {
// 		return nil, err
// 	}
// 	return bytes.NewReader(uncompressed), nil
// }

// DEBUG

func CompressData(uncompressed []byte) []byte {
	return uncompressed
}

func DecompressReader(compressed []byte) (io.Reader, error) {
	return bytes.NewReader(compressed), nil
}
