package common

// Now aux data is compressed when returning from engine to worker
// FUTURE: add slib level compress when necessary

func CompressData(uncompressed []byte) []byte {
	// compressed := snappy.Encode(nil, uncompressed)
	// encoded := base64.StdEncoding.EncodeToString(compressed)
	// return []byte(encoded)
	return uncompressed
}

func DecompressData(compressed []byte) ([]byte, error) {
	// decoded, err := base64.StdEncoding.DecodeString(string(compressed))
	// if err != nil {
	// 	return nil, err
	// }
	// return snappy.Decode(nil, decoded)
	return compressed, nil
}
