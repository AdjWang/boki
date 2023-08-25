package common

import (
	"hash/fnv"
)

var KeyCommitResult = NameHash("r")

func NameHash(name string) uint64 {
	h := fnv.New64a()
	h.Write([]byte(name))
	return h.Sum64()
}
