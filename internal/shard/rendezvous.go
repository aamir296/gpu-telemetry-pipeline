package shard

import "hash/fnv"

// Owner returns the rendezvous-hash owner for a key from a stable member list.
func Owner(namespace, key string, members []string) string {
	var owner string
	var best uint64
	for _, member := range members {
		h := fnv.New64a()
		_, _ = h.Write([]byte(namespace))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(key))
		_, _ = h.Write([]byte{0})
		_, _ = h.Write([]byte(member))
		if score := h.Sum64(); owner == "" || score > best {
			owner, best = member, score
		}
	}
	return owner
}
