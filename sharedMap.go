package sharded

import (
	"encoding/json"
	"hash/fnv"
	"sync"
)

type (
	// KT is an alias for interface{}
	KT interface{}
	// VT is an alias for interface{}
	VT interface{}
)

type shard struct {
	sync.RWMutex
	items map[KT]VT
}

// Map is a container of shards.
type Map struct {
	shards []*shard
}

// New returns a ShardedMap with shardCount shards in it.
func New(shardCount int) *Map {
	m := &Map{
		shards: make([]*shard, shardCount),
	}

	for i := 0; i < shardCount; i++ {
		m.shards[i] = &shard{
			items: make(map[KT]VT),
		}
	}
	return m
}

func getShardNumber(key KT, numberOfShards int) int {
	if numberOfShards == 1 {
		return 0
	}

	j, _ := json.Marshal(key)

	hasher := fnv.New32()
	hasher.Write(j)
	return int(hasher.Sum32()) % numberOfShards
}

func (m *Map) getShard(key KT) *shard {
	return m.shards[getShardNumber(key, len(m.shards))]
}

// Set sets the given value under the specified key.
func (m *Map) Set(key KT, value VT) {
	shard := m.getShard(key)

	shard.Lock()
	defer shard.Unlock()

	shard.items[key] = value
}

// Get retrieves an element from map under given key.
func (m *Map) Get(key KT) (VT, bool) {
	shard := m.getShard(key)

	shard.RLock()
	defer shard.RUnlock()

	val, ok := shard.items[key]
	return val, ok
}

// Count returns the number of elements within the map.
func (m *Map) Count() int {
	count := 0
	for i := 0; i < len(m.shards); i++ {
		shard := m.shards[i]
		shard.RLock()
		count += len(shard.items)
		shard.RUnlock()
	}
	return count
}

// Has looks up an item under specified key
func (m *Map) Has(key KT) bool {
	_, ok := m.Get(key)
	return ok
}

// Remove removes an element from the map.
func (m *Map) Remove(key KT) {
	shard := m.getShard(key)
	shard.Lock()
	defer shard.Unlock()
	delete(shard.items, key)
}

// IsEmpty checks if map is empty.
func (m *Map) IsEmpty() bool {
	return m.Count() == 0
}

// Tuple is used by the Iter & IterBuffered functions to wrap two variables together over a channel,
type Tuple struct {
	Key KT
	Val VT
}

// Iter returns an iterator which could be used in a for range loop.
func (m *Map) Iter() <-chan Tuple {
	ch := make(chan Tuple)
	go func() {
		// Foreach shard.
		for _, shard := range m.shards {
			// For each key, value pair.
			shard.RLock()
			for key, val := range shard.items {
				ch <- Tuple{key, val}
			}
			shard.RUnlock()
		}
		close(ch)
	}()
	return ch
}

// IterBuffered returns a buffered iterator which could be used in a for range loop.
// The buffer size is set to the size of the map so that the go-routine can fill the channel
// with item in one go and the caller can iterate in its own sweet time.
func (m *Map) IterBuffered() <-chan Tuple {
	ch := make(chan Tuple, m.Count())
	go func() {
		// For each shard.
		for _, shard := range m.shards {
			// Foreach key, value pair.
			shard.RLock()
			for key, val := range shard.items {
				ch <- Tuple{key, val}
			}
			shard.RUnlock()
		}
		close(ch)
	}()
	return ch
}

// Update calls `fn` with the key's old value (or nil) and assign the returned value to the key.
// The shard containing the key will be locked, it is NOT safe to call other cmap funcs inside `fn`.
func (m *Map) Update(key KT, fn func(oldval VT) (newval VT)) {
	shard := m.getShard(key)
	shard.Lock()
	defer shard.Unlock()
	shard.items[key] = fn(shard.items[key])
}
