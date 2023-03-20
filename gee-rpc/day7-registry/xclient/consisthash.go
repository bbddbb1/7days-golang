package xclient

import (
	"hash/crc32"
	"math/rand"
	"sort"
	"strconv"
)

type Hash func(data []byte) uint32

type ConsistentHash struct {
	hash     Hash
	replicas int
	keys     []int
	hashMap  map[int]string
}

func NewConsistentHash(replicas int, hash Hash) *ConsistentHash {
	ch := &ConsistentHash{
		hash:     hash,
		replicas: replicas,
		hashMap:  make(map[int]string),
	}
	if ch.hash == nil {
		ch.hash = crc32.ChecksumIEEE
	}
	return ch
}

func (c *ConsistentHash) Add(keys ...string) {
	for _, key := range keys {
		for i := 0; i < c.replicas; i++ {
			hash := int(c.hash([]byte(strconv.Itoa(i) + key)))
			c.keys = append(c.keys, hash)
			c.hashMap[hash] = key
		}
	}
	sort.Ints(c.keys)
}

func (c *ConsistentHash) Get(key string) string {
	if len(key) == 0 {
		key = randStringBytes(rand.Int() % 10000)
	}
	hash := int(c.hash([]byte(key)))
	idx := sort.Search(len(c.keys), func(i int) bool {
		return c.keys[i] >= hash
	})
	return c.hashMap[c.keys[idx%len(c.keys)]]
}

func (c *ConsistentHash) delete(key string) {
	for i := 0; i < c.replicas; i++ {
		hash := int(c.hash([]byte(strconv.Itoa(i) + key)))
		idx := sort.SearchInts(c.keys, hash)
		c.keys = append(c.keys[:idx], c.keys[idx+1:]...)
		delete(c.hashMap, hash)
	}
}

const letterBytes = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"

func randStringBytes(n int) string {
	b := make([]byte, n)
	for i := range b {
		b[i] = letterBytes[rand.Intn(len(letterBytes))]
	}
	return string(b)
}
