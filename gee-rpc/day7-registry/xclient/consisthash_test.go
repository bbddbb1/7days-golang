package xclient

import (
	"math/rand"
	"strconv"
	"testing"
)

func TestHashing(t *testing.T) {
	hash := NewConsistentHash(3, func(key []byte) uint32 {
		i, _ := strconv.Atoi(string(key))
		return uint32(i)
	})

	// Given the above hash function, this will give replicas with "hashes":
	// 2, 4, 6, 12, 14, 16, 22, 24, 26
	hash.Add("6", "4", "2")

	testCases := map[string]string{
		"2":  "2",
		"11": "2",
		"23": "4",
		"27": "2",
	}

	for k, v := range testCases {
		if hash.Get(k) != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}

	// Adds 8, 18, 28
	hash.Add("8")

	// 27 should now map to 8.
	testCases["27"] = "8"

	for k, v := range testCases {
		if hash.Get(k) != v {
			t.Errorf("Asking for %s, should have yielded %s", k, v)
		}
	}

}

func TestHashingBalance(t *testing.T) {
	hash := NewConsistentHash(160, nil)
	num_server := 5
	num_get := 100
	for i := 0; i < num_server; i++ {
		hash.Add(strconv.FormatInt(int64(i), 10))
	}
	hash_cnt := make(map[string]int)
	random_cnt := make(map[int]int)
	for i := 0; i < num_get; i++ {
		hash_cnt[hash.Get("")]++
		random_cnt[rand.Int()%num_server]++
	}
	hash_var := 0.0
	random_var := 0.0
	for _, v := range hash_cnt {
		hash_var += (float64(v) - float64(num_get/num_server)) * (float64(v) - float64(num_get/num_server))
	}
	for _, v := range random_cnt {
		random_var += (float64(v) - float64(num_get/num_server)) * (float64(v) - float64(num_get/num_server))
	}
	t.Log(hash_var / float64(num_server))
	t.Log(random_var / float64(num_server))
}
