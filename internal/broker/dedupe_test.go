package broker

import (
	"sync"
	"testing"
	"time"
)

func TestTTLDedupeStoreSharedBehavior(t *testing.T) {
	d := newTTLDedupeStore()
	if d.SeenOrAdd("jobs:key", time.Hour) {
		t.Fatal("first key was reported as a duplicate")
	}
	if !d.SeenOrAdd("jobs:key", time.Hour) {
		t.Fatal("duplicate key was accepted")
	}
	if d.SeenOrAdd("other:key", time.Hour) {
		t.Fatal("same key in another queue was rejected")
	}
	if d.SeenOrAdd("jobs:expired", time.Nanosecond) {
		t.Fatal("expired key was reported as a duplicate")
	}
	time.Sleep(time.Millisecond)
	if d.SeenOrAdd("jobs:expired", time.Hour) {
		t.Fatal("expired key was not accepted again")
	}
}

func TestLRUDedupeStoreEvictionAndRecency(t *testing.T) {
	d := newLRUDedupeStore(2, nil)
	for _, key := range []string{"A", "B"} {
		if d.SeenOrAdd(key, time.Hour) {
			t.Fatalf("first %s was duplicate", key)
		}
	}
	if !d.SeenOrAdd("A", time.Hour) {
		t.Fatal("A should be a duplicate")
	}
	if d.SeenOrAdd("C", time.Hour) {
		t.Fatal("C should be new")
	}
	if d.SeenOrAdd("A", time.Hour) != true || d.SeenOrAdd("C", time.Hour) != true {
		t.Fatal("A and C should remain in the cache")
	}
	if d.SeenOrAdd("B", time.Hour) {
		t.Fatal("least recently used B was not evicted")
	}
	if got := d.Len(); got != 2 {
		t.Fatalf("cache length = %d, want 2", got)
	}
}

func TestLRUDedupeStoreExpiryDoesNotRefreshTTL(t *testing.T) {
	d := newLRUDedupeStore(2, nil)
	d.SeenOrAdd("A", 20*time.Millisecond)
	time.Sleep(10 * time.Millisecond)
	if !d.SeenOrAdd("A", 20*time.Millisecond) {
		t.Fatal("A should be a duplicate before its original TTL")
	}
	time.Sleep(15 * time.Millisecond)
	if d.SeenOrAdd("A", time.Hour) {
		t.Fatal("duplicate access incorrectly extended A's TTL")
	}
	if d.Len() != 1 {
		t.Fatal("expired A was not replaced as one entry")
	}
}

func TestLRUDedupeStoreCapacityNormalization(t *testing.T) {
	d := newLRUDedupeStore(0, nil)
	if d.capacity != DefaultDedupeCapacity {
		t.Fatalf("capacity = %d, want %d", d.capacity, DefaultDedupeCapacity)
	}
}

func TestDedupeStoreConcurrent(t *testing.T) {
	d := newLRUDedupeStore(1000, nil)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				key := "jobs:repeated"
				if j%2 == 0 {
					key = "jobs:unique-" + string(rune(i*100+j))
				}
				d.SeenOrAdd(key, time.Minute)
				d.Len()
			}
		}(i)
	}
	wg.Wait()
	if d.Len() > 1000 {
		t.Fatalf("cache length %d exceeded capacity", d.Len())
	}
}

func TestNewDedupeStoreRejectsUnsupportedPolicy(t *testing.T) {
	if _, err := newDedupeStore(DedupeOptions{Policy: "nope"}, nil); err == nil {
		t.Fatal("unsupported policy did not return an error")
	}
}
