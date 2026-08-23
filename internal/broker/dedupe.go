package broker

import (
	"container/list"
	"fmt"
	"strings"
	"sync"
	"time"
)

// DefaultDedupeTTL is the window during which a duplicate enqueue is dropped.
const DefaultDedupeTTL = 5 * time.Minute

const DefaultDedupeCapacity = 100_000

type DedupePolicy string

const (
	DedupePolicyUnboundedTTL DedupePolicy = "unbounded_ttl"
	DedupePolicyBoundedLRU   DedupePolicy = "bounded_lru"
	DedupePolicyDurable      DedupePolicy = "durable"
)

type DedupeOptions struct {
	Policy   DedupePolicy
	Capacity int
}

type dedupeStore interface {
	SeenOrAdd(key string, ttl time.Duration) bool
	Delete(key string)
	Len() int
}

type queueDedupePurger interface {
	purgeQueue(queue string)
}

// ttlDedupeStore is the original unbounded, opportunistically-swept TTL map.
type ttlDedupeStore struct {
	mu   sync.Mutex
	seen map[string]time.Time
}

func newTTLDedupeStore() *ttlDedupeStore {
	return &ttlDedupeStore{seen: make(map[string]time.Time)}
}

func (d *ttlDedupeStore) SeenOrAdd(key string, ttl time.Duration) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	if exp, ok := d.seen[key]; ok && now.Before(exp) {
		return true
	}
	d.seen[key] = now.Add(ttl)
	if len(d.seen) > 1024 {
		for k, exp := range d.seen {
			if !now.Before(exp) {
				delete(d.seen, k)
			}
		}
	}
	return false
}

func (d *ttlDedupeStore) Delete(key string) {
	d.mu.Lock()
	delete(d.seen, key)
	d.mu.Unlock()
}

func (d *ttlDedupeStore) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.seen)
}

func (d *ttlDedupeStore) purgeQueue(queue string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	prefix := queue + ":"
	for key := range d.seen {
		if strings.HasPrefix(key, prefix) {
			delete(d.seen, key)
		}
	}
}

type dedupeEntry struct {
	key       string
	expiresAt time.Time
}

type lruDedupeStore struct {
	mu       sync.Mutex
	capacity int
	items    map[string]*list.Element
	list     *list.List
	onEvict  func()
}

func newLRUDedupeStore(capacity int, onEvict func()) *lruDedupeStore {
	if capacity <= 0 {
		capacity = DefaultDedupeCapacity
	}
	return &lruDedupeStore{
		capacity: capacity,
		items:    make(map[string]*list.Element),
		list:     list.New(),
		onEvict:  onEvict,
	}
}

func (d *lruDedupeStore) SeenOrAdd(key string, ttl time.Duration) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	now := time.Now()
	if element, ok := d.items[key]; ok {
		entry := element.Value.(*dedupeEntry)
		if now.Before(entry.expiresAt) {
			d.list.MoveToFront(element)
			return true
		}
		d.list.Remove(element)
		delete(d.items, key)
	}

	element := d.list.PushFront(&dedupeEntry{key: key, expiresAt: now.Add(ttl)})
	d.items[key] = element
	if len(d.items) > d.capacity {
		oldest := d.list.Back()
		entry := oldest.Value.(*dedupeEntry)
		delete(d.items, entry.key)
		d.list.Remove(oldest)
		if d.onEvict != nil {
			d.onEvict()
		}
	}
	return false
}

func (d *lruDedupeStore) Delete(key string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if element, ok := d.items[key]; ok {
		d.list.Remove(element)
		delete(d.items, key)
	}
}

func (d *lruDedupeStore) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return len(d.items)
}

func (d *lruDedupeStore) purgeQueue(queue string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	prefix := queue + ":"
	for key, element := range d.items {
		if strings.HasPrefix(key, prefix) {
			d.list.Remove(element)
			delete(d.items, key)
		}
	}
}

func newDedupeStore(opts DedupeOptions, onEvict func()) (dedupeStore, error) {
	if opts.Policy == "" {
		opts.Policy = DedupePolicyUnboundedTTL
	}
	switch opts.Policy {
	case DedupePolicyUnboundedTTL:
		return newTTLDedupeStore(), nil
	case DedupePolicyBoundedLRU:
		return newLRUDedupeStore(opts.Capacity, onEvict), nil
	default:
		return nil, fmt.Errorf("shoebox: unsupported dedupe policy %q", opts.Policy)
	}
}
