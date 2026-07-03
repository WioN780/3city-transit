package main

import (
	"sync"
	"time"
)

// ttlCache holds pre-marshalled JSON per query key. Gold tables refresh
// hourly, so a 60s TTL is purely to absorb bursts of identical dashboard
// requests, not to track freshness -- a plain mutex+map is enough at this
// request volume.
type ttlCache struct {
	mu      sync.Mutex
	ttl     time.Duration
	entries map[string]cacheEntry
}

type cacheEntry struct {
	body      []byte
	expiresAt time.Time
}

func newTTLCache(ttl time.Duration) *ttlCache {
	return &ttlCache{ttl: ttl, entries: map[string]cacheEntry{}}
}

func (c *ttlCache) get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok || time.Now().After(e.expiresAt) {
		return nil, false
	}
	return e.body, true
}

func (c *ttlCache) set(key string, body []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[key] = cacheEntry{body: body, expiresAt: time.Now().Add(c.ttl)}
}
