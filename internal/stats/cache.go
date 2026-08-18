package stats

import "sync"

type AccountStats struct {
	CallCount        int64
	TotalDurationSec int64
}

type Cache struct {
	mu sync.RWMutex
	m  map[string]*AccountStats
}

func NewCache() *Cache {
	return &Cache{m: make(map[string]*AccountStats)}
}

func (c *Cache) Get(accountID string) AccountStats {
	c.mu.RLock()
	defer c.mu.RUnlock()

	s, ok := c.m[accountID]
	if !ok {
		return AccountStats{}
	}
	return *s
}

func (c *Cache) Record(accountID string, durationSec int) {
	s, ok := c.m[accountID]
	if !ok {
		s = &AccountStats{}
		c.m[accountID] = s
	}
	s.CallCount++
	s.TotalDurationSec += int64(durationSec)
}
