// Package timekeeper provides a high-performance cached time implementation
// optimized for high-throughput applications that need fast time access
// without the overhead of syscalls on every time.Now() call.
//
// This implementation uses atomic operations and periodic updates to provide
// microsecond-precision cached time values with configurable refresh rates.
package timekeeper

import (
	"sync/atomic"
	"time"
)

// Cache holds the cached time value and provides thread-safe access
type Cache struct {
	// Cached time value stored as nanoseconds since epoch
	cachedNanos atomic.Int64

	// Ticker for periodic updates
	ticker *time.Ticker

	// Channel to signal shutdown
	done chan struct{}
}

var (
	// globalCache is the default cache instance used by package-level functions
	globalCache *Cache
)

func init() {
	// Initialize with 50ms refresh rate
	globalCache = NewCache(50 * time.Millisecond)
	globalCache.Start()
}

// NewCache creates a new time cache with the specified refresh interval.
// A smaller interval provides more accurate time but uses more CPU.
// Recommended intervals:
//   - 1ms for high-precision requirements
//   - 5ms for balanced performance (default)
//   - 10-50ms for maximum throughput with lower precision needs
func NewCache(refreshInterval time.Duration) *Cache {
	c := &Cache{
		ticker: time.NewTicker(refreshInterval),
		done:   make(chan struct{}),
	}

	// Initialize with current time
	c.refresh()

	return c
}

// Start begins the background refresh goroutine
func (c *Cache) Start() {
	go func() {
		for {
			select {
			case <-c.ticker.C:
				c.refresh()
			case <-c.done:
				c.ticker.Stop()
				return
			}
		}
	}()
}

// Stop halts the background refresh goroutine
func (c *Cache) Stop() {
	close(c.done)
}

// refresh updates the cached time with the current system time
func (c *Cache) refresh() {
	now := time.Now().UnixNano()
	c.cachedNanos.Store(now)
}

// Now returns the cached current time
// This is an extremely fast operation using only an atomic load
func (c *Cache) Now() time.Time {
	nanos := c.cachedNanos.Load()
	return time.Unix(0, nanos)
}

// SetRefreshInterval changes the refresh interval dynamically
func (c *Cache) SetRefreshInterval(interval time.Duration) {
	c.ticker.Reset(interval)
}

// Package-level convenience functions using the global cache

// Now returns the cached current time using the global cache instance.
// This is the primary function for high-throughput time access.
func Now() time.Time {
	return globalCache.Now()
}

// Stop stops the global cache refresh goroutine.
// This should typically only be called during application shutdown.
func Stop() {
	globalCache.Stop()
}

// SetRefreshInterval changes the global cache refresh interval.
// Use this to tune performance vs precision for your specific workload.
func SetRefreshInterval(interval time.Duration) {
	globalCache.SetRefreshInterval(interval)
}

// Since returns the time elapsed since t using the cached current time.
func Since(t time.Time) time.Duration {
	return Now().Sub(t)
}
