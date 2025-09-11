// Package collector provides memory management and monitoring for OHLCV data collection.
package collector

import (
	"log/slog"
	"runtime"
	"sync"
	"time"
)

// memoryManager handles memory usage monitoring and management
type memoryManager struct {
	limitMB      int64
	logger       *slog.Logger
	mu           sync.RWMutex
	lastGCTime   time.Time
	gcCount      uint32
	lastUsageMB  int64
}

// MemoryStats provides detailed memory statistics
type MemoryStats struct {
	AllocMB      int64
	TotalAllocMB int64
	SysMB        int64
	NumGC        uint32
	GCPauseNs    uint64
	HeapObjects  uint64
}

// newMemoryManager creates a new memory manager
func newMemoryManager(limitMB int) *memoryManager {
	return &memoryManager{
		limitMB:    int64(limitMB),
		logger:     slog.Default(),
		lastGCTime: time.Now(),
	}
}

// GetUsageMB returns current memory usage in MB
func (mm *memoryManager) GetUsageMB() int64 {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	return int64(memStats.Alloc / 1024 / 1024)
}

// GetDetailedStats returns detailed memory statistics
func (mm *memoryManager) GetDetailedStats() *MemoryStats {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	return &MemoryStats{
		AllocMB:      int64(memStats.Alloc / 1024 / 1024),
		TotalAllocMB: int64(memStats.TotalAlloc / 1024 / 1024),
		SysMB:        int64(memStats.Sys / 1024 / 1024),
		NumGC:        memStats.NumGC,
		GCPauseNs:    memStats.PauseNs[(memStats.NumGC+255)%256],
		HeapObjects:  memStats.HeapObjects,
	}
}

// IsOverLimit checks if memory usage exceeds the configured limit
func (mm *memoryManager) IsOverLimit() bool {
	usageMB := mm.GetUsageMB()
	
	mm.mu.Lock()
	mm.lastUsageMB = usageMB
	mm.mu.Unlock()
	
	return usageMB > mm.limitMB
}

// IsNearLimit checks if memory usage is approaching the limit (80% threshold)
func (mm *memoryManager) IsNearLimit() bool {
	usageMB := mm.GetUsageMB()
	threshold := int64(float64(mm.limitMB) * 0.8)
	return usageMB > threshold
}

// CheckAndReport checks memory usage and logs warnings if necessary
func (mm *memoryManager) CheckAndReport() {
	stats := mm.GetDetailedStats()
	
	mm.mu.Lock()
	defer mm.mu.Unlock()
	
	mm.lastUsageMB = stats.AllocMB
	
	// Log memory statistics periodically
	mm.logger.Debug("Memory usage check",
		"alloc_mb", stats.AllocMB,
		"total_alloc_mb", stats.TotalAllocMB,
		"sys_mb", stats.SysMB,
		"heap_objects", stats.HeapObjects,
		"num_gc", stats.NumGC,
		"limit_mb", mm.limitMB,
	)
	
	// Warn if approaching limit
	if mm.IsNearLimit() {
		mm.logger.Warn("Memory usage approaching limit",
			"current_mb", stats.AllocMB,
			"limit_mb", mm.limitMB,
			"usage_percent", float64(stats.AllocMB)/float64(mm.limitMB)*100,
		)
	}
	
	// Error if over limit
	if mm.IsOverLimit() {
		mm.logger.Error("Memory usage exceeds limit",
			"current_mb", stats.AllocMB,
			"limit_mb", mm.limitMB,
			"overage_mb", stats.AllocMB-mm.limitMB,
		)
	}
	
	// Track GC statistics
	if stats.NumGC > mm.gcCount {
		timeSinceLastGC := time.Since(mm.lastGCTime)
		mm.gcCount = stats.NumGC
		mm.lastGCTime = time.Now()
		
		mm.logger.Debug("Garbage collection occurred",
			"gc_count", stats.NumGC,
			"time_since_last", timeSinceLastGC,
			"pause_ns", stats.GCPauseNs,
			"heap_objects", stats.HeapObjects,
		)
	}
}

// ForceGC forces a garbage collection cycle
func (mm *memoryManager) ForceGC() {
	before := mm.GetUsageMB()
	start := time.Now()
	
	runtime.GC()
	
	after := mm.GetUsageMB()
	duration := time.Since(start)
	freed := before - after
	
	mm.logger.Info("Forced garbage collection",
		"before_mb", before,
		"after_mb", after,
		"freed_mb", freed,
		"duration", duration,
	)
}

// OptimizeMemory performs memory optimization operations
func (mm *memoryManager) OptimizeMemory() {
	mm.logger.Info("Starting memory optimization")
	
	// Get initial stats
	initialStats := mm.GetDetailedStats()
	start := time.Now()
	
	// Force garbage collection
	runtime.GC()
	
	// Return unused memory to OS
	runtime.GC()
	
	// Get final stats
	finalStats := mm.GetDetailedStats()
	duration := time.Since(start)
	
	freed := initialStats.AllocMB - finalStats.AllocMB
	
	mm.logger.Info("Memory optimization completed",
		"initial_mb", initialStats.AllocMB,
		"final_mb", finalStats.AllocMB,
		"freed_mb", freed,
		"duration", duration,
		"heap_objects_before", initialStats.HeapObjects,
		"heap_objects_after", finalStats.HeapObjects,
	)
}

// GetMemoryReport generates a comprehensive memory usage report
func (mm *memoryManager) GetMemoryReport() map[string]interface{} {
	stats := mm.GetDetailedStats()
	
	mm.mu.RLock()
	lastUsageMB := mm.lastUsageMB
	mm.mu.RUnlock()
	
	usagePercent := float64(stats.AllocMB) / float64(mm.limitMB) * 100
	
	return map[string]interface{}{
		"current_usage_mb":    stats.AllocMB,
		"limit_mb":            mm.limitMB,
		"usage_percent":       usagePercent,
		"is_over_limit":       mm.IsOverLimit(),
		"is_near_limit":       mm.IsNearLimit(),
		"total_allocated_mb":  stats.TotalAllocMB,
		"system_memory_mb":    stats.SysMB,
		"heap_objects":        stats.HeapObjects,
		"gc_count":            stats.NumGC,
		"last_gc_pause_ns":    stats.GCPauseNs,
		"last_usage_mb":       lastUsageMB,
		"memory_efficiency":   float64(stats.AllocMB) / float64(stats.SysMB) * 100,
	}
}

// SetLimit updates the memory limit
func (mm *memoryManager) SetLimit(limitMB int) {
	mm.mu.Lock()
	defer mm.mu.Unlock()
	
	oldLimit := mm.limitMB
	mm.limitMB = int64(limitMB)
	
	mm.logger.Info("Memory limit updated",
		"old_limit_mb", oldLimit,
		"new_limit_mb", mm.limitMB,
	)
}

// GetLimit returns the current memory limit
func (mm *memoryManager) GetLimit() int64 {
	mm.mu.RLock()
	defer mm.mu.RUnlock()
	return mm.limitMB
}