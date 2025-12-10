// pkg/vdbe/profiler.go
// Package vdbe provides query profiling capabilities for the VDBE.
package vdbe

import (
	"sync"
	"time"
)

// OpcodeStats holds timing statistics for a single opcode type.
type OpcodeStats struct {
	Opcode    Opcode        // The opcode being tracked
	Count     int64         // Number of times this opcode was executed
	TotalTime time.Duration // Total time spent executing this opcode
	MinTime   time.Duration // Minimum execution time
	MaxTime   time.Duration // Maximum execution time
	AvgTime   time.Duration // Average execution time (TotalTime / Count)
}

// Profiler collects timing statistics for VDBE opcode execution.
type Profiler struct {
	mu              sync.Mutex
	opcodeStats     map[Opcode]*opcodeStatsAccumulator
	totalTime       time.Duration
	executionStart  time.Time
	enabled         bool
}

// opcodeStatsAccumulator accumulates stats during execution
type opcodeStatsAccumulator struct {
	count     int64
	totalTime time.Duration
	minTime   time.Duration
	maxTime   time.Duration
}

// NewProfiler creates a new Profiler instance.
func NewProfiler() *Profiler {
	return &Profiler{
		opcodeStats: make(map[Opcode]*opcodeStatsAccumulator),
		enabled:     true,
	}
}

// BeforeOpcode is called before executing an opcode.
// Returns the start time for timing measurement.
func (p *Profiler) BeforeOpcode(op Opcode) time.Time {
	if !p.enabled {
		return time.Time{}
	}
	return time.Now()
}

// AfterOpcode is called after executing an opcode.
// Records the elapsed time since startTime.
func (p *Profiler) AfterOpcode(op Opcode, startTime time.Time) {
	if !p.enabled || startTime.IsZero() {
		return
	}

	elapsed := time.Since(startTime)

	p.mu.Lock()
	defer p.mu.Unlock()

	stats, ok := p.opcodeStats[op]
	if !ok {
		stats = &opcodeStatsAccumulator{
			minTime: elapsed,
			maxTime: elapsed,
		}
		p.opcodeStats[op] = stats
	}

	stats.count++
	stats.totalTime += elapsed

	if elapsed < stats.minTime {
		stats.minTime = elapsed
	}
	if elapsed > stats.maxTime {
		stats.maxTime = elapsed
	}

	p.totalTime += elapsed
}

// OpcodeStats returns a snapshot of the current opcode statistics.
func (p *Profiler) OpcodeStats() []OpcodeStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	result := make([]OpcodeStats, 0, len(p.opcodeStats))
	for op, acc := range p.opcodeStats {
		var avgTime time.Duration
		if acc.count > 0 {
			avgTime = acc.totalTime / time.Duration(acc.count)
		}

		result = append(result, OpcodeStats{
			Opcode:    op,
			Count:     acc.count,
			TotalTime: acc.totalTime,
			MinTime:   acc.minTime,
			MaxTime:   acc.maxTime,
			AvgTime:   avgTime,
		})
	}
	return result
}

// TotalExecutionTime returns the total time spent executing opcodes.
func (p *Profiler) TotalExecutionTime() time.Duration {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.totalTime
}

// Reset clears all collected statistics.
func (p *Profiler) Reset() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.opcodeStats = make(map[Opcode]*opcodeStatsAccumulator)
	p.totalTime = 0
}

// SetEnabled enables or disables profiling.
func (p *Profiler) SetEnabled(enabled bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.enabled = enabled
}

// IsEnabled returns whether profiling is enabled.
func (p *Profiler) IsEnabled() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.enabled
}
