// pkg/vdbe/profiler.go
// Package vdbe provides query profiling capabilities for the VDBE.
package vdbe

import (
	"sync"
	"time"
)

// QueryPhase represents a phase of query execution.
type QueryPhase int

const (
	// PhaseParse is the SQL parsing phase.
	PhaseParse QueryPhase = iota
	// PhaseCompile is the bytecode compilation phase.
	PhaseCompile
	// PhaseExecute is the VM execution phase.
	PhaseExecute
	// PhaseFetch is the result fetching phase.
	PhaseFetch
)

// String returns the string representation of the query phase.
func (p QueryPhase) String() string {
	switch p {
	case PhaseParse:
		return "parse"
	case PhaseCompile:
		return "compile"
	case PhaseExecute:
		return "execute"
	case PhaseFetch:
		return "fetch"
	default:
		return "unknown"
	}
}

// PhaseStats holds timing statistics for a query phase.
type PhaseStats struct {
	Phase    QueryPhase    // The phase being tracked
	Duration time.Duration // Time spent in this phase
}

// MemoryStatsData holds memory allocation statistics.
type MemoryStatsData struct {
	TotalAllocated  int64 // Total bytes allocated
	TotalFreed      int64 // Total bytes freed
	CurrentUsage    int64 // Current memory usage (allocated - freed)
	PeakUsage       int64 // Peak memory usage
	AllocationCount int64 // Number of allocations
}

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
	phaseStats      map[QueryPhase]*phaseStatsAccumulator
	phaseStartTimes map[QueryPhase]time.Time
	memoryStats     memoryStatsAccumulator
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

// phaseStatsAccumulator accumulates stats for query phases
type phaseStatsAccumulator struct {
	duration time.Duration
}

// memoryStatsAccumulator accumulates memory allocation statistics
type memoryStatsAccumulator struct {
	totalAllocated  int64
	totalFreed      int64
	currentUsage    int64
	peakUsage       int64
	allocationCount int64
}

// NewProfiler creates a new Profiler instance.
func NewProfiler() *Profiler {
	return &Profiler{
		opcodeStats:     make(map[Opcode]*opcodeStatsAccumulator),
		phaseStats:      make(map[QueryPhase]*phaseStatsAccumulator),
		phaseStartTimes: make(map[QueryPhase]time.Time),
		enabled:         true,
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
	p.phaseStats = make(map[QueryPhase]*phaseStatsAccumulator)
	p.phaseStartTimes = make(map[QueryPhase]time.Time)
	p.memoryStats = memoryStatsAccumulator{}
	p.totalTime = 0
}

// StartPhase marks the beginning of a query phase.
func (p *Profiler) StartPhase(phase QueryPhase) {
	if !p.enabled {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	p.phaseStartTimes[phase] = time.Now()
}

// EndPhase marks the end of a query phase and records the duration.
func (p *Profiler) EndPhase(phase QueryPhase) {
	if !p.enabled {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	startTime, ok := p.phaseStartTimes[phase]
	if !ok {
		return
	}

	elapsed := time.Since(startTime)
	delete(p.phaseStartTimes, phase)

	stats, ok := p.phaseStats[phase]
	if !ok {
		stats = &phaseStatsAccumulator{}
		p.phaseStats[phase] = stats
	}
	stats.duration += elapsed
}

// PhaseStats returns a snapshot of the current phase statistics.
func (p *Profiler) PhaseStats() map[QueryPhase]PhaseStats {
	p.mu.Lock()
	defer p.mu.Unlock()

	result := make(map[QueryPhase]PhaseStats, len(p.phaseStats))
	for phase, acc := range p.phaseStats {
		result[phase] = PhaseStats{
			Phase:    phase,
			Duration: acc.duration,
		}
	}
	return result
}

// RecordAllocation records a memory allocation of the given size in bytes.
func (p *Profiler) RecordAllocation(bytes int64) {
	if !p.enabled {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	p.memoryStats.totalAllocated += bytes
	p.memoryStats.currentUsage += bytes
	p.memoryStats.allocationCount++

	if p.memoryStats.currentUsage > p.memoryStats.peakUsage {
		p.memoryStats.peakUsage = p.memoryStats.currentUsage
	}
}

// RecordDeallocation records a memory deallocation of the given size in bytes.
func (p *Profiler) RecordDeallocation(bytes int64) {
	if !p.enabled {
		return
	}
	p.mu.Lock()
	defer p.mu.Unlock()

	p.memoryStats.totalFreed += bytes
	p.memoryStats.currentUsage -= bytes
}

// MemoryStats returns a snapshot of the current memory statistics.
func (p *Profiler) MemoryStats() MemoryStatsData {
	p.mu.Lock()
	defer p.mu.Unlock()

	return MemoryStatsData{
		TotalAllocated:  p.memoryStats.totalAllocated,
		TotalFreed:      p.memoryStats.totalFreed,
		CurrentUsage:    p.memoryStats.currentUsage,
		PeakUsage:       p.memoryStats.peakUsage,
		AllocationCount: p.memoryStats.allocationCount,
	}
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
