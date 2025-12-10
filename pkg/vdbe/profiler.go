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

// ProfileReport contains a complete profiling report for a query execution.
type ProfileReport struct {
	TotalTime   time.Duration          // Total execution time
	OpcodeStats []OpcodeStats          // Opcode timing statistics (sorted by total time)
	PhaseStats  map[QueryPhase]PhaseStats // Phase timing statistics
	MemoryStats MemoryStatsData        // Memory allocation statistics
}

// Report generates a complete profiling report from the collected statistics.
func (p *Profiler) Report() ProfileReport {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Collect opcode stats
	opcodeStats := make([]OpcodeStats, 0, len(p.opcodeStats))
	for op, acc := range p.opcodeStats {
		var avgTime time.Duration
		if acc.count > 0 {
			avgTime = acc.totalTime / time.Duration(acc.count)
		}
		opcodeStats = append(opcodeStats, OpcodeStats{
			Opcode:    op,
			Count:     acc.count,
			TotalTime: acc.totalTime,
			MinTime:   acc.minTime,
			MaxTime:   acc.maxTime,
			AvgTime:   avgTime,
		})
	}

	// Sort opcode stats by total time (descending)
	sortOpcodeStatsByTime(opcodeStats)

	// Collect phase stats
	phaseStats := make(map[QueryPhase]PhaseStats, len(p.phaseStats))
	for phase, acc := range p.phaseStats {
		phaseStats[phase] = PhaseStats{
			Phase:    phase,
			Duration: acc.duration,
		}
	}

	// Collect memory stats
	memStats := MemoryStatsData{
		TotalAllocated:  p.memoryStats.totalAllocated,
		TotalFreed:      p.memoryStats.totalFreed,
		CurrentUsage:    p.memoryStats.currentUsage,
		PeakUsage:       p.memoryStats.peakUsage,
		AllocationCount: p.memoryStats.allocationCount,
	}

	return ProfileReport{
		TotalTime:   p.totalTime,
		OpcodeStats: opcodeStats,
		PhaseStats:  phaseStats,
		MemoryStats: memStats,
	}
}

// sortOpcodeStatsByTime sorts opcode stats by total time in descending order.
func sortOpcodeStatsByTime(stats []OpcodeStats) {
	// Simple insertion sort (efficient for small arrays)
	for i := 1; i < len(stats); i++ {
		key := stats[i]
		j := i - 1
		for j >= 0 && stats[j].TotalTime < key.TotalTime {
			stats[j+1] = stats[j]
			j--
		}
		stats[j+1] = key
	}
}

// String returns a human-readable string representation of the profile report.
func (r ProfileReport) String() string {
	var sb stringBuilder
	sb.WriteString("=== Query Profile Report ===\n\n")

	// Total time
	sb.WriteString(formatString("Total Execution Time: %v\n\n", r.TotalTime))

	// Phase timing
	sb.WriteString("--- Phase Timing ---\n")
	phases := []QueryPhase{PhaseParse, PhaseCompile, PhaseExecute, PhaseFetch}
	for _, phase := range phases {
		if stat, ok := r.PhaseStats[phase]; ok {
			sb.WriteString(formatString("  %-10s: %v\n", phase.String(), stat.Duration))
		}
	}
	sb.WriteString("\n")

	// Opcode statistics
	sb.WriteString("--- Opcode Statistics ---\n")
	sb.WriteString(formatString("  %-15s %10s %12s %12s %12s\n",
		"Opcode", "Count", "Total", "Avg", "Max"))
	for _, stat := range r.OpcodeStats {
		sb.WriteString(formatString("  %-15s %10d %12v %12v %12v\n",
			stat.Opcode.String(), stat.Count, stat.TotalTime, stat.AvgTime, stat.MaxTime))
	}
	sb.WriteString("\n")

	// Memory statistics
	sb.WriteString("--- Memory Statistics ---\n")
	sb.WriteString(formatString("  Total Allocated:  %d bytes\n", r.MemoryStats.TotalAllocated))
	sb.WriteString(formatString("  Total Freed:      %d bytes\n", r.MemoryStats.TotalFreed))
	sb.WriteString(formatString("  Current Usage:    %d bytes\n", r.MemoryStats.CurrentUsage))
	sb.WriteString(formatString("  Peak Usage:       %d bytes\n", r.MemoryStats.PeakUsage))
	sb.WriteString(formatString("  Allocation Count: %d\n", r.MemoryStats.AllocationCount))

	return sb.String()
}

// stringBuilder is a simple string builder for report generation
type stringBuilder struct {
	data []byte
}

func (sb *stringBuilder) WriteString(s string) {
	sb.data = append(sb.data, s...)
}

func (sb *stringBuilder) String() string {
	return string(sb.data)
}

// formatString formats a string with the given arguments
func formatString(format string, args ...interface{}) string {
	// Simple implementation using fmt.Sprintf
	return formatWithArgs(format, args...)
}

// formatWithArgs formats a string with arguments
func formatWithArgs(format string, args ...interface{}) string {
	result := make([]byte, 0, len(format)*2)
	argIdx := 0

	for i := 0; i < len(format); i++ {
		if format[i] == '%' && i+1 < len(format) {
			// Find the format specifier
			j := i + 1
			// Handle width/precision
			for j < len(format) && (format[j] == '-' || format[j] == '+' || format[j] == ' ' || format[j] == '#' || format[j] == '0' || (format[j] >= '0' && format[j] <= '9') || format[j] == '.') {
				j++
			}
			if j < len(format) {
				spec := format[j]
				if argIdx < len(args) {
					// Get width specification
					width := 0
					leftAlign := false
					k := i + 1
					if k < j && format[k] == '-' {
						leftAlign = true
						k++
					}
					for k < j && format[k] >= '0' && format[k] <= '9' {
						width = width*10 + int(format[k]-'0')
						k++
					}

					formatted := formatArg(args[argIdx], spec)
					if width > 0 {
						if leftAlign {
							formatted = padRight(formatted, width)
						} else {
							formatted = padLeft(formatted, width)
						}
					}
					result = append(result, formatted...)
					argIdx++
				}
				i = j
				continue
			}
		}
		result = append(result, format[i])
	}
	return string(result)
}

func formatArg(arg interface{}, spec byte) string {
	switch spec {
	case 'd':
		switch v := arg.(type) {
		case int:
			return intToString(int64(v))
		case int64:
			return intToString(v)
		case int32:
			return intToString(int64(v))
		}
	case 's':
		switch v := arg.(type) {
		case string:
			return v
		}
	case 'v':
		switch v := arg.(type) {
		case time.Duration:
			return v.String()
		case string:
			return v
		case int:
			return intToString(int64(v))
		case int64:
			return intToString(v)
		}
	}
	return ""
}

func intToString(n int64) string {
	if n == 0 {
		return "0"
	}
	negative := n < 0
	if negative {
		n = -n
	}
	digits := make([]byte, 0, 20)
	for n > 0 {
		digits = append(digits, byte('0'+n%10))
		n /= 10
	}
	// Reverse
	for i, j := 0, len(digits)-1; i < j; i, j = i+1, j-1 {
		digits[i], digits[j] = digits[j], digits[i]
	}
	if negative {
		return "-" + string(digits)
	}
	return string(digits)
}

func padLeft(s string, width int) string {
	if len(s) >= width {
		return s
	}
	padding := make([]byte, width-len(s))
	for i := range padding {
		padding[i] = ' '
	}
	return string(padding) + s
}

func padRight(s string, width int) string {
	if len(s) >= width {
		return s
	}
	padding := make([]byte, width-len(s))
	for i := range padding {
		padding[i] = ' '
	}
	return s + string(padding)
}
