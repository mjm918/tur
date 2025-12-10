// pkg/vdbe/profiler_test.go
package vdbe

import (
	"testing"
	"time"
)

func TestProfilerRecordsOpcodeExecutionTime(t *testing.T) {
	// Create a simple program: Init -> Integer -> Halt
	prog := NewProgram()
	prog.AddOp(OpInit, 0, 1, 0)   // 0: Jump to 1
	prog.AddOp(OpInteger, 42, 0, 0) // 1: r[0] = 42
	prog.AddOp(OpHalt, 0, 0, 0)    // 2: Halt

	vm := NewVM(prog, nil)

	// Enable profiling
	profiler := NewProfiler()
	vm.SetProfiler(profiler)

	err := vm.Run()
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Verify profiler collected timing data
	stats := profiler.OpcodeStats()

	// Should have stats for Init, Integer, and Halt
	if len(stats) != 3 {
		t.Errorf("Expected 3 opcode stats, got %d", len(stats))
	}

	// Each opcode should have been executed once
	for _, stat := range stats {
		if stat.Count != 1 {
			t.Errorf("Expected count 1 for %s, got %d", stat.Opcode, stat.Count)
		}
		// Total time should be non-negative
		if stat.TotalTime < 0 {
			t.Errorf("Expected non-negative time for %s, got %v", stat.Opcode, stat.TotalTime)
		}
	}
}

func TestProfilerTracksOpcodeCallCount(t *testing.T) {
	// Create a program with a loop that executes Integer multiple times
	prog := NewProgram()
	prog.AddOp(OpInit, 0, 1, 0)     // 0: Jump to 1
	prog.AddOp(OpInteger, 0, 0, 0)  // 1: r[0] = 0 (counter)
	prog.AddOp(OpInteger, 5, 1, 0)  // 2: r[1] = 5 (limit)
	// Loop start
	prog.AddOp(OpGe, 0, 7, 1)       // 3: if r[0] >= r[1] goto 7
	prog.AddOp(OpInteger, 1, 2, 0)  // 4: r[2] = 1
	prog.AddOp(OpAdd, 0, 2, 0)      // 5: r[0] = r[0] + r[2]
	prog.AddOp(OpGoto, 0, 3, 0)     // 6: goto 3
	prog.AddOp(OpHalt, 0, 0, 0)     // 7: Halt

	vm := NewVM(prog, nil)
	profiler := NewProfiler()
	vm.SetProfiler(profiler)

	err := vm.Run()
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	stats := profiler.OpcodeStats()

	// Find Integer opcode stats
	var integerStats *OpcodeStats
	for i := range stats {
		if stats[i].Opcode == OpInteger {
			integerStats = &stats[i]
			break
		}
	}

	if integerStats == nil {
		t.Fatal("No stats found for Integer opcode")
	}

	// Integer is executed: 1 (init counter) + 1 (limit) + 5 (loop iterations) = 7 times
	if integerStats.Count != 7 {
		t.Errorf("Expected Integer count 7, got %d", integerStats.Count)
	}
}

func TestProfilerDisabledByDefault(t *testing.T) {
	prog := NewProgram()
	prog.AddOp(OpInit, 0, 1, 0)
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)

	// Don't set profiler - should work without profiling
	err := vm.Run()
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Verify r[0] is default (null)
	val := vm.Register(0)
	if !val.IsNull() {
		t.Errorf("Expected null, got %v", val)
	}
}

func TestProfilerAverageTime(t *testing.T) {
	prog := NewProgram()
	prog.AddOp(OpInit, 0, 1, 0)
	prog.AddOp(OpInteger, 1, 0, 0)
	prog.AddOp(OpInteger, 2, 1, 0)
	prog.AddOp(OpInteger, 3, 2, 0)
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	profiler := NewProfiler()
	vm.SetProfiler(profiler)

	err := vm.Run()
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	stats := profiler.OpcodeStats()

	// Find Integer opcode stats
	var integerStats *OpcodeStats
	for i := range stats {
		if stats[i].Opcode == OpInteger {
			integerStats = &stats[i]
			break
		}
	}

	if integerStats == nil {
		t.Fatal("No stats found for Integer opcode")
	}

	// Average should be TotalTime / Count
	expectedAvg := integerStats.TotalTime / time.Duration(integerStats.Count)
	if integerStats.AvgTime != expectedAvg {
		t.Errorf("Expected avg time %v, got %v", expectedAvg, integerStats.AvgTime)
	}
}

func TestProfilerMinMaxTime(t *testing.T) {
	prog := NewProgram()
	prog.AddOp(OpInit, 0, 1, 0)
	prog.AddOp(OpInteger, 1, 0, 0)
	prog.AddOp(OpInteger, 2, 1, 0)
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	profiler := NewProfiler()
	vm.SetProfiler(profiler)

	err := vm.Run()
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	stats := profiler.OpcodeStats()

	for _, stat := range stats {
		// MinTime should be <= AvgTime <= MaxTime
		if stat.MinTime > stat.AvgTime {
			t.Errorf("%s: MinTime %v > AvgTime %v", stat.Opcode, stat.MinTime, stat.AvgTime)
		}
		if stat.AvgTime > stat.MaxTime {
			t.Errorf("%s: AvgTime %v > MaxTime %v", stat.Opcode, stat.AvgTime, stat.MaxTime)
		}
	}
}

func TestProfilerReset(t *testing.T) {
	prog := NewProgram()
	prog.AddOp(OpInit, 0, 1, 0)
	prog.AddOp(OpInteger, 42, 0, 0)
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	profiler := NewProfiler()
	vm.SetProfiler(profiler)

	// Run once
	err := vm.Run()
	if err != nil {
		t.Fatalf("First run failed: %v", err)
	}

	stats := profiler.OpcodeStats()
	if len(stats) == 0 {
		t.Fatal("Expected stats after first run")
	}

	// Reset profiler
	profiler.Reset()

	stats = profiler.OpcodeStats()
	if len(stats) != 0 {
		t.Errorf("Expected empty stats after reset, got %d", len(stats))
	}
}

func TestProfilerTotalExecutionTime(t *testing.T) {
	prog := NewProgram()
	prog.AddOp(OpInit, 0, 1, 0)
	prog.AddOp(OpInteger, 42, 0, 0)
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	profiler := NewProfiler()
	vm.SetProfiler(profiler)

	startTime := time.Now()
	err := vm.Run()
	elapsed := time.Since(startTime)

	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	totalTime := profiler.TotalExecutionTime()

	// Total execution time should be positive and not exceed wall clock time
	if totalTime <= 0 {
		t.Errorf("Expected positive total execution time, got %v", totalTime)
	}
	if totalTime > elapsed+time.Millisecond {
		t.Errorf("Total execution time %v exceeds wall clock time %v", totalTime, elapsed)
	}
}

func TestProfilerWithResultRow(t *testing.T) {
	prog := NewProgram()
	prog.AddOp(OpInit, 0, 1, 0)
	prog.AddOp(OpInteger, 42, 0, 0)
	prog.AddOp(OpResultRow, 0, 1, 0)
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	profiler := NewProfiler()
	vm.SetProfiler(profiler)

	err := vm.Run()
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	// Verify result was still produced
	results := vm.Results()
	if len(results) != 1 {
		t.Fatalf("Expected 1 result row, got %d", len(results))
	}

	if results[0][0].Int() != 42 {
		t.Errorf("Expected 42, got %v", results[0][0])
	}

	// Verify profiler captured ResultRow
	stats := profiler.OpcodeStats()
	foundResultRow := false
	for _, stat := range stats {
		if stat.Opcode == OpResultRow {
			foundResultRow = true
			if stat.Count != 1 {
				t.Errorf("Expected ResultRow count 1, got %d", stat.Count)
			}
			break
		}
	}
	if !foundResultRow {
		t.Error("ResultRow opcode not found in profiler stats")
	}
}

// Tests for query phase tracking

func TestProfilerQueryPhases(t *testing.T) {
	profiler := NewProfiler()

	// Simulate parsing phase
	profiler.StartPhase(PhaseParse)
	time.Sleep(1 * time.Millisecond)
	profiler.EndPhase(PhaseParse)

	// Simulate compilation phase
	profiler.StartPhase(PhaseCompile)
	time.Sleep(1 * time.Millisecond)
	profiler.EndPhase(PhaseCompile)

	// Simulate execution phase
	profiler.StartPhase(PhaseExecute)
	time.Sleep(1 * time.Millisecond)
	profiler.EndPhase(PhaseExecute)

	phases := profiler.PhaseStats()

	// Should have 3 phases
	if len(phases) != 3 {
		t.Errorf("Expected 3 phase stats, got %d", len(phases))
	}

	// Check each phase was recorded
	for _, phase := range []QueryPhase{PhaseParse, PhaseCompile, PhaseExecute} {
		stat, ok := phases[phase]
		if !ok {
			t.Errorf("Phase %s not found in stats", phase)
			continue
		}
		if stat.Duration <= 0 {
			t.Errorf("Phase %s has non-positive duration: %v", phase, stat.Duration)
		}
	}
}

func TestProfilerQueryPhaseNesting(t *testing.T) {
	profiler := NewProfiler()

	// Start outer phase (execute)
	profiler.StartPhase(PhaseExecute)

	// Start inner phase (fetch)
	profiler.StartPhase(PhaseFetch)
	time.Sleep(1 * time.Millisecond)
	profiler.EndPhase(PhaseFetch)

	// End outer phase
	profiler.EndPhase(PhaseExecute)

	phases := profiler.PhaseStats()

	// Both phases should be recorded
	execStat, ok := phases[PhaseExecute]
	if !ok {
		t.Fatal("Execute phase not found")
	}

	fetchStat, ok := phases[PhaseFetch]
	if !ok {
		t.Fatal("Fetch phase not found")
	}

	// Execute phase should be >= fetch phase (since fetch is nested)
	if execStat.Duration < fetchStat.Duration {
		t.Errorf("Execute phase (%v) should be >= fetch phase (%v)",
			execStat.Duration, fetchStat.Duration)
	}
}

func TestProfilerPhaseReset(t *testing.T) {
	profiler := NewProfiler()

	profiler.StartPhase(PhaseParse)
	profiler.EndPhase(PhaseParse)

	phases := profiler.PhaseStats()
	if len(phases) != 1 {
		t.Errorf("Expected 1 phase, got %d", len(phases))
	}

	profiler.Reset()

	phases = profiler.PhaseStats()
	if len(phases) != 0 {
		t.Errorf("Expected 0 phases after reset, got %d", len(phases))
	}
}

func TestProfilerPhaseString(t *testing.T) {
	tests := []struct {
		phase    QueryPhase
		expected string
	}{
		{PhaseParse, "parse"},
		{PhaseCompile, "compile"},
		{PhaseExecute, "execute"},
		{PhaseFetch, "fetch"},
		{QueryPhase(99), "unknown"},
	}

	for _, test := range tests {
		if test.phase.String() != test.expected {
			t.Errorf("Phase %d: expected %q, got %q",
				test.phase, test.expected, test.phase.String())
		}
	}
}

// Tests for memory allocation statistics

func TestProfilerMemoryStats(t *testing.T) {
	profiler := NewProfiler()

	// Simulate memory tracking during query execution
	profiler.RecordAllocation(1024)      // 1KB allocation
	profiler.RecordAllocation(2048)      // 2KB allocation
	profiler.RecordDeallocation(1024)    // Free 1KB

	stats := profiler.MemoryStats()

	// Total allocated should be 3KB
	if stats.TotalAllocated != 3072 {
		t.Errorf("Expected TotalAllocated 3072, got %d", stats.TotalAllocated)
	}

	// Total freed should be 1KB
	if stats.TotalFreed != 1024 {
		t.Errorf("Expected TotalFreed 1024, got %d", stats.TotalFreed)
	}

	// Current usage should be 2KB
	if stats.CurrentUsage != 2048 {
		t.Errorf("Expected CurrentUsage 2048, got %d", stats.CurrentUsage)
	}

	// Allocation count should be 2
	if stats.AllocationCount != 2 {
		t.Errorf("Expected AllocationCount 2, got %d", stats.AllocationCount)
	}
}

func TestProfilerPeakMemory(t *testing.T) {
	profiler := NewProfiler()

	// Allocate 4KB
	profiler.RecordAllocation(4096)

	// Allocate another 2KB (peak should be 6KB)
	profiler.RecordAllocation(2048)

	// Free 4KB (current is 2KB, but peak remains 6KB)
	profiler.RecordDeallocation(4096)

	stats := profiler.MemoryStats()

	// Peak should be 6KB
	if stats.PeakUsage != 6144 {
		t.Errorf("Expected PeakUsage 6144, got %d", stats.PeakUsage)
	}

	// Current should be 2KB
	if stats.CurrentUsage != 2048 {
		t.Errorf("Expected CurrentUsage 2048, got %d", stats.CurrentUsage)
	}
}

func TestProfilerMemoryReset(t *testing.T) {
	profiler := NewProfiler()

	profiler.RecordAllocation(1024)
	profiler.RecordAllocation(2048)

	stats := profiler.MemoryStats()
	if stats.TotalAllocated == 0 {
		t.Error("Expected non-zero allocations before reset")
	}

	profiler.Reset()

	stats = profiler.MemoryStats()
	if stats.TotalAllocated != 0 {
		t.Errorf("Expected 0 allocations after reset, got %d", stats.TotalAllocated)
	}
	if stats.CurrentUsage != 0 {
		t.Errorf("Expected 0 current usage after reset, got %d", stats.CurrentUsage)
	}
	if stats.PeakUsage != 0 {
		t.Errorf("Expected 0 peak usage after reset, got %d", stats.PeakUsage)
	}
}

func TestProfilerMemoryDisabled(t *testing.T) {
	profiler := NewProfiler()
	profiler.SetEnabled(false)

	profiler.RecordAllocation(1024)

	stats := profiler.MemoryStats()
	if stats.TotalAllocated != 0 {
		t.Errorf("Expected 0 allocations when disabled, got %d", stats.TotalAllocated)
	}
}

// Tests for profiling report generation

func TestProfilerReport(t *testing.T) {
	// Create a program and run it with profiling
	prog := NewProgram()
	prog.AddOp(OpInit, 0, 1, 0)
	prog.AddOp(OpInteger, 42, 0, 0)
	prog.AddOp(OpInteger, 100, 1, 0)
	prog.AddOp(OpAdd, 0, 1, 2)
	prog.AddOp(OpResultRow, 2, 1, 0)
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	profiler := NewProfiler()
	vm.SetProfiler(profiler)

	// Simulate some phase timing
	profiler.StartPhase(PhaseParse)
	profiler.EndPhase(PhaseParse)
	profiler.StartPhase(PhaseCompile)
	profiler.EndPhase(PhaseCompile)
	profiler.StartPhase(PhaseExecute)

	err := vm.Run()
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	profiler.EndPhase(PhaseExecute)

	// Record some memory allocations
	profiler.RecordAllocation(1024)
	profiler.RecordAllocation(2048)

	// Generate report
	report := profiler.Report()

	// Report should contain opcode stats
	if len(report.OpcodeStats) == 0 {
		t.Error("Expected opcode stats in report")
	}

	// Report should contain phase stats
	if len(report.PhaseStats) == 0 {
		t.Error("Expected phase stats in report")
	}

	// Report should contain memory stats
	if report.MemoryStats.TotalAllocated != 3072 {
		t.Errorf("Expected TotalAllocated 3072, got %d", report.MemoryStats.TotalAllocated)
	}

	// Total execution time should be positive
	if report.TotalTime <= 0 {
		t.Errorf("Expected positive total time, got %v", report.TotalTime)
	}
}

func TestProfilerReportString(t *testing.T) {
	profiler := NewProfiler()

	// Add some data
	profiler.StartPhase(PhaseParse)
	profiler.EndPhase(PhaseParse)
	profiler.RecordAllocation(1024)

	// Generate report
	report := profiler.Report()

	// Get string representation
	str := report.String()

	// String should contain key sections
	if len(str) == 0 {
		t.Error("Report string should not be empty")
	}

	// Should contain "Query Profile Report"
	if !containsSubstring(str, "Query Profile Report") {
		t.Error("Report should contain 'Query Profile Report' header")
	}

	// Should contain memory stats section
	if !containsSubstring(str, "Memory Statistics") {
		t.Error("Report should contain 'Memory Statistics' section")
	}

	// Should contain phase stats section
	if !containsSubstring(str, "Phase Timing") {
		t.Error("Report should contain 'Phase Timing' section")
	}
}

func TestProfilerReportOpcodesSortedByTime(t *testing.T) {
	// Create program with multiple opcode types
	prog := NewProgram()
	prog.AddOp(OpInit, 0, 1, 0)
	prog.AddOp(OpInteger, 1, 0, 0)
	prog.AddOp(OpInteger, 2, 1, 0)
	prog.AddOp(OpInteger, 3, 2, 0)
	prog.AddOp(OpAdd, 0, 1, 3)
	prog.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(prog, nil)
	profiler := NewProfiler()
	vm.SetProfiler(profiler)

	err := vm.Run()
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}

	report := profiler.Report()

	// Opcodes should be sorted by total time (descending)
	for i := 1; i < len(report.OpcodeStats); i++ {
		if report.OpcodeStats[i].TotalTime > report.OpcodeStats[i-1].TotalTime {
			t.Errorf("Opcode stats not sorted by time: %v > %v",
				report.OpcodeStats[i].TotalTime, report.OpcodeStats[i-1].TotalTime)
		}
	}
}

// Helper function to check if a string contains a substring
func containsSubstring(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstringHelper(s, substr))
}

func containsSubstringHelper(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
