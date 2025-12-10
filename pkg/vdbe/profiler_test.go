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
