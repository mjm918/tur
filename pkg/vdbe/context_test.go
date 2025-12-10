// pkg/vdbe/context_test.go
package vdbe

import (
	"context"
	"testing"
	"time"
)

func TestVM_RunContext_BasicExecution(t *testing.T) {
	// Create a simple program that halts immediately
	program := NewProgram()
	program.AddOp(OpInit, 0, 1, 0)  // Jump to address 1
	program.AddOp(OpHalt, 0, 0, 0)  // Halt

	vm := NewVM(program, nil)

	ctx := context.Background()
	err := vm.RunContext(ctx)
	if err != nil {
		t.Fatalf("RunContext failed: %v", err)
	}
}

func TestVM_RunContext_CanceledContext(t *testing.T) {
	// Create a program with a tight loop
	program := NewProgram()
	program.AddOp(OpInit, 0, 1, 0)  // Jump to address 1
	program.AddOp(OpGoto, 0, 1, 0)  // Loop back to address 1 (infinite loop)

	vm := NewVM(program, nil)

	// Create an already-canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	err := vm.RunContext(ctx)
	if err == nil {
		t.Error("expected error with canceled context, got nil")
	}
	if err != context.Canceled {
		t.Errorf("expected context.Canceled error, got: %v", err)
	}
}

func TestVM_RunContext_TimeoutContext(t *testing.T) {
	// Create a program with a tight loop
	program := NewProgram()
	program.AddOp(OpInit, 0, 1, 0)  // Jump to address 1
	program.AddOp(OpGoto, 0, 1, 0)  // Loop back to address 1 (infinite loop)

	vm := NewVM(program, nil)

	// Create a context that times out
	// Note: The timeout must be set before the test runs, because the VM loop
	// is fast and will check context periodically. We set a very short timeout
	// and let it expire before starting.
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
	defer cancel()

	// Wait for the timeout to occur
	time.Sleep(1 * time.Millisecond)

	err := vm.RunContext(ctx)
	if err == nil {
		t.Error("expected error with timed-out context, got nil")
	}
	if err != context.DeadlineExceeded {
		t.Errorf("expected context.DeadlineExceeded error, got: %v", err)
	}
}

func TestVM_RunContext_ContextCheckFrequency(t *testing.T) {
	// Verify that context is checked periodically during execution
	// This test ensures the VM doesn't hang on long-running operations

	program := NewProgram()
	program.AddOp(OpInit, 0, 1, 0)  // Jump to address 1
	program.AddOp(OpGoto, 0, 1, 0)  // Loop back to address 1 (infinite loop)

	vm := NewVM(program, nil)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := vm.RunContext(ctx)
	elapsed := time.Since(start)

	// Should have stopped due to timeout
	if err == nil {
		t.Error("expected error due to timeout")
	}

	// Should not have taken much longer than the timeout
	if elapsed > 100*time.Millisecond {
		t.Errorf("VM took too long to stop: %v", elapsed)
	}
}

func TestVM_Run_StillWorks(t *testing.T) {
	// Verify that the original Run() method still works
	program := NewProgram()
	program.AddOp(OpInit, 0, 1, 0)  // Jump to address 1
	program.AddOp(OpHalt, 0, 0, 0)  // Halt

	vm := NewVM(program, nil)

	err := vm.Run()
	if err != nil {
		t.Fatalf("Run failed: %v", err)
	}
}

func TestVM_Cleanup_ClosesAllCursors(t *testing.T) {
	// Test that Cleanup properly closes all open cursors
	program := NewProgram()
	program.AddOp(OpInit, 0, 1, 0)
	program.AddOp(OpHalt, 0, 0, 0)

	vm := NewVM(program, nil)

	// Manually create some cursor slots to simulate open cursors
	// In real usage, these would be created by OpOpenRead/OpOpenWrite
	vm.cursors = make([]*VDBECursor, 3)
	vm.cursors[0] = &VDBECursor{isOpen: true}
	vm.cursors[1] = &VDBECursor{isOpen: true}
	vm.cursors[2] = nil // Nil cursor should be handled gracefully

	// Call cleanup
	vm.Cleanup()

	// Verify all cursors are closed
	for i, cursor := range vm.cursors {
		if cursor != nil && cursor.isOpen {
			t.Errorf("cursor %d should be closed after Cleanup", i)
		}
	}
}

func TestVM_RunContext_CleansUpOnCancellation(t *testing.T) {
	// Verify that context cancellation triggers resource cleanup
	program := NewProgram()
	program.AddOp(OpInit, 0, 1, 0)  // Jump to address 1
	program.AddOp(OpGoto, 0, 1, 0)  // Loop back to address 1 (infinite loop)

	vm := NewVM(program, nil)

	// Manually set up cursors to verify they get cleaned up
	vm.cursors = make([]*VDBECursor, 2)
	vm.cursors[0] = &VDBECursor{isOpen: true}
	vm.cursors[1] = &VDBECursor{isOpen: true}

	// Create an already-canceled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	// Run should return context error
	err := vm.RunContext(ctx)
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got: %v", err)
	}

	// Verify cursors were cleaned up
	for i, cursor := range vm.cursors {
		if cursor != nil && cursor.isOpen {
			t.Errorf("cursor %d should be closed after context cancellation", i)
		}
	}
}
