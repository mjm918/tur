// pkg/vdbe/cursor_test.go
package vdbe

import (
	"path/filepath"
	"testing"

	"tur/pkg/btree"
	"tur/pkg/pager"
	"tur/pkg/record"
	"tur/pkg/types"
)

func TestVMCursorOpenRewindNext(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := pager.Open(path, pager.Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	// Create a B-tree and insert some data
	bt, err := btree.Create(p)
	if err != nil {
		t.Fatalf("failed to create btree: %v", err)
	}

	// Insert 3 rows with key 1, 2, 3
	for i := 1; i <= 3; i++ {
		key := make([]byte, 8)
		key[7] = byte(i)
		values := []types.Value{types.NewInt(int64(i)), types.NewText("row")}
		data := record.Encode(values)
		if err := bt.Insert(key, data); err != nil {
			t.Fatalf("insert failed: %v", err)
		}
	}

	// Program to iterate through the B-tree:
	// OpenRead cursor 0 on root page
	// Rewind cursor 0, jump to end if empty
	// Loop: Column 0, 0 -> r[1]  (read first column into r[1])
	// ResultRow r[1], 1
	// Next cursor 0, jump to Loop
	// Halt

	prog := NewProgram()
	rootPage := bt.RootPage()

	prog.AddOp(OpOpenRead, 0, int(rootPage), 0) // 0: Open cursor 0 for reading
	addrRewind := prog.AddOp(OpRewind, 0, 0, 0) // 1: Rewind, jump to end if empty
	prog.AddOp(OpColumn, 0, 0, 1)               // 2: r[1] = cursor[0].column[0]
	prog.AddOp(OpResultRow, 1, 1, 0)            // 3: Output r[1]
	addrNext := prog.AddOp(OpNext, 0, 2, 0)     // 4: Next, jump to 2 if more rows
	prog.AddOp(OpClose, 0, 0, 0)                // 5: Close cursor
	prog.AddOp(OpHalt, 0, 0, 0)                 // 6: Halt

	// Fix jump targets
	prog.ChangeP2(addrRewind, 5) // Rewind jumps to Close if empty
	_ = addrNext                 // Next jumps to 2 (already set)

	vm := NewVM(prog, p)
	vm.SetNumRegisters(5)

	err = vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	results := vm.Results()
	if len(results) != 3 {
		t.Fatalf("expected 3 result rows, got %d", len(results))
	}

	// Check we got values 1, 2, 3
	for i, row := range results {
		expected := int64(i + 1)
		if row[0].Int() != expected {
			t.Errorf("row %d: expected %d, got %d", i, expected, row[0].Int())
		}
	}
}

func TestVMCursorInsert(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := pager.Open(path, pager.Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	// Create an empty B-tree
	bt, err := btree.Create(p)
	if err != nil {
		t.Fatalf("failed to create btree: %v", err)
	}

	// Program to insert a row:
	// OpenWrite cursor 0
	// Integer 1 -> r[1] (rowid)
	// Integer 42 -> r[2] (value)
	// String "hello" -> r[3]
	// MakeRecord r[2], 2 -> r[4] (record from r[2] and r[3])
	// Insert cursor 0, r[4], r[1]
	// Close cursor 0
	// Halt

	prog := NewProgram()
	rootPage := bt.RootPage()

	prog.AddOp(OpOpenWrite, 0, int(rootPage), 0) // 0: Open cursor 0 for writing
	prog.AddOp(OpInteger, 1, 1, 0)               // 1: r[1] = 1 (rowid)
	prog.AddOp(OpInteger, 42, 2, 0)              // 2: r[2] = 42
	prog.AddOp4(OpString, 5, 3, 0, "hello")      // 3: r[3] = "hello"
	prog.AddOp(OpMakeRecord, 2, 2, 4)            // 4: r[4] = record(r[2], r[3])
	prog.AddOp(OpInsert, 0, 4, 1)                // 5: Insert r[4] with rowid r[1]
	prog.AddOp(OpClose, 0, 0, 0)                 // 6: Close cursor
	prog.AddOp(OpHalt, 0, 0, 0)                  // 7: Halt

	vm := NewVM(prog, p)
	vm.SetNumRegisters(10)

	err = vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify the insert by reading back
	cursor := bt.Cursor()
	defer cursor.Close()

	cursor.First()
	if !cursor.Valid() {
		t.Fatal("expected one row after insert")
	}

	values := record.Decode(cursor.Value())
	if len(values) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(values))
	}
	if values[0].Int() != 42 {
		t.Errorf("expected 42, got %d", values[0].Int())
	}
	if values[1].Text() != "hello" {
		t.Errorf("expected 'hello', got '%s'", values[1].Text())
	}
}

func TestVMCursorEmptyTable(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := pager.Open(path, pager.Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	// Create an empty B-tree
	bt, err := btree.Create(p)
	if err != nil {
		t.Fatalf("failed to create btree: %v", err)
	}

	// Program to iterate empty table - should jump immediately
	prog := NewProgram()
	rootPage := bt.RootPage()

	prog.AddOp(OpOpenRead, 0, int(rootPage), 0) // 0: Open cursor 0
	prog.AddOp(OpRewind, 0, 3, 0)               // 1: Rewind, jump to 3 if empty
	prog.AddOp(OpResultRow, 1, 1, 0)            // 2: Output (should be skipped)
	prog.AddOp(OpClose, 0, 0, 0)                // 3: Close cursor
	prog.AddOp(OpHalt, 0, 0, 0)                 // 4: Halt

	vm := NewVM(prog, p)
	vm.SetNumRegisters(5)

	err = vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	results := vm.Results()
	if len(results) != 0 {
		t.Errorf("expected 0 result rows for empty table, got %d", len(results))
	}
}

func TestVMCursorMultipleColumns(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "test.db")

	p, err := pager.Open(path, pager.Options{PageSize: 4096})
	if err != nil {
		t.Fatalf("failed to open pager: %v", err)
	}
	defer p.Close()

	// Create a B-tree and insert data with multiple columns
	bt, err := btree.Create(p)
	if err != nil {
		t.Fatalf("failed to create btree: %v", err)
	}

	// Insert a row with 3 columns
	key := make([]byte, 8)
	key[7] = 1
	values := []types.Value{
		types.NewInt(100),
		types.NewText("test"),
		types.NewFloat(3.14),
	}
	data := record.Encode(values)
	if err := bt.Insert(key, data); err != nil {
		t.Fatalf("insert failed: %v", err)
	}

	// Program to read all 3 columns
	prog := NewProgram()
	rootPage := bt.RootPage()

	prog.AddOp(OpOpenRead, 0, int(rootPage), 0) // 0: Open cursor 0
	prog.AddOp(OpRewind, 0, 6, 0)               // 1: Rewind
	prog.AddOp(OpColumn, 0, 0, 1)               // 2: r[1] = column 0
	prog.AddOp(OpColumn, 0, 1, 2)               // 3: r[2] = column 1
	prog.AddOp(OpColumn, 0, 2, 3)               // 4: r[3] = column 2
	prog.AddOp(OpResultRow, 1, 3, 0)            // 5: Output r[1..3]
	prog.AddOp(OpClose, 0, 0, 0)                // 6: Close cursor
	prog.AddOp(OpHalt, 0, 0, 0)                 // 7: Halt

	vm := NewVM(prog, p)
	vm.SetNumRegisters(10)

	err = vm.Run()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	results := vm.Results()
	if len(results) != 1 {
		t.Fatalf("expected 1 result row, got %d", len(results))
	}

	row := results[0]
	if len(row) != 3 {
		t.Fatalf("expected 3 columns, got %d", len(row))
	}

	if row[0].Int() != 100 {
		t.Errorf("column 0: expected 100, got %d", row[0].Int())
	}
	if row[1].Text() != "test" {
		t.Errorf("column 1: expected 'test', got '%s'", row[1].Text())
	}
	if row[2].Float() != 3.14 {
		t.Errorf("column 2: expected 3.14, got %f", row[2].Float())
	}
}
