package executor

import (
	"fmt"
	"tur/pkg/btree"
	"tur/pkg/record"
	"tur/pkg/sql/parser"
	"tur/pkg/types"
)

// RowIterator is the interface for iterating over rows
type RowIterator interface {
	// Next advances the iterator to the next row. Returns false if no more rows.
	Next() bool

	// Value returns the current row.
	Value() []types.Value

	// Close releases resources.
	Close()
}

// TableScanIterator iterates over a table using a B-tree cursor
type TableScanIterator struct {
	cursor *btree.Cursor
	val    []types.Value
}

func NewTableScanIterator(tree *btree.BTree) *TableScanIterator {
	cursor := tree.Cursor()
	cursor.First()
	// Cursor starts *before* the first element or at first?
	// btree.Cursor logic: First() positions at first.
	// But usually Next() is called to advance.
	// Scan logic in executor.go: for cursor.First(); cursor.Valid(); cursor.Next()
	// My Iterator interface usually assumes Next() moves to next.
	// Does Next() return true if valid?
	// Standard SQL iterator: Next() moves to next valid row and returns true.
	// If First() puts it on first row, then iterator state should track "started".

	return &TableScanIterator{
		cursor: cursor,
	}
}

// Next advances to the next row
func (it *TableScanIterator) Next() bool {
	// If value is nil (first call), we might already rely on cursor being at First??
	// Executor loop: cursor.First(); cursor.Valid(); cursor.Next()
	// My iterator:
	// First call to Next() -> check if current isValid?
	// Or First call -> do nothing?
	// Usually Iterator starts "before" first row.
	// But btree cursor First() positions on first row.
	// So first Next() should just start?
	// Let's implement simpler:
	// Store state "started".
	// If !started: check Valid(). If valid, set started=true, decode value. Return true.
	// If started: call cursor.Next(). Check Valid(). If valid, decode. Return true.

	// But wait, constructing TableScanIterator calls cursor.First().
	// So it's ALREADY on first row.
	// So first call to Next() should return THIS row.
	// Subsequent calls should call cursor.Next().

	// This implies we need a flag `first`
	// Or we initialize cursor before first row? BTree doesn't support that easily?
	// Let's use `started` flag.

	if it.val == nil { // Not started or cached? No, val is current row.
		if !it.cursor.Valid() {
			return false
		}
		// First row
		valFunc := it.cursor.Value()
		if valFunc == nil {
			return false // specific logic for deleted?
		}
		it.val = record.Decode(valFunc)
		// We need to advance cursor for NEXT call?
		// No, Next() puts us ON the row.
		// So consecutive Next() calls:
		// 1. Next() -> (started=false). Check Valid(). OK. Set started=true. Return true.
		// 2. Next() -> (started=true). Call cursor.Next(). Check Valid(). OK. Return true.

		// To handle this, we need a struct field `started bool`.
		// But I defined `val []types.Value`. If `val` is set, we are on a row.
		// But distinguishing "Before First" and "On First" using `val`?
		// If `val` is nil, it might be Before First.
		// But in Next(), if we are Before First, we stay on First.
		// If we are On First, move to Second.
		// Wait, this logic logic is messy.

		// Better: NewTableScanIterator does NOT call First().
		// Next() calls First() if first time, else Next().
		return it.cursor.Valid() // placeholder logic, will fix in implementation below
	}

	it.cursor.Next()
	if !it.cursor.Valid() {
		it.val = nil
		return false
	}

	valBytes := it.cursor.Value()
	if valBytes == nil {
		it.val = nil
		return false
	}
	it.val = record.Decode(valBytes)
	return true
}

func (it *TableScanIterator) Value() []types.Value {
	return it.val
}

func (it *TableScanIterator) Close() {
	it.cursor.Close()
}

// FilterIterator filters rows from child iterator
type FilterIterator struct {
	child     RowIterator
	condition parser.Expression
	colMap    map[string]int
	executor  *Executor
	val       []types.Value
}

func (it *FilterIterator) Next() bool {
	for it.child.Next() {
		row := it.child.Value()
		match, err := it.executor.evaluateCondition(it.condition, row, it.colMap)
		if err != nil {
			// In iterator interface, hard to propagate error.
			// TODO: Add Error() method to iterator?
			// For now, treat error as non-match or log?
			// panic? panic for now as fallback.
			fmt.Printf("Filter error: %v\n", err)
			return false
		}
		if match {
			it.val = row
			return true
		}
	}
	it.val = nil
	return false
}

func (it *FilterIterator) Value() []types.Value {
	return it.val
}

func (it *FilterIterator) Close() {
	it.child.Close()
}

// ProjectionIterator projects columns
type ProjectionIterator struct {
	child       RowIterator
	expressions []parser.Expression
	colMap      map[string]int // Input schema mapping
	executor    *Executor
	val         []types.Value
}

func (it *ProjectionIterator) Next() bool {
	if it.child.Next() {
		inputRow := it.child.Value()
		outputRow := make([]types.Value, len(it.expressions))
		for i, expr := range it.expressions {
			val, err := it.executor.evaluateExpr(expr, inputRow, it.colMap)
			if err != nil {
				fmt.Printf("Projection error: %v\n", err)
				return false
			}
			outputRow[i] = val
		}
		it.val = outputRow
		return true
	}
	it.val = nil
	return false
}

func (it *ProjectionIterator) Value() []types.Value {
	return it.val
}

func (it *ProjectionIterator) Close() {
	it.child.Close()
}

// NestedLoopJoinIterator performs nested loop join
type NestedLoopJoinIterator struct {
	left      RowIterator
	right     RowIterator
	condition parser.Expression
	executor  *Executor

	// Join state
	leftRow []types.Value
	val     []types.Value

	// Schema info needed for condition evaluation
	// We need a combined colMap for (Left + Right)
	leftSchemaLen int
	combinedMap   map[string]int // Maps column name -> index in combined row

	// We need to restart Right iterator for every Left row.
	// But RowIterator interface doesn't have Restart/Reset.
	// We might need to cache Right rows if it's not resettable (like a scan).
	// BTree cursor scan IS resettable (create new iterator).
	// But generic RowIterator?
	// Constraint: Right child must be resettable or materialized.
	// For simple MVP, assume Right side can be materialized in memory?
	// Or add Reset() method to Iterator interface?
	// Or pass a "Right Child Factory" (function that returns new iterator)?

	// Materializing right side is safest for now for generic inputs.
	rightRows         [][]types.Value
	rightIdx          int
	rightMaterialized bool
}

// Note: To support Join properly with arbitrary Plan, we need to be able to iterate Right multiple times.
// TableScan can be reset. But complex subtree?
// Materialization is standard for simple NLJ.

func (it *NestedLoopJoinIterator) Next() bool {
	// First time: materialize right side
	if !it.rightMaterialized {
		for it.right.Next() {
			// Copy row to avoid referencing changing buffer
			row := it.right.Value()
			clone := make([]types.Value, len(row))
			copy(clone, row)
			it.rightRows = append(it.rightRows, clone)
		}
		it.right.Close() // Close original right iterator
		it.rightMaterialized = true

		// Initialize left
		if !it.left.Next() {
			return false // Empty left
		}
		it.leftRow = it.left.Value()
		it.rightIdx = 0
	}

	for {
		// Iterate right rows
		if it.rightIdx < len(it.rightRows) {
			rightRow := it.rightRows[it.rightIdx]
			it.rightIdx++

			// Combine rows
			combined := append(append([]types.Value{}, it.leftRow...), rightRow...) // simple concat

			// Check condition
			match, err := it.executor.evaluateCondition(it.condition, combined, it.combinedMap)
			if err != nil {
				fmt.Printf("Join error: %v\n", err)
				return false
			}

			if match {
				it.val = combined
				return true
			}
			continue
		}

		// Right exhausted, move to next left
		if it.left.Next() {
			it.leftRow = it.left.Value()
			it.rightIdx = 0 // Reset right
			continue
		}

		// Left exhausted
		return false
	}
}

func (it *NestedLoopJoinIterator) Value() []types.Value {
	return it.val
}

func (it *NestedLoopJoinIterator) Close() {
	it.left.Close()
	// Right is already closed/materialized
}
