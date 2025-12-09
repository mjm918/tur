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

	// Join type (INNER, LEFT, RIGHT, FULL)
	joinType parser.JoinType

	// Join state
	leftRow []types.Value
	val     []types.Value

	// Schema info needed for condition evaluation
	// We need a combined colMap for (Left + Right)
	leftSchemaLen  int
	rightSchemaLen int
	combinedMap    map[string]int // Maps column name -> index in combined row

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

	// Outer join state
	leftMatched   bool   // Current left row matched at least one right row
	rightMatched  []bool // Tracks which right rows have matched (for RIGHT/FULL joins)
	phase         int    // 0 = normal join, 1 = emitting unmatched right rows (for RIGHT/FULL)
	unmatchedIdx  int    // Index for iterating unmatched right rows
	leftExhausted bool   // Flag to indicate left side is exhausted
}

// Note: To support Join properly with arbitrary Plan, we need to be able to iterate Right multiple times.
// TableScan can be reset. But complex subtree?
// Materialization is standard for simple NLJ.

func (it *NestedLoopJoinIterator) Next() bool {
	// Phase 1: emit unmatched right rows (for RIGHT/FULL joins)
	if it.phase == 1 {
		return it.nextUnmatchedRight()
	}

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

		// Initialize rightMatched tracking for RIGHT/FULL joins
		if it.joinType == parser.JoinRight || it.joinType == parser.JoinFull {
			it.rightMatched = make([]bool, len(it.rightRows))
		}

		// Infer rightSchemaLen from first right row if not set
		if it.rightSchemaLen == 0 && len(it.rightRows) > 0 {
			it.rightSchemaLen = len(it.rightRows[0])
		}

		// Initialize left
		if !it.left.Next() {
			// Left is empty - for RIGHT/FULL joins, still need to emit right rows with NULL left
			if it.joinType == parser.JoinRight || it.joinType == parser.JoinFull {
				it.phase = 1
				it.unmatchedIdx = 0
				return it.nextUnmatchedRight()
			}
			return false // Empty left for INNER/LEFT join
		}
		it.leftRow = make([]types.Value, len(it.left.Value()))
		copy(it.leftRow, it.left.Value())
		it.rightIdx = 0
		it.leftMatched = false
	}

	for {
		// Iterate right rows
		if it.rightIdx < len(it.rightRows) {
			rightRow := it.rightRows[it.rightIdx]
			rightIdx := it.rightIdx
			it.rightIdx++

			// Combine rows
			combined := make([]types.Value, len(it.leftRow)+len(rightRow))
			copy(combined, it.leftRow)
			copy(combined[len(it.leftRow):], rightRow)

			// Check condition
			match, err := it.executor.evaluateCondition(it.condition, combined, it.combinedMap)
			if err != nil {
				fmt.Printf("Join error: %v\n", err)
				return false
			}

			if match {
				it.leftMatched = true
				// Track that this right row matched (for RIGHT/FULL joins)
				if it.rightMatched != nil {
					it.rightMatched[rightIdx] = true
				}
				it.val = combined
				return true
			}
			continue
		}

		// Right exhausted for current left row
		// For LEFT/FULL joins: emit unmatched left row with NULL right
		if !it.leftMatched && !it.leftExhausted && (it.joinType == parser.JoinLeft || it.joinType == parser.JoinFull) {
			it.val = it.makeLeftWithNullRight()
			// Now advance to next left row
			if it.left.Next() {
				it.leftRow = make([]types.Value, len(it.left.Value()))
				copy(it.leftRow, it.left.Value())
				it.rightIdx = 0
				it.leftMatched = false
			} else {
				// Left exhausted after this emission
				it.leftExhausted = true
				// For FULL join, need to emit unmatched right rows next
				if it.joinType == parser.JoinFull {
					it.phase = 1
					it.unmatchedIdx = 0
				}
			}
			return true
		}

		// If left is exhausted (for LEFT join after emitting last NULL-padded row)
		if it.leftExhausted {
			return false
		}

		// Move to next left row
		if it.left.Next() {
			it.leftRow = make([]types.Value, len(it.left.Value()))
			copy(it.leftRow, it.left.Value())
			it.rightIdx = 0 // Reset right
			it.leftMatched = false
			continue
		}

		// Left exhausted - for RIGHT/FULL joins, emit unmatched right rows
		if it.joinType == parser.JoinRight || it.joinType == parser.JoinFull {
			it.phase = 1
			it.unmatchedIdx = 0
			return it.nextUnmatchedRight()
		}

		return false
	}
}

// nextUnmatchedRight emits right rows that didn't match any left row (for RIGHT/FULL joins)
func (it *NestedLoopJoinIterator) nextUnmatchedRight() bool {
	for it.unmatchedIdx < len(it.rightRows) {
		idx := it.unmatchedIdx
		it.unmatchedIdx++

		if !it.rightMatched[idx] {
			it.val = it.makeNullLeftWithRight(it.rightRows[idx])
			return true
		}
	}
	return false
}

// makeLeftWithNullRight creates a row with left values and NULL-padded right side
func (it *NestedLoopJoinIterator) makeLeftWithNullRight() []types.Value {
	result := make([]types.Value, it.leftSchemaLen+it.rightSchemaLen)
	copy(result, it.leftRow)
	for i := it.leftSchemaLen; i < len(result); i++ {
		result[i] = types.NewNull()
	}
	return result
}

// makeNullLeftWithRight creates a row with NULL-padded left side and right values
func (it *NestedLoopJoinIterator) makeNullLeftWithRight(rightRow []types.Value) []types.Value {
	result := make([]types.Value, it.leftSchemaLen+it.rightSchemaLen)
	for i := 0; i < it.leftSchemaLen; i++ {
		result[i] = types.NewNull()
	}
	copy(result[it.leftSchemaLen:], rightRow)
	return result
}

func (it *NestedLoopJoinIterator) Value() []types.Value {
	return it.val
}

func (it *NestedLoopJoinIterator) Close() {
	it.left.Close()
	// Right is already closed/materialized
}

// HashJoinIterator performs hash join for equi-joins
// Build phase: materialize left side into hash table keyed by join key
// Probe phase: for each right row, look up matching left rows
type HashJoinIterator struct {
	left     RowIterator
	right    RowIterator
	executor *Executor

	// Join key column indices
	leftKeyIdx  int
	rightKeyIdx int

	// Schema info
	leftSchemaLen  int
	rightSchemaLen int
	combinedMap    map[string]int

	// Hash table: key hash -> list of left rows with that key
	hashTable map[string][][]types.Value

	// Current state
	currentRightRow  []types.Value
	matchingLeftRows [][]types.Value
	matchingIdx      int
	built            bool
	rightExhausted   bool
	val              []types.Value
}

// buildHashTable materializes the left side into a hash map
func (it *HashJoinIterator) buildHashTable() {
	it.hashTable = make(map[string][][]types.Value)

	for it.left.Next() {
		row := it.left.Value()
		clone := make([]types.Value, len(row))
		copy(clone, row)

		// Get key value
		if it.leftKeyIdx >= len(clone) {
			continue
		}
		keyVal := clone[it.leftKeyIdx]
		keyStr := valueToHashKey(keyVal)

		it.hashTable[keyStr] = append(it.hashTable[keyStr], clone)
	}
	it.left.Close()

	// Infer leftSchemaLen from first entry if not set
	if it.leftSchemaLen == 0 {
		for _, rows := range it.hashTable {
			if len(rows) > 0 {
				it.leftSchemaLen = len(rows[0])
				break
			}
		}
	}
}

func (it *HashJoinIterator) Next() bool {
	// Build phase (first call)
	if !it.built {
		it.buildHashTable()
		it.built = true
	}

	// Continue matching from current bucket
	for {
		// If we have matching left rows for current right row
		if it.matchingIdx < len(it.matchingLeftRows) {
			leftRow := it.matchingLeftRows[it.matchingIdx]
			it.matchingIdx++

			// Combine left + right rows
			combined := make([]types.Value, len(leftRow)+len(it.currentRightRow))
			copy(combined, leftRow)
			copy(combined[len(leftRow):], it.currentRightRow)
			it.val = combined
			return true
		}

		// Need next right row
		if it.rightExhausted {
			return false
		}

		if !it.right.Next() {
			it.rightExhausted = true
			return false
		}

		// Get right row and probe hash table
		rightRow := it.right.Value()
		it.currentRightRow = make([]types.Value, len(rightRow))
		copy(it.currentRightRow, rightRow)

		// Infer rightSchemaLen
		if it.rightSchemaLen == 0 {
			it.rightSchemaLen = len(rightRow)
		}

		// Get key value from right row
		if it.rightKeyIdx >= len(rightRow) {
			it.matchingLeftRows = nil
			it.matchingIdx = 0
			continue
		}
		keyVal := rightRow[it.rightKeyIdx]
		keyStr := valueToHashKey(keyVal)

		// Look up in hash table
		it.matchingLeftRows = it.hashTable[keyStr]
		it.matchingIdx = 0
	}
}

func (it *HashJoinIterator) Value() []types.Value {
	return it.val
}

func (it *HashJoinIterator) Close() {
	it.right.Close()
	// Left is already closed
}

// valueToHashKey converts a value to a string key for the hash table
func valueToHashKey(v types.Value) string {
	if v.IsNull() {
		return "NULL"
	}
	switch v.Type() {
	case types.TypeInt:
		return fmt.Sprintf("I:%d", v.Int())
	case types.TypeFloat:
		return fmt.Sprintf("F:%f", v.Float())
	case types.TypeText:
		return fmt.Sprintf("T:%s", v.Text())
	case types.TypeBlob:
		return fmt.Sprintf("B:%x", v.Blob())
	default:
		return fmt.Sprintf("?:%v", v)
	}
}

// SortIterator materializes and sorts rows from child
type SortIterator struct {
	child    RowIterator
	orderBy  []parser.OrderByExpr
	colMap   map[string]int
	executor *Executor

	// Sorted rows
	rows     [][]types.Value
	idx      int
	prepared bool
}

func (it *SortIterator) Next() bool {
	if !it.prepared {
		// Materialize all rows from child
		for it.child.Next() {
			row := it.child.Value()
			clone := make([]types.Value, len(row))
			copy(clone, row)
			it.rows = append(it.rows, clone)
		}
		it.child.Close()

		// Sort rows using ORDER BY expressions
		it.sortRows()

		it.idx = 0
		it.prepared = true
	}

	if it.idx < len(it.rows) {
		it.idx++
		return true
	}
	return false
}

func (it *SortIterator) Value() []types.Value {
	if it.idx > 0 && it.idx <= len(it.rows) {
		return it.rows[it.idx-1]
	}
	return nil
}

func (it *SortIterator) Close() {
	// Child already closed
}

// sortRows sorts the materialized rows by ORDER BY expressions
func (it *SortIterator) sortRows() {
	if len(it.rows) == 0 || len(it.orderBy) == 0 {
		return
	}

	// Simple bubble sort for correctness (can optimize later)
	n := len(it.rows)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			if it.compare(it.rows[j], it.rows[j+1]) > 0 {
				it.rows[j], it.rows[j+1] = it.rows[j+1], it.rows[j]
			}
		}
	}
}

// compare compares two rows based on ORDER BY expressions
// Returns: -1 if a < b, 0 if equal, 1 if a > b
func (it *SortIterator) compare(a, b []types.Value) int {
	for _, ob := range it.orderBy {
		// Evaluate expression for both rows
		valA, errA := it.executor.evaluateExpr(ob.Expr, a, it.colMap)
		valB, errB := it.executor.evaluateExpr(ob.Expr, b, it.colMap)

		if errA != nil || errB != nil {
			// On error, treat as equal
			continue
		}

		cmp := compareValuesForSort(valA, valB)
		if cmp != 0 {
			if ob.Direction == parser.OrderDesc {
				return -cmp
			}
			return cmp
		}
	}
	return 0
}

// compareValuesForSort compares two types.Value for sorting
func compareValuesForSort(a, b types.Value) int {
	// Handle NULLs: NULL is considered less than any other value
	if a.IsNull() && b.IsNull() {
		return 0
	}
	if a.IsNull() {
		return -1
	}
	if b.IsNull() {
		return 1
	}

	// Compare by type
	switch a.Type() {
	case types.TypeInt:
		ai, bi := a.Int(), b.Int()
		if ai < bi {
			return -1
		} else if ai > bi {
			return 1
		}
		return 0

	case types.TypeFloat:
		af, bf := a.Float(), b.Float()
		if af < bf {
			return -1
		} else if af > bf {
			return 1
		}
		return 0

	case types.TypeText:
		at, bt := a.Text(), b.Text()
		if at < bt {
			return -1
		} else if at > bt {
			return 1
		}
		return 0

	default:
		// For other types, compare string representation
		as, bs := fmt.Sprintf("%v", a), fmt.Sprintf("%v", b)
		if as < bs {
			return -1
		} else if as > bs {
			return 1
		}
		return 0
	}
}

// LimitIterator limits rows from child iterator
type LimitIterator struct {
	child    RowIterator
	limit    int64 // -1 means no limit
	offset   int64 // 0 means no offset
	count    int64 // rows returned so far
	skipped  int64 // rows skipped due to offset
	prepared bool
}

func (it *LimitIterator) Next() bool {
	// Skip offset rows on first call
	if !it.prepared {
		for i := int64(0); i < it.offset; i++ {
			if !it.child.Next() {
				it.prepared = true
				return false // Not enough rows to skip
			}
			it.skipped++
		}
		it.prepared = true
	}

	// Check limit
	if it.limit >= 0 && it.count >= it.limit {
		return false
	}

	if it.child.Next() {
		it.count++
		return true
	}
	return false
}

func (it *LimitIterator) Value() []types.Value {
	return it.child.Value()
}

func (it *LimitIterator) Close() {
	it.child.Close()
}

// HashGroupByIterator performs GROUP BY with hash-based grouping
type HashGroupByIterator struct {
	child    RowIterator
	groupBy  []parser.Expression // GROUP BY expressions
	having   parser.Expression   // Optional HAVING clause
	colMap   map[string]int      // Input schema mapping
	executor *Executor

	// State
	groups   []groupEntry // Collected groups after materialization
	idx      int          // Current position in groups
	prepared bool
}

// groupEntry represents a single group with its key and accumulated aggregates
type groupEntry struct {
	key        string                     // Serialized group key
	keyValues  []types.Value              // Original key values for output
	aggregates map[string]*aggregateState // funcName -> state
	rows       [][]types.Value            // All rows in this group (for computing aggregates)
}

// aggregateState holds the state of an aggregate function
type aggregateState struct {
	funcName string
	count    int64   // Used by COUNT, AVG
	sum      float64 // Used by SUM, AVG
	min      types.Value
	max      types.Value
	hasValue bool
}

func (it *HashGroupByIterator) Next() bool {
	if !it.prepared {
		it.prepare()
	}

	for it.idx < len(it.groups) {
		group := it.groups[it.idx]
		it.idx++

		// Check HAVING clause if present
		if it.having != nil {
			// Create a row with group key values and aggregate results for HAVING evaluation
			havingRow := it.buildOutputRow(&group)
			match, err := it.executor.evaluateCondition(it.having, havingRow, it.buildHavingColMap(&group))
			if err != nil || !match {
				continue
			}
		}

		return true
	}
	return false
}

// prepare materializes input and builds groups
func (it *HashGroupByIterator) prepare() {
	groupMap := make(map[string]*groupEntry)

	// Materialize all input rows and group them
	for it.child.Next() {
		row := it.child.Value()
		clone := make([]types.Value, len(row))
		copy(clone, row)

		// Compute group key
		key, keyValues := it.computeGroupKey(row)

		// Get or create group
		group, exists := groupMap[key]
		if !exists {
			group = &groupEntry{
				key:        key,
				keyValues:  keyValues,
				aggregates: make(map[string]*aggregateState),
				rows:       nil,
			}
			groupMap[key] = group
		}
		group.rows = append(group.rows, clone)
	}
	it.child.Close()

	// Convert map to slice and compute final aggregates
	for _, group := range groupMap {
		it.computeAggregates(group)
		it.groups = append(it.groups, *group)
	}

	it.idx = 0
	it.prepared = true
}

// computeGroupKey computes a string key for grouping
func (it *HashGroupByIterator) computeGroupKey(row []types.Value) (string, []types.Value) {
	if len(it.groupBy) == 0 {
		// No GROUP BY - everything in one group
		return "", nil
	}

	keyValues := make([]types.Value, len(it.groupBy))
	parts := make([]string, len(it.groupBy))

	for i, expr := range it.groupBy {
		val, err := it.executor.evaluateExpr(expr, row, it.colMap)
		if err != nil {
			parts[i] = "NULL"
			keyValues[i] = types.NewNull()
		} else {
			keyValues[i] = val
			// Serialize value to string for map key
			if val.IsNull() {
				parts[i] = "NULL"
			} else {
				parts[i] = fmt.Sprintf("%v", val)
			}
		}
	}

	// Combine parts with separator
	key := ""
	for i, p := range parts {
		if i > 0 {
			key += "|"
		}
		key += p
	}

	return key, keyValues
}

// computeAggregates computes aggregate values for a group
func (it *HashGroupByIterator) computeAggregates(group *groupEntry) {
	// Initialize aggregate states for common aggregates
	for _, name := range []string{"COUNT", "COUNT*", "SUM", "AVG", "MIN", "MAX"} {
		group.aggregates[name] = &aggregateState{funcName: name}
	}

	// Process each row in the group
	for _, row := range group.rows {
		// COUNT(*)
		group.aggregates["COUNT*"].count++

		// For other aggregates, we'd need to know which columns to aggregate
		// For now, use the first non-key column as a default
		if len(row) > len(it.groupBy) {
			val := row[len(it.groupBy)] // Simple heuristic

			// COUNT (non-null)
			if !val.IsNull() {
				group.aggregates["COUNT"].count++
			}

			// SUM and AVG
			switch val.Type() {
			case types.TypeInt:
				group.aggregates["SUM"].sum += float64(val.Int())
				group.aggregates["AVG"].sum += float64(val.Int())
				group.aggregates["AVG"].count++
				group.aggregates["SUM"].hasValue = true
				group.aggregates["AVG"].hasValue = true
			case types.TypeFloat:
				group.aggregates["SUM"].sum += val.Float()
				group.aggregates["AVG"].sum += val.Float()
				group.aggregates["AVG"].count++
				group.aggregates["SUM"].hasValue = true
				group.aggregates["AVG"].hasValue = true
			}

			// MIN
			if !val.IsNull() {
				state := group.aggregates["MIN"]
				if !state.hasValue || compareValuesForSort(val, state.min) < 0 {
					state.min = val
					state.hasValue = true
				}
			}

			// MAX
			if !val.IsNull() {
				state := group.aggregates["MAX"]
				if !state.hasValue || compareValuesForSort(val, state.max) > 0 {
					state.max = val
					state.hasValue = true
				}
			}
		}
	}
}

// buildOutputRow builds an output row for a group (key values + aggregate results)
func (it *HashGroupByIterator) buildOutputRow(group *groupEntry) []types.Value {
	// Build row with: [group key values...] + commonly computed aggregate
	// Number of output columns = len(groupBy) + 1 (for COUNT* as placeholder)
	result := make([]types.Value, len(group.keyValues)+1)
	copy(result, group.keyValues)

	// Add COUNT(*) as the default aggregate value
	result[len(group.keyValues)] = types.NewInt(group.aggregates["COUNT*"].count)

	return result
}

// buildHavingColMap builds a column map for HAVING evaluation
func (it *HashGroupByIterator) buildHavingColMap(group *groupEntry) map[string]int {
	// For HAVING, we need to map aggregate function names to column indices
	m := make(map[string]int)

	// Map group by column names
	for i, expr := range it.groupBy {
		if colRef, ok := expr.(*parser.ColumnRef); ok {
			m[colRef.Name] = i
		}
	}

	// Map COUNT(*) to the last column
	m["COUNT(*)"] = len(group.keyValues)

	return m
}

func (it *HashGroupByIterator) Value() []types.Value {
	if it.idx > 0 && it.idx <= len(it.groups) {
		group := &it.groups[it.idx-1]
		return it.buildOutputRow(group)
	}
	return nil
}

func (it *HashGroupByIterator) Close() {
	// Child already closed during prepare
}

// CTEScanIterator iterates over materialized CTE results
type CTEScanIterator struct {
	rows  [][]types.Value
	index int
}

func (it *CTEScanIterator) Next() bool {
	it.index++
	return it.index < len(it.rows)
}

func (it *CTEScanIterator) Value() []types.Value {
	if it.index >= 0 && it.index < len(it.rows) {
		return it.rows[it.index]
	}
	return nil
}

func (it *CTEScanIterator) Close() {
	// Nothing to clean up - rows are just references to in-memory data
}

// WindowIterator computes window functions over an input iterator
// It materializes all input rows, then computes window function values
type WindowIterator struct {
	child           RowIterator
	expressions     []parser.Expression // All SELECT expressions (including window funcs)
	colMap          map[string]int
	executor        *Executor
	windowFuncInfos []windowFuncInfo // Info about each window function in expressions

	// State
	outputRows [][]types.Value // Computed output rows with window function values
	index      int
	prepared   bool
}

// windowFuncInfo holds info about a window function in the expression list
type windowFuncInfo struct {
	exprIndex  int                   // Index in expressions slice
	winFunc    *parser.WindowFunction
	funcName   string
}

// NewWindowIterator creates a new WindowIterator
func NewWindowIterator(child RowIterator, expressions []parser.Expression, colMap map[string]int, executor *Executor) *WindowIterator {
	it := &WindowIterator{
		child:       child,
		expressions: expressions,
		colMap:      colMap,
		executor:    executor,
		index:       -1,
	}

	// Find window functions in expressions
	for i, expr := range expressions {
		if wf, ok := expr.(*parser.WindowFunction); ok {
			funcName := ""
			if fc, ok := wf.Function.(*parser.FunctionCall); ok {
				funcName = fc.Name
			}
			it.windowFuncInfos = append(it.windowFuncInfos, windowFuncInfo{
				exprIndex:  i,
				winFunc:    wf,
				funcName:   funcName,
			})
		}
	}

	return it
}

func (it *WindowIterator) Next() bool {
	if !it.prepared {
		it.prepare()
	}
	it.index++
	return it.index < len(it.outputRows)
}

func (it *WindowIterator) Value() []types.Value {
	if it.index >= 0 && it.index < len(it.outputRows) {
		return it.outputRows[it.index]
	}
	return nil
}

func (it *WindowIterator) Close() {
	// Child closed during prepare
}

// prepare materializes input and computes window function values
func (it *WindowIterator) prepare() {
	// Step 1: Materialize all input rows
	var inputRows [][]types.Value
	for it.child.Next() {
		row := it.child.Value()
		clone := make([]types.Value, len(row))
		copy(clone, row)
		inputRows = append(inputRows, clone)
	}
	it.child.Close()

	if len(inputRows) == 0 {
		it.prepared = true
		return
	}

	// Step 2: For each window function, compute values for all rows
	// We'll store computed window function values indexed by [rowIndex][windowFuncIndex]
	windowValues := make([][]types.Value, len(inputRows))
	for i := range windowValues {
		windowValues[i] = make([]types.Value, len(it.windowFuncInfos))
	}

	for wfIdx, wfInfo := range it.windowFuncInfos {
		it.computeWindowFunction(wfInfo, inputRows, windowValues, wfIdx)
	}

	// Step 3: Build output rows
	for rowIdx, inputRow := range inputRows {
		outputRow := make([]types.Value, len(it.expressions))
		wfResultIdx := 0

		for exprIdx, expr := range it.expressions {
			if _, ok := expr.(*parser.WindowFunction); ok {
				// Use pre-computed window function value
				outputRow[exprIdx] = windowValues[rowIdx][wfResultIdx]
				wfResultIdx++
			} else {
				// Evaluate regular expression
				val, err := it.executor.evaluateExpr(expr, inputRow, it.colMap)
				if err != nil {
					outputRow[exprIdx] = types.NewNull()
				} else {
					outputRow[exprIdx] = val
				}
			}
		}
		it.outputRows = append(it.outputRows, outputRow)
	}

	it.prepared = true
}

// computeWindowFunction computes values for a single window function across all rows
func (it *WindowIterator) computeWindowFunction(wfInfo windowFuncInfo, inputRows [][]types.Value, windowValues [][]types.Value, wfIdx int) {
	wf := wfInfo.winFunc
	funcName := wfInfo.funcName

	// Group rows by PARTITION BY
	partitions := it.partitionRows(inputRows, wf.Over.PartitionBy)

	// For each partition, sort by ORDER BY and compute ranks
	for _, partition := range partitions {
		// Sort partition by ORDER BY
		it.sortPartition(partition, wf.Over.OrderBy)

		// Compute function values based on function name
		switch funcName {
		case "RANK":
			it.computeRank(partition, wf.Over.OrderBy, windowValues, wfIdx)
		case "DENSE_RANK":
			it.computeDenseRank(partition, wf.Over.OrderBy, windowValues, wfIdx)
		case "ROW_NUMBER":
			it.computeRowNumber(partition, windowValues, wfIdx)
		default:
			// Unknown window function - return NULL
			for _, entry := range partition {
				windowValues[entry.originalIndex][wfIdx] = types.NewNull()
			}
		}
	}
}

// partitionEntry holds a row with its original index for tracking
type partitionEntry struct {
	row           []types.Value
	originalIndex int
}

// partitionRows groups rows by PARTITION BY expressions
func (it *WindowIterator) partitionRows(inputRows [][]types.Value, partitionBy []parser.Expression) [][]partitionEntry {
	if len(partitionBy) == 0 {
		// No partitioning - all rows in one partition
		partition := make([]partitionEntry, len(inputRows))
		for i, row := range inputRows {
			partition[i] = partitionEntry{row: row, originalIndex: i}
		}
		return [][]partitionEntry{partition}
	}

	// Group by partition key
	partitionMap := make(map[string][]partitionEntry)
	var partitionOrder []string // Track order of first occurrence

	for i, row := range inputRows {
		key := it.computePartitionKey(row, partitionBy)
		if _, exists := partitionMap[key]; !exists {
			partitionOrder = append(partitionOrder, key)
		}
		partitionMap[key] = append(partitionMap[key], partitionEntry{row: row, originalIndex: i})
	}

	// Return partitions in order of first occurrence
	var result [][]partitionEntry
	for _, key := range partitionOrder {
		result = append(result, partitionMap[key])
	}
	return result
}

// computePartitionKey computes a string key for grouping
func (it *WindowIterator) computePartitionKey(row []types.Value, partitionBy []parser.Expression) string {
	var parts []string
	for _, expr := range partitionBy {
		val, err := it.executor.evaluateExpr(expr, row, it.colMap)
		if err != nil || val.IsNull() {
			parts = append(parts, "NULL")
		} else {
			parts = append(parts, fmt.Sprintf("%v", val))
		}
	}
	key := ""
	for i, p := range parts {
		if i > 0 {
			key += "|"
		}
		key += p
	}
	return key
}

// sortPartition sorts partition entries by ORDER BY expressions
func (it *WindowIterator) sortPartition(partition []partitionEntry, orderBy []parser.OrderByExpr) {
	if len(orderBy) == 0 {
		return
	}

	// Simple bubble sort (for correctness; optimize later if needed)
	for i := 0; i < len(partition)-1; i++ {
		for j := 0; j < len(partition)-i-1; j++ {
			if it.compareOrderBy(partition[j].row, partition[j+1].row, orderBy) > 0 {
				partition[j], partition[j+1] = partition[j+1], partition[j]
			}
		}
	}
}

// compareOrderBy compares two rows by ORDER BY expressions
// Returns negative if row1 < row2, 0 if equal, positive if row1 > row2
func (it *WindowIterator) compareOrderBy(row1, row2 []types.Value, orderBy []parser.OrderByExpr) int {
	for _, ob := range orderBy {
		val1, _ := it.executor.evaluateExpr(ob.Expr, row1, it.colMap)
		val2, _ := it.executor.evaluateExpr(ob.Expr, row2, it.colMap)

		cmp := it.executor.compareValues(val1, val2)
		if cmp != 0 {
			if ob.Direction == parser.OrderDesc {
				return -cmp
			}
			return cmp
		}
	}
	return 0
}

// areOrderByValuesEqual checks if two rows have equal ORDER BY values (for peer detection)
func (it *WindowIterator) areOrderByValuesEqual(row1, row2 []types.Value, orderBy []parser.OrderByExpr) bool {
	for _, ob := range orderBy {
		val1, _ := it.executor.evaluateExpr(ob.Expr, row1, it.colMap)
		val2, _ := it.executor.evaluateExpr(ob.Expr, row2, it.colMap)

		if it.executor.compareValues(val1, val2) != 0 {
			return false
		}
	}
	return true
}

// computeRank computes RANK() with gaps for ties
func (it *WindowIterator) computeRank(partition []partitionEntry, orderBy []parser.OrderByExpr, windowValues [][]types.Value, wfIdx int) {
	if len(partition) == 0 {
		return
	}

	rank := int64(1)
	for i, entry := range partition {
		if i > 0 {
			// Check if this row has same ORDER BY values as previous (peer)
			if !it.areOrderByValuesEqual(partition[i-1].row, entry.row, orderBy) {
				// Not a peer - rank jumps to current position + 1
				rank = int64(i + 1)
			}
			// If peer, rank stays the same
		}
		windowValues[entry.originalIndex][wfIdx] = types.NewInt(rank)
	}
}

// computeDenseRank computes DENSE_RANK() without gaps
func (it *WindowIterator) computeDenseRank(partition []partitionEntry, orderBy []parser.OrderByExpr, windowValues [][]types.Value, wfIdx int) {
	if len(partition) == 0 {
		return
	}

	denseRank := int64(1)
	for i, entry := range partition {
		if i > 0 {
			// Check if this row has same ORDER BY values as previous (peer)
			if !it.areOrderByValuesEqual(partition[i-1].row, entry.row, orderBy) {
				// Not a peer - dense rank increments by 1 (no gaps)
				denseRank++
			}
			// If peer, rank stays the same
		}
		windowValues[entry.originalIndex][wfIdx] = types.NewInt(denseRank)
	}
}

// computeRowNumber computes ROW_NUMBER() (unique sequential number)
func (it *WindowIterator) computeRowNumber(partition []partitionEntry, windowValues [][]types.Value, wfIdx int) {
	for i, entry := range partition {
		windowValues[entry.originalIndex][wfIdx] = types.NewInt(int64(i + 1))
	}
}
