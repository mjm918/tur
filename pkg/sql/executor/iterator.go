package executor

import (
	"fmt"
	"sort"

	"tur/pkg/record"
	"tur/pkg/schema"
	"tur/pkg/sql/optimizer"
	"tur/pkg/sql/parser"
	"tur/pkg/tree"
	"tur/pkg/types"
)

// RowIterator is the interface for iterating over rows
type RowIterator interface {
	// Next advances the iterator to the next row. Returns false if no more rows.
	Next() bool

	// Value returns the current row.
	Value() []types.Value

	// Err returns any error that occurred during iteration (e.g., RAISE in triggers)
	Err() error

	// Close releases resources.
	Close()
}

// TableScanIterator iterates over a table using a B-tree cursor
type TableScanIterator struct {
	cursor tree.Cursor
	table  *schema.TableDef // Optional: for type conversion (JSON columns)
	val    []types.Value
}

func NewTableScanIterator(t tree.Tree) *TableScanIterator {
	cursor := t.Cursor()
	cursor.First()
	return &TableScanIterator{
		cursor: cursor,
	}
}

// NewTableScanIteratorWithSchema creates a TableScanIterator with schema for type conversion
func NewTableScanIteratorWithSchema(t tree.Tree, table *schema.TableDef) *TableScanIterator {
	cursor := t.Cursor()
	cursor.First()
	return &TableScanIterator{
		cursor: cursor,
		table:  table,
	}
}

// Next advances to the next row
func (it *TableScanIterator) Next() bool {
	if it.val == nil { // First call
		if !it.cursor.Valid() {
			return false
		}
		valFunc := it.cursor.Value()
		if valFunc == nil {
			return false
		}
		it.val = record.Decode(valFunc)
		it.applyTypeConversions()
		return it.cursor.Valid()
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
	it.applyTypeConversions()
	return true
}

// applyTypeConversions converts TEXT back to JSON for JSON columns
func (it *TableScanIterator) applyTypeConversions() {
	if it.table == nil || it.val == nil {
		return
	}
	for i, col := range it.table.Columns {
		if i < len(it.val) && col.Type == types.TypeJSON && it.val[i].Type() == types.TypeText {
			it.val[i] = types.NewJSON(it.val[i].Text())
		}
	}
}

func (it *TableScanIterator) Value() []types.Value {
	return it.val
}

func (it *TableScanIterator) Err() error {
	return nil
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
	err       error
}

func (it *FilterIterator) Next() bool {
	// Check for child errors first
	if err := it.child.Err(); err != nil {
		it.err = err
		return false
	}
	for it.child.Next() {
		row := it.child.Value()
		match, err := it.executor.evaluateCondition(it.condition, row, it.colMap)
		if err != nil {
			it.err = err
			return false
		}
		if match {
			it.val = row
			return true
		}
	}
	// Check for child errors after iteration
	if err := it.child.Err(); err != nil {
		it.err = err
	}
	it.val = nil
	return false
}

func (it *FilterIterator) Value() []types.Value {
	return it.val
}

func (it *FilterIterator) Err() error {
	return it.err
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
	err         error
}

func (it *ProjectionIterator) Next() bool {
	// Check for child errors first
	if err := it.child.Err(); err != nil {
		it.err = err
		return false
	}
	if it.child.Next() {
		inputRow := it.child.Value()
		outputRow := make([]types.Value, len(it.expressions))
		for i, expr := range it.expressions {
			val, err := it.executor.evaluateExpr(expr, inputRow, it.colMap)
			if err != nil {
				it.err = err
				return false
			}
			outputRow[i] = val
		}
		it.val = outputRow
		return true
	}
	// Check for child errors after iteration
	if err := it.child.Err(); err != nil {
		it.err = err
	}
	it.val = nil
	return false
}

func (it *ProjectionIterator) Value() []types.Value {
	return it.val
}

func (it *ProjectionIterator) Err() error {
	return it.err
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

func (it *NestedLoopJoinIterator) Err() error {
	return nil
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

func (it *HashJoinIterator) Err() error {
	return nil
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

func (it *SortIterator) Err() error {
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

// isIntegerTypeForSort returns true if the type is any integer type
func isIntegerTypeForSort(t types.ValueType) bool {
	switch t {
	case types.TypeInt, types.TypeSmallInt, types.TypeInt32, types.TypeBigInt, types.TypeSerial, types.TypeBigSerial:
		return true
	}
	return false
}

// isStringTypeForSort returns true if the type is any string type
func isStringTypeForSort(t types.ValueType) bool {
	switch t {
	case types.TypeText, types.TypeVarchar, types.TypeChar:
		return true
	}
	return false
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

	// Handle cross-type comparisons for compatible types
	// All integer types can be compared with each other
	if isIntegerTypeForSort(a.Type()) && isIntegerTypeForSort(b.Type()) {
		ai, bi := a.Int(), b.Int()
		if ai < bi {
			return -1
		} else if ai > bi {
			return 1
		}
		return 0
	}

	// All string types can be compared with each other
	if isStringTypeForSort(a.Type()) && isStringTypeForSort(b.Type()) {
		at, bt := a.Text(), b.Text()
		if at < bt {
			return -1
		} else if at > bt {
			return 1
		}
		return 0
	}

	// Integer and float comparison
	if isIntegerTypeForSort(a.Type()) && b.Type() == types.TypeFloat {
		af, bf := float64(a.Int()), b.Float()
		if af < bf {
			return -1
		} else if af > bf {
			return 1
		}
		return 0
	}
	if a.Type() == types.TypeFloat && isIntegerTypeForSort(b.Type()) {
		af, bf := a.Float(), float64(b.Int())
		if af < bf {
			return -1
		} else if af > bf {
			return 1
		}
		return 0
	}

	// Same type comparisons
	switch a.Type() {
	case types.TypeFloat:
		af, bf := a.Float(), b.Float()
		if af < bf {
			return -1
		} else if af > bf {
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

func (it *LimitIterator) Err() error {
	return it.child.Err()
}

func (it *LimitIterator) Close() {
	it.child.Close()
}

// HashGroupByIterator performs GROUP BY with hash-based grouping
type HashGroupByIterator struct {
	child      RowIterator
	groupBy    []parser.Expression       // GROUP BY expressions
	aggregates []optimizer.AggregateExpr // Aggregate functions to compute
	having     parser.Expression         // Optional HAVING clause
	colMap     map[string]int            // Input schema mapping
	executor   *Executor

	// State
	groups   []groupEntry // Collected groups after materialization
	idx      int          // Current position in groups
	prepared bool
}

// groupEntry represents a single group with its key and accumulated aggregates
type groupEntry struct {
	key             string            // Serialized group key
	keyValues       []types.Value     // Original key values for output
	aggregateValues []types.Value     // Computed aggregate results (one per aggregate)
	rows            [][]types.Value   // All rows in this group (for computing aggregates)
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
				key:       key,
				keyValues: keyValues,
				rows:      nil,
			}
			groupMap[key] = group
		}
		group.rows = append(group.rows, clone)
	}
	it.child.Close()

	// Handle aggregate without GROUP BY that has no matching rows
	// SQL semantics: SELECT COUNT(*) FROM t WHERE 1=0 should return 1 row with COUNT=0
	if len(groupMap) == 0 && len(it.groupBy) == 0 {
		// Create an empty group for aggregates
		emptyGroup := &groupEntry{
			key:       "",
			keyValues: nil,
			rows:      nil, // No rows
		}
		groupMap[""] = emptyGroup
	}

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

// computeAggregates computes aggregate values for a group based on specified aggregate expressions
func (it *HashGroupByIterator) computeAggregates(group *groupEntry) {
	// If no explicit aggregates, add implicit COUNT(*)
	if len(it.aggregates) == 0 {
		group.aggregateValues = []types.Value{types.NewInt(int64(len(group.rows)))}
		return
	}

	// Initialize aggregate values slice
	group.aggregateValues = make([]types.Value, len(it.aggregates))

	// For each aggregate function, compute its value over the group's rows
	for i, agg := range it.aggregates {
		group.aggregateValues[i] = it.computeSingleAggregate(agg, group.rows)
	}
}

// computeSingleAggregate computes a single aggregate function over a set of rows
func (it *HashGroupByIterator) computeSingleAggregate(agg optimizer.AggregateExpr, rows [][]types.Value) types.Value {
	switch agg.FuncName {
	case "COUNT":
		// COUNT(*) if no arg, otherwise count non-null values
		if agg.Arg == nil {
			return types.NewInt(int64(len(rows)))
		}
		count := int64(0)
		for _, row := range rows {
			val, err := it.executor.evaluateExpr(agg.Arg, row, it.colMap)
			if err == nil && !val.IsNull() {
				count++
			}
		}
		return types.NewInt(count)

	case "SUM":
		if agg.Arg == nil {
			return types.NewNull()
		}
		sum := float64(0)
		hasValue := false
		for _, row := range rows {
			val, err := it.executor.evaluateExpr(agg.Arg, row, it.colMap)
			if err != nil || val.IsNull() {
				continue
			}
			hasValue = true
			switch val.Type() {
			case types.TypeInt, types.TypeSmallInt, types.TypeInt32, types.TypeBigInt, types.TypeSerial, types.TypeBigSerial:
				sum += float64(val.Int())
			case types.TypeFloat:
				sum += val.Float()
			}
		}
		if !hasValue {
			return types.NewNull()
		}
		return types.NewInt(int64(sum))

	case "AVG":
		if agg.Arg == nil {
			return types.NewNull()
		}
		sum := float64(0)
		count := int64(0)
		for _, row := range rows {
			val, err := it.executor.evaluateExpr(agg.Arg, row, it.colMap)
			if err != nil || val.IsNull() {
				continue
			}
			count++
			switch val.Type() {
			case types.TypeInt, types.TypeSmallInt, types.TypeInt32, types.TypeBigInt, types.TypeSerial, types.TypeBigSerial:
				sum += float64(val.Int())
			case types.TypeFloat:
				sum += val.Float()
			}
		}
		if count == 0 {
			return types.NewNull()
		}
		return types.NewFloat(sum / float64(count))

	case "MIN":
		if agg.Arg == nil {
			return types.NewNull()
		}
		var minVal types.Value
		hasValue := false
		for _, row := range rows {
			val, err := it.executor.evaluateExpr(agg.Arg, row, it.colMap)
			if err != nil || val.IsNull() {
				continue
			}
			if !hasValue || compareValuesForSort(val, minVal) < 0 {
				minVal = val
				hasValue = true
			}
		}
		if !hasValue {
			return types.NewNull()
		}
		return minVal

	case "MAX":
		if agg.Arg == nil {
			return types.NewNull()
		}
		var maxVal types.Value
		hasValue := false
		for _, row := range rows {
			val, err := it.executor.evaluateExpr(agg.Arg, row, it.colMap)
			if err != nil || val.IsNull() {
				continue
			}
			if !hasValue || compareValuesForSort(val, maxVal) > 0 {
				maxVal = val
				hasValue = true
			}
		}
		if !hasValue {
			return types.NewNull()
		}
		return maxVal

	default:
		return types.NewNull()
	}
}

// buildOutputRow builds an output row for a group (key values + aggregate results)
func (it *HashGroupByIterator) buildOutputRow(group *groupEntry) []types.Value {
	// Build row with: [group key values...] + [aggregate values...]
	result := make([]types.Value, len(group.keyValues)+len(group.aggregateValues))
	copy(result, group.keyValues)
	copy(result[len(group.keyValues):], group.aggregateValues)

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

func (it *HashGroupByIterator) Err() error {
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

func (it *CTEScanIterator) Err() error {
	return nil
}

func (it *CTEScanIterator) Close() {
	// Nothing to clean up - rows are just references to in-memory data
}

// DualIterator returns a single empty row for queries without FROM clause
// (e.g., SELECT 1+1, SELECT function())
type DualIterator struct {
	done bool
}

func (it *DualIterator) Next() bool {
	if it.done {
		return false
	}
	it.done = true
	return true
}

func (it *DualIterator) Value() []types.Value {
	// Return empty row - projection will add the computed values
	return []types.Value{}
}

func (it *DualIterator) Err() error {
	return nil
}

func (it *DualIterator) Close() {
	// Nothing to clean up
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
	exprIndex int // Index in expressions slice
	winFunc   *parser.WindowFunction
	funcName  string
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
				exprIndex: i,
				winFunc:   wf,
				funcName:  funcName,
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

func (it *WindowIterator) Err() error {
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

// WindowFunctionIterator computes window functions over a set of rows
// It materializes all input rows, sorts/partitions them, and computes window function values
type WindowFunctionIterator struct {
	executor     *Executor
	expressions  []parser.Expression      // All SELECT expressions
	windowFuncs  []*parser.WindowFunction // Window functions to compute
	colMap       map[string]int           // Input column mapping
	inputRows    [][]types.Value          // Materialized input rows
	computedRows [][]types.Value          // Output rows with computed window values
	index        int                      // Current row index
	prepared     bool                     // Whether we've computed window values
}

func NewWindowFunctionIterator(
	child RowIterator,
	expressions []parser.Expression,
	windowFuncs []*parser.WindowFunction,
	colMap map[string]int,
	executor *Executor,
) *WindowFunctionIterator {
	// Materialize all input rows
	var inputRows [][]types.Value
	for child.Next() {
		row := child.Value()
		rowCopy := make([]types.Value, len(row))
		copy(rowCopy, row)
		inputRows = append(inputRows, rowCopy)
	}
	child.Close()

	return &WindowFunctionIterator{
		executor:    executor,
		expressions: expressions,
		windowFuncs: windowFuncs,
		colMap:      colMap,
		inputRows:   inputRows,
		index:       -1,
		prepared:    false,
	}
}

func (it *WindowFunctionIterator) Next() bool {
	if !it.prepared {
		it.computeWindowValues()
		it.prepared = true
	}

	it.index++
	return it.index < len(it.computedRows)
}

func (it *WindowFunctionIterator) Value() []types.Value {
	if it.index >= 0 && it.index < len(it.computedRows) {
		return it.computedRows[it.index]
	}
	return nil
}

func (it *WindowFunctionIterator) Err() error {
	return nil
}

func (it *WindowFunctionIterator) Close() {
	// Nothing to close, data is in memory
}

// computeWindowValues computes window function values for all rows
func (it *WindowFunctionIterator) computeWindowValues() {
	if len(it.inputRows) == 0 {
		return
	}

	// For each window function, we need to:
	// 1. Sort rows by PARTITION BY + ORDER BY
	// 2. For each partition, compute the window function values
	// 3. Map these values back to the output rows

	// For simplicity, we'll handle one window function at a time
	// (In a full implementation, window functions with the same spec would be combined)

	// Pre-compute window function results for each row
	// Map from original row index to window function results
	windowResults := make([]map[int]types.Value, len(it.inputRows))
	for i := range windowResults {
		windowResults[i] = make(map[int]types.Value)
	}

	// Get sorted indices from the first window function (used for output order)
	var sortedIndices []int
	if len(it.windowFuncs) > 0 && it.windowFuncs[0].Over != nil {
		sortedIndices = it.sortRowsForWindow(it.windowFuncs[0].Over)
	} else {
		sortedIndices = make([]int, len(it.inputRows))
		for i := range sortedIndices {
			sortedIndices[i] = i
		}
	}

	// Process each window function
	for wfIdx, wf := range it.windowFuncs {
		it.computeSingleWindowFunction(wf, wfIdx, windowResults)
	}

	// Build output rows by evaluating all expressions
	// Output rows in the sorted order of the first window function
	it.computedRows = make([][]types.Value, len(it.inputRows))
	for outIdx, origIdx := range sortedIndices {
		inputRow := it.inputRows[origIdx]
		outputRow := make([]types.Value, len(it.expressions))
		for j, expr := range it.expressions {
			// Check if this is a window function
			if wf, ok := expr.(*parser.WindowFunction); ok {
				// Find which window function index this is
				for wfIdx, wf2 := range it.windowFuncs {
					if wf == wf2 {
						outputRow[j] = windowResults[origIdx][wfIdx]
						break
					}
				}
			} else {
				// Regular expression - evaluate against input row
				val, err := it.executor.evaluateExpr(expr, inputRow, it.colMap)
				if err != nil {
					outputRow[j] = types.NewNull()
				} else {
					outputRow[j] = val
				}
			}
		}
		it.computedRows[outIdx] = outputRow
	}
}

// computeSingleWindowFunction computes values for a single window function across all rows
func (it *WindowFunctionIterator) computeSingleWindowFunction(
	wf *parser.WindowFunction,
	wfIdx int,
	windowResults []map[int]types.Value,
) {
	// Get the function call inside the window function
	funcCall, ok := wf.Function.(*parser.FunctionCall)
	if !ok {
		// Not a function call, set NULL for all rows
		for i := range windowResults {
			windowResults[i][wfIdx] = types.NewNull()
		}
		return
	}

	funcName := funcCall.Name

	// Create sorted indices based on window spec
	sortedIndices := it.sortRowsForWindow(wf.Over)

	// Compute values based on function type
	switch funcName {
	case "LAG":
		it.computeLag(funcCall, wf.Over, sortedIndices, wfIdx, windowResults)
	case "LEAD":
		it.computeLead(funcCall, wf.Over, sortedIndices, wfIdx, windowResults)
	case "ROW_NUMBER":
		it.computeRowNumber(wf.Over, sortedIndices, wfIdx, windowResults)
	case "RANK":
		it.computeRank(wf.Over, sortedIndices, wfIdx, windowResults)
	case "DENSE_RANK":
		it.computeDenseRank(wf.Over, sortedIndices, wfIdx, windowResults)
	case "SUM":
		it.computeAggregateWindowFunc(funcCall, wf.Over, sortedIndices, wfIdx, windowResults, "SUM")
	case "AVG":
		it.computeAggregateWindowFunc(funcCall, wf.Over, sortedIndices, wfIdx, windowResults, "AVG")
	case "COUNT":
		it.computeAggregateWindowFunc(funcCall, wf.Over, sortedIndices, wfIdx, windowResults, "COUNT")
	case "MIN":
		it.computeAggregateWindowFunc(funcCall, wf.Over, sortedIndices, wfIdx, windowResults, "MIN")
	case "MAX":
		it.computeAggregateWindowFunc(funcCall, wf.Over, sortedIndices, wfIdx, windowResults, "MAX")
	default:
		// Unknown window function, set NULL
		for i := range windowResults {
			windowResults[i][wfIdx] = types.NewNull()
		}
	}
}

// sortRowsForWindow returns indices of rows sorted according to window spec
func (it *WindowFunctionIterator) sortRowsForWindow(spec *parser.WindowSpec) []int {
	indices := make([]int, len(it.inputRows))
	for i := range indices {
		indices[i] = i
	}

	if spec == nil {
		return indices
	}

	// Sort by PARTITION BY columns first, then ORDER BY columns
	sort.Slice(indices, func(a, b int) bool {
		rowA := it.inputRows[indices[a]]
		rowB := it.inputRows[indices[b]]

		// Compare by PARTITION BY
		for _, partExpr := range spec.PartitionBy {
			valA, _ := it.executor.evaluateExpr(partExpr, rowA, it.colMap)
			valB, _ := it.executor.evaluateExpr(partExpr, rowB, it.colMap)
			cmp := it.executor.compareValues(valA, valB)
			if cmp != 0 {
				return cmp < 0
			}
		}

		// Compare by ORDER BY
		for _, ob := range spec.OrderBy {
			valA, _ := it.executor.evaluateExpr(ob.Expr, rowA, it.colMap)
			valB, _ := it.executor.evaluateExpr(ob.Expr, rowB, it.colMap)
			cmp := it.executor.compareValues(valA, valB)
			if cmp != 0 {
				if ob.Direction == parser.OrderDesc {
					return cmp > 0
				}
				return cmp < 0
			}
		}

		return false
	})

	return indices
}

// computeLag computes LAG(expr, offset, default) for each row
func (it *WindowFunctionIterator) computeLag(
	funcCall *parser.FunctionCall,
	spec *parser.WindowSpec,
	sortedIndices []int,
	wfIdx int,
	windowResults []map[int]types.Value,
) {
	// LAG arguments: (expression, offset=1, default=NULL)
	if len(funcCall.Args) < 1 {
		for i := range windowResults {
			windowResults[i][wfIdx] = types.NewNull()
		}
		return
	}

	expr := funcCall.Args[0]
	offset := int64(1) // default offset
	var defaultVal types.Value = types.NewNull()

	// Get offset if provided
	if len(funcCall.Args) >= 2 {
		offsetVal, err := it.executor.evaluateExpr(funcCall.Args[1], nil, nil)
		if err == nil && types.IsIntegerType(offsetVal.Type()) {
			offset = offsetVal.Int()
		}
	}

	// Get default value if provided
	if len(funcCall.Args) >= 3 {
		defVal, err := it.executor.evaluateExpr(funcCall.Args[2], nil, nil)
		if err == nil {
			defaultVal = defVal
		}
	}

	// Process in partition groups
	partitionStart := 0
	for i := 0; i < len(sortedIndices); i++ {
		// Check if new partition starts
		isNewPartition := i == 0
		if !isNewPartition && spec != nil && len(spec.PartitionBy) > 0 {
			currRow := it.inputRows[sortedIndices[i]]
			prevRow := it.inputRows[sortedIndices[i-1]]
			for _, partExpr := range spec.PartitionBy {
				valCurr, _ := it.executor.evaluateExpr(partExpr, currRow, it.colMap)
				valPrev, _ := it.executor.evaluateExpr(partExpr, prevRow, it.colMap)
				if it.executor.compareValues(valCurr, valPrev) != 0 {
					isNewPartition = true
					break
				}
			}
		}

		if isNewPartition {
			partitionStart = i
		}

		// Compute LAG - look back 'offset' rows within partition
		origIdx := sortedIndices[i]
		posInPartition := i - partitionStart
		if posInPartition >= int(offset) {
			// Get value from 'offset' rows back in sorted order
			lagIdx := sortedIndices[i-int(offset)]
			lagRow := it.inputRows[lagIdx]
			val, err := it.executor.evaluateExpr(expr, lagRow, it.colMap)
			if err != nil {
				windowResults[origIdx][wfIdx] = defaultVal
			} else {
				windowResults[origIdx][wfIdx] = val
			}
		} else {
			// Not enough rows before - use default
			windowResults[origIdx][wfIdx] = defaultVal
		}
	}
}

// computeLead computes LEAD(expr, offset, default) for each row
func (it *WindowFunctionIterator) computeLead(
	funcCall *parser.FunctionCall,
	spec *parser.WindowSpec,
	sortedIndices []int,
	wfIdx int,
	windowResults []map[int]types.Value,
) {
	// LEAD arguments: (expression, offset=1, default=NULL)
	if len(funcCall.Args) < 1 {
		for i := range windowResults {
			windowResults[i][wfIdx] = types.NewNull()
		}
		return
	}

	expr := funcCall.Args[0]
	offset := int64(1) // default offset
	var defaultVal types.Value = types.NewNull()

	// Get offset if provided
	if len(funcCall.Args) >= 2 {
		offsetVal, err := it.executor.evaluateExpr(funcCall.Args[1], nil, nil)
		if err == nil && types.IsIntegerType(offsetVal.Type()) {
			offset = offsetVal.Int()
		}
	}

	// Get default value if provided
	if len(funcCall.Args) >= 3 {
		defVal, err := it.executor.evaluateExpr(funcCall.Args[2], nil, nil)
		if err == nil {
			defaultVal = defVal
		}
	}

	// First, compute partition boundaries
	partitionEnds := make([]int, len(sortedIndices))
	currentPartitionEnd := len(sortedIndices) - 1

	// Scan backwards to find partition ends
	for i := len(sortedIndices) - 1; i >= 0; i-- {
		if i == len(sortedIndices)-1 {
			currentPartitionEnd = i
		} else if spec != nil && len(spec.PartitionBy) > 0 {
			currRow := it.inputRows[sortedIndices[i]]
			nextRow := it.inputRows[sortedIndices[i+1]]
			isPartitionEnd := false
			for _, partExpr := range spec.PartitionBy {
				valCurr, _ := it.executor.evaluateExpr(partExpr, currRow, it.colMap)
				valNext, _ := it.executor.evaluateExpr(partExpr, nextRow, it.colMap)
				if it.executor.compareValues(valCurr, valNext) != 0 {
					isPartitionEnd = true
					break
				}
			}
			if isPartitionEnd {
				currentPartitionEnd = i
			}
		}
		partitionEnds[i] = currentPartitionEnd
	}

	// Now compute LEAD values
	for i := 0; i < len(sortedIndices); i++ {
		origIdx := sortedIndices[i]
		partEnd := partitionEnds[i]

		// LEAD looks forward 'offset' rows within partition
		leadPos := i + int(offset)
		if leadPos <= partEnd {
			leadIdx := sortedIndices[leadPos]
			leadRow := it.inputRows[leadIdx]
			val, err := it.executor.evaluateExpr(expr, leadRow, it.colMap)
			if err != nil {
				windowResults[origIdx][wfIdx] = defaultVal
			} else {
				windowResults[origIdx][wfIdx] = val
			}
		} else {
			// Beyond partition end - use default
			windowResults[origIdx][wfIdx] = defaultVal
		}
	}
}

// computeRowNumber computes ROW_NUMBER() for each row within partitions
func (it *WindowFunctionIterator) computeRowNumber(
	spec *parser.WindowSpec,
	sortedIndices []int,
	wfIdx int,
	windowResults []map[int]types.Value,
) {
	if len(sortedIndices) == 0 {
		return
	}

	// Track partition boundaries and assign row numbers
	rowNum := int64(0)
	for i := 0; i < len(sortedIndices); i++ {
		// Check if new partition starts
		isNewPartition := i == 0
		if !isNewPartition && spec != nil && len(spec.PartitionBy) > 0 {
			currRow := it.inputRows[sortedIndices[i]]
			prevRow := it.inputRows[sortedIndices[i-1]]
			for _, partExpr := range spec.PartitionBy {
				valCurr, _ := it.executor.evaluateExpr(partExpr, currRow, it.colMap)
				valPrev, _ := it.executor.evaluateExpr(partExpr, prevRow, it.colMap)
				if it.executor.compareValues(valCurr, valPrev) != 0 {
					isNewPartition = true
					break
				}
			}
		}

		if isNewPartition {
			rowNum = 1
		} else {
			rowNum++
		}

		origIdx := sortedIndices[i]
		windowResults[origIdx][wfIdx] = types.NewInt(rowNum)
	}
}

// computeRank computes RANK() for each row within partitions
// RANK assigns the same rank to rows with equal ORDER BY values, with gaps
func (it *WindowFunctionIterator) computeRank(
	spec *parser.WindowSpec,
	sortedIndices []int,
	wfIdx int,
	windowResults []map[int]types.Value,
) {
	if len(sortedIndices) == 0 {
		return
	}

	rowNum := int64(0)  // Position counter (always increments)
	rankVal := int64(0) // Current rank value (only changes when order values change)

	for i := 0; i < len(sortedIndices); i++ {
		// Check if new partition starts
		isNewPartition := i == 0
		if !isNewPartition && spec != nil && len(spec.PartitionBy) > 0 {
			currRow := it.inputRows[sortedIndices[i]]
			prevRow := it.inputRows[sortedIndices[i-1]]
			for _, partExpr := range spec.PartitionBy {
				valCurr, _ := it.executor.evaluateExpr(partExpr, currRow, it.colMap)
				valPrev, _ := it.executor.evaluateExpr(partExpr, prevRow, it.colMap)
				if it.executor.compareValues(valCurr, valPrev) != 0 {
					isNewPartition = true
					break
				}
			}
		}

		if isNewPartition {
			rowNum = 1
			rankVal = 1
		} else {
			rowNum++
			// Check if ORDER BY values changed from previous row
			orderChanged := false
			if spec != nil && len(spec.OrderBy) > 0 {
				currRow := it.inputRows[sortedIndices[i]]
				prevRow := it.inputRows[sortedIndices[i-1]]
				for _, ob := range spec.OrderBy {
					valCurr, _ := it.executor.evaluateExpr(ob.Expr, currRow, it.colMap)
					valPrev, _ := it.executor.evaluateExpr(ob.Expr, prevRow, it.colMap)
					if it.executor.compareValues(valCurr, valPrev) != 0 {
						orderChanged = true
						break
					}
				}
			}
			if orderChanged {
				rankVal = rowNum // Gap in ranks
			}
			// If order values are same, rankVal stays the same (tie)
		}

		origIdx := sortedIndices[i]
		windowResults[origIdx][wfIdx] = types.NewInt(rankVal)
	}
}

// computeDenseRank computes DENSE_RANK() for each row within partitions
// DENSE_RANK assigns the same rank to rows with equal ORDER BY values, without gaps
func (it *WindowFunctionIterator) computeDenseRank(
	spec *parser.WindowSpec,
	sortedIndices []int,
	wfIdx int,
	windowResults []map[int]types.Value,
) {
	if len(sortedIndices) == 0 {
		return
	}

	rankVal := int64(0) // Current rank value

	for i := 0; i < len(sortedIndices); i++ {
		// Check if new partition starts
		isNewPartition := i == 0
		if !isNewPartition && spec != nil && len(spec.PartitionBy) > 0 {
			currRow := it.inputRows[sortedIndices[i]]
			prevRow := it.inputRows[sortedIndices[i-1]]
			for _, partExpr := range spec.PartitionBy {
				valCurr, _ := it.executor.evaluateExpr(partExpr, currRow, it.colMap)
				valPrev, _ := it.executor.evaluateExpr(partExpr, prevRow, it.colMap)
				if it.executor.compareValues(valCurr, valPrev) != 0 {
					isNewPartition = true
					break
				}
			}
		}

		if isNewPartition {
			rankVal = 1
		} else {
			// Check if ORDER BY values changed from previous row
			orderChanged := false
			if spec != nil && len(spec.OrderBy) > 0 {
				currRow := it.inputRows[sortedIndices[i]]
				prevRow := it.inputRows[sortedIndices[i-1]]
				for _, ob := range spec.OrderBy {
					valCurr, _ := it.executor.evaluateExpr(ob.Expr, currRow, it.colMap)
					valPrev, _ := it.executor.evaluateExpr(ob.Expr, prevRow, it.colMap)
					if it.executor.compareValues(valCurr, valPrev) != 0 {
						orderChanged = true
						break
					}
				}
			}
			if orderChanged {
				rankVal++ // No gap, just increment by 1
			}
			// If order values are same, rankVal stays the same (tie)
		}

		origIdx := sortedIndices[i]
		windowResults[origIdx][wfIdx] = types.NewInt(rankVal)
	}
}

// computeAggregateWindowFunc computes aggregate window functions (SUM, AVG, COUNT, MIN, MAX) with frame support
func (it *WindowFunctionIterator) computeAggregateWindowFunc(
	funcCall *parser.FunctionCall,
	spec *parser.WindowSpec,
	sortedIndices []int,
	wfIdx int,
	windowResults []map[int]types.Value,
	aggFunc string,
) {
	if len(sortedIndices) == 0 {
		return
	}

	// Get the expression to aggregate (first argument, or * for COUNT(*))
	var aggExpr parser.Expression
	isCountStar := false
	if len(funcCall.Args) > 0 {
		// Check if it's COUNT(*)
		if lit, ok := funcCall.Args[0].(*parser.Literal); ok && lit.Value.Text() == "*" {
			isCountStar = true
		} else {
			aggExpr = funcCall.Args[0]
		}
	} else if aggFunc == "COUNT" {
		isCountStar = true
	}

	// Determine partition boundaries
	partitionStarts := make([]int, len(sortedIndices))
	partitionEnds := make([]int, len(sortedIndices))

	currentPartStart := 0
	for i := 0; i < len(sortedIndices); i++ {
		isNewPartition := i == 0
		if !isNewPartition && spec != nil && len(spec.PartitionBy) > 0 {
			currRow := it.inputRows[sortedIndices[i]]
			prevRow := it.inputRows[sortedIndices[i-1]]
			for _, partExpr := range spec.PartitionBy {
				valCurr, _ := it.executor.evaluateExpr(partExpr, currRow, it.colMap)
				valPrev, _ := it.executor.evaluateExpr(partExpr, prevRow, it.colMap)
				if it.executor.compareValues(valCurr, valPrev) != 0 {
					isNewPartition = true
					break
				}
			}
		}
		if isNewPartition {
			currentPartStart = i
		}
		partitionStarts[i] = currentPartStart
	}

	// Compute partition ends (scan backwards)
	currentPartEnd := len(sortedIndices) - 1
	for i := len(sortedIndices) - 1; i >= 0; i-- {
		if i == len(sortedIndices)-1 {
			currentPartEnd = i
		} else if spec != nil && len(spec.PartitionBy) > 0 {
			currRow := it.inputRows[sortedIndices[i]]
			nextRow := it.inputRows[sortedIndices[i+1]]
			isPartEnd := false
			for _, partExpr := range spec.PartitionBy {
				valCurr, _ := it.executor.evaluateExpr(partExpr, currRow, it.colMap)
				valNext, _ := it.executor.evaluateExpr(partExpr, nextRow, it.colMap)
				if it.executor.compareValues(valCurr, valNext) != 0 {
					isPartEnd = true
					break
				}
			}
			if isPartEnd {
				currentPartEnd = i
			}
		}
		partitionEnds[i] = currentPartEnd
	}

	// For each row, compute the aggregate over its frame
	for i := 0; i < len(sortedIndices); i++ {
		origIdx := sortedIndices[i]
		partStart := partitionStarts[i]
		partEnd := partitionEnds[i]

		// Determine frame bounds for this row
		frameStart, frameEnd := it.computeFrameBounds(spec, i, partStart, partEnd)

		// Compute aggregate over frame
		result := it.computeFrameAggregate(sortedIndices, frameStart, frameEnd, aggExpr, isCountStar, aggFunc)
		windowResults[origIdx][wfIdx] = result
	}
}

// computeFrameBounds computes the start and end indices of the frame for a given row
func (it *WindowFunctionIterator) computeFrameBounds(spec *parser.WindowSpec, rowIdx, partStart, partEnd int) (int, int) {
	// Default frame: RANGE BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW (when ORDER BY is present)
	// Without ORDER BY: RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
	frameStart := partStart
	frameEnd := rowIdx // Default to current row

	if spec == nil || spec.Frame == nil {
		// Default behavior
		if spec != nil && len(spec.OrderBy) > 0 {
			// With ORDER BY: UNBOUNDED PRECEDING to CURRENT ROW
			frameEnd = rowIdx
		} else {
			// Without ORDER BY: whole partition
			frameEnd = partEnd
		}
		return frameStart, frameEnd
	}

	frame := spec.Frame

	// Compute start bound
	if frame.StartBound != nil {
		switch frame.StartBound.Type {
		case parser.FrameBoundUnboundedPreceding:
			frameStart = partStart
		case parser.FrameBoundCurrentRow:
			frameStart = rowIdx
		case parser.FrameBoundPreceding:
			offset := int64(1)
			if frame.StartBound.Offset != nil {
				if lit, ok := frame.StartBound.Offset.(*parser.Literal); ok {
					offset = lit.Value.Int()
				}
			}
			frameStart = rowIdx - int(offset)
			if frameStart < partStart {
				frameStart = partStart
			}
		case parser.FrameBoundFollowing:
			offset := int64(1)
			if frame.StartBound.Offset != nil {
				if lit, ok := frame.StartBound.Offset.(*parser.Literal); ok {
					offset = lit.Value.Int()
				}
			}
			frameStart = rowIdx + int(offset)
			if frameStart > partEnd {
				frameStart = partEnd + 1 // Empty frame
			}
		}
	}

	// Compute end bound
	if frame.EndBound != nil {
		switch frame.EndBound.Type {
		case parser.FrameBoundUnboundedFollowing:
			frameEnd = partEnd
		case parser.FrameBoundCurrentRow:
			frameEnd = rowIdx
		case parser.FrameBoundPreceding:
			offset := int64(1)
			if frame.EndBound.Offset != nil {
				if lit, ok := frame.EndBound.Offset.(*parser.Literal); ok {
					offset = lit.Value.Int()
				}
			}
			frameEnd = rowIdx - int(offset)
			if frameEnd < partStart {
				frameEnd = partStart - 1 // Empty frame
			}
		case parser.FrameBoundFollowing:
			offset := int64(1)
			if frame.EndBound.Offset != nil {
				if lit, ok := frame.EndBound.Offset.(*parser.Literal); ok {
					offset = lit.Value.Int()
				}
			}
			frameEnd = rowIdx + int(offset)
			if frameEnd > partEnd {
				frameEnd = partEnd
			}
		}
	}

	return frameStart, frameEnd
}

// computeFrameAggregate computes an aggregate value over a frame
func (it *WindowFunctionIterator) computeFrameAggregate(
	sortedIndices []int,
	frameStart, frameEnd int,
	aggExpr parser.Expression,
	isCountStar bool,
	aggFunc string,
) types.Value {
	if frameStart > frameEnd {
		// Empty frame
		if aggFunc == "COUNT" {
			return types.NewInt(0)
		}
		return types.NewNull()
	}

	var sum float64
	var count int64
	var minVal, maxVal types.Value
	hasMin, hasMax := false, false

	for i := frameStart; i <= frameEnd; i++ {
		if i < 0 || i >= len(sortedIndices) {
			continue
		}
		origIdx := sortedIndices[i]
		row := it.inputRows[origIdx]

		if isCountStar {
			count++
			continue
		}

		if aggExpr == nil {
			continue
		}

		val, err := it.executor.evaluateExpr(aggExpr, row, it.colMap)
		if err != nil || val.IsNull() {
			continue
		}

		count++

		switch aggFunc {
		case "SUM", "AVG":
			if isIntegerTypeForSort(val.Type()) {
				sum += float64(val.Int())
			} else if val.Type() == types.TypeFloat {
				sum += val.Float()
			}
		case "MIN":
			if !hasMin || it.executor.compareValues(val, minVal) < 0 {
				minVal = val
				hasMin = true
			}
		case "MAX":
			if !hasMax || it.executor.compareValues(val, maxVal) > 0 {
				maxVal = val
				hasMax = true
			}
		case "COUNT":
			// count already incremented
		}
	}

	// Return appropriate result
	switch aggFunc {
	case "SUM":
		if count == 0 {
			return types.NewNull()
		}
		return types.NewInt(int64(sum))
	case "AVG":
		if count == 0 {
			return types.NewNull()
		}
		return types.NewFloat(sum / float64(count))
	case "COUNT":
		return types.NewInt(count)
	case "MIN":
		if !hasMin {
			return types.NewNull()
		}
		return minVal
	case "MAX":
		if !hasMax {
			return types.NewNull()
		}
		return maxVal
	default:
		return types.NewNull()
	}
}
