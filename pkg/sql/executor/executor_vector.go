// pkg/sql/executor/executor_vector.go
// Vector extension functions for the executor.
package executor

import (
	"encoding/binary"
	"fmt"
	"strings"

	"tur/pkg/hnsw"
	"tur/pkg/record"
	"tur/pkg/schema"
	"tur/pkg/sql/optimizer"
	"tur/pkg/sql/parser"
	"tur/pkg/types"
)

// executeVectorQuantize implements the vector_quantize(table_name, column_name) function.
// It builds an HNSW index on the specified VECTOR column.
// Returns the number of vectors indexed.
func (e *Executor) executeVectorQuantize(args []types.Value) (types.Value, error) {
	// Validate arguments: need exactly 2 string arguments
	if len(args) != 2 {
		return types.NewNull(), fmt.Errorf("vector_quantize requires 2 arguments: table_name, column_name")
	}

	// Extract table name (first argument)
	if args[0].Type() != types.TypeText {
		return types.NewNull(), fmt.Errorf("vector_quantize: table_name must be a string")
	}
	tableName := args[0].Text()

	// Extract column name (second argument)
	if args[1].Type() != types.TypeText {
		return types.NewNull(), fmt.Errorf("vector_quantize: column_name must be a string")
	}
	columnName := args[1].Text()

	// Look up table in catalog
	table := e.catalog.GetTable(tableName)
	if table == nil {
		return types.NewNull(), fmt.Errorf("vector_quantize: table %q not found", tableName)
	}

	// Find the column and validate it's a VECTOR type
	var vecColumn *schema.ColumnDef
	var colIndex int = -1
	for i, col := range table.Columns {
		if strings.EqualFold(col.Name, columnName) {
			vecColumn = &table.Columns[i]
			colIndex = i
			break
		}
	}
	if vecColumn == nil {
		return types.NewNull(), fmt.Errorf("vector_quantize: column %q not found in table %q", columnName, tableName)
	}
	if vecColumn.Type != types.TypeVector && vecColumn.Type != types.TypeBlob {
		return types.NewNull(), fmt.Errorf("vector_quantize: column %q is not a VECTOR type", columnName)
	}
	if vecColumn.VectorDim <= 0 {
		return types.NewNull(), fmt.Errorf("vector_quantize: column %q has invalid vector dimension", columnName)
	}

	// Scan table to collect all vectors
	vectors, rowIDs, err := e.scanVectorColumn(table, colIndex, vecColumn.VectorDim)
	if err != nil {
		return types.NewNull(), fmt.Errorf("vector_quantize: failed to scan table: %w", err)
	}

	if len(vectors) == 0 {
		return types.NewInt(0), nil
	}

	// Build HNSW index
	config := hnsw.DefaultConfig(vecColumn.VectorDim)
	idx := hnsw.NewIndex(config)

	for i, vec := range vectors {
		if err := idx.Insert(rowIDs[i], vec); err != nil {
			return types.NewNull(), fmt.Errorf("vector_quantize: failed to insert vector: %w", err)
		}
	}

	// Store index metadata in catalog
	indexName := fmt.Sprintf("hnsw_%s_%s", tableName, columnName)
	indexDef := &schema.IndexDef{
		Name:      indexName,
		TableName: tableName,
		Columns:   []string{columnName},
		Type:      schema.IndexTypeHNSW,
		Unique:    false,
		RootPage:  0, // HNSW indexes are in-memory for now
		HNSWParams: &schema.HNSWParams{
			M:              config.M,
			EfConstruction: config.EfConstruction,
		},
	}

	if err := e.catalog.CreateIndex(indexDef); err != nil {
		return types.NewNull(), fmt.Errorf("vector_quantize: failed to register index: %w", err)
	}

	// Store the HNSW index in executor's index map
	if e.hnswIndexes == nil {
		e.hnswIndexes = make(map[string]*hnsw.Index)
	}
	e.hnswIndexes[indexName] = idx

	return types.NewInt(int64(len(vectors))), nil
}

// scanVectorColumn scans a table and extracts all vectors from the specified column.
func (e *Executor) scanVectorColumn(table *schema.TableDef, colIndex int, dimension int) ([]*types.Vector, []int64, error) {
	var vectors []*types.Vector
	var rowIDs []int64

	// Get the B-tree for this table
	tree := e.trees[table.Name]
	if tree == nil {
		return nil, nil, fmt.Errorf("table B-tree not found for %q", table.Name)
	}

	// Create cursor to iterate through all rows
	cursor := tree.Cursor()
	cursor.First()

	for cursor.Valid() {
		key := cursor.Key()
		val := cursor.Value()

		// Key is stored as raw big-endian uint64 rowid
		if len(key) < 8 {
			cursor.Next()
			continue
		}
		rowID := int64(binary.BigEndian.Uint64(key))

		// Value is stored as record.Encode format
		values := record.Decode(val)
		if len(values) == 0 {
			cursor.Next()
			continue
		}

		// Extract vector from the specified column
		if colIndex < len(values) {
			colVal := values[colIndex]
			vec, err := extractVectorFromValue(colVal)
			if err == nil && vec != nil && vec.Dimension() == dimension {
				vectors = append(vectors, vec)
				rowIDs = append(rowIDs, rowID)
			}
		}

		cursor.Next()
	}

	return vectors, rowIDs, nil
}

// extractVectorFromValue extracts a Vector from a types.Value.
func extractVectorFromValue(val types.Value) (*types.Vector, error) {
	switch val.Type() {
	case types.TypeVector:
		return val.Vector(), nil
	case types.TypeBlob:
		return types.VectorFromBytes(val.Blob())
	default:
		return nil, fmt.Errorf("value is not a vector type")
	}
}

// executeTableFunction executes a table-valued function and returns a row iterator
func (e *Executor) executeTableFunction(node *optimizer.TableFunctionNode, cteData map[string]*cteResult) (RowIterator, []string, error) {
	switch strings.ToUpper(node.Name) {
	case "VECTOR_QUANTIZE_SCAN":
		return e.executeVectorQuantizeScan(node.Args)
	default:
		return nil, nil, fmt.Errorf("unknown table function: %s", node.Name)
	}
}

// executeVectorQuantizeScan implements the vector_quantize_scan(table, column, query_vec, k) function.
// Returns an iterator over (rowid, distance) pairs.
func (e *Executor) executeVectorQuantizeScan(args []parser.Expression) (RowIterator, []string, error) {
	// Validate arguments: need exactly 4 arguments
	if len(args) != 4 {
		return nil, nil, fmt.Errorf("vector_quantize_scan requires 4 arguments: table_name, column_name, query_vector, k")
	}

	// Evaluate arguments
	argValues := make([]types.Value, 4)
	for i, arg := range args {
		val, err := e.evaluateExpr(arg, nil, nil)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to evaluate argument %d: %w", i, err)
		}
		argValues[i] = val
	}

	// Extract table name (first argument)
	if argValues[0].Type() != types.TypeText {
		return nil, nil, fmt.Errorf("vector_quantize_scan: table_name must be a string")
	}
	tableName := argValues[0].Text()

	// Extract column name (second argument)
	if argValues[1].Type() != types.TypeText {
		return nil, nil, fmt.Errorf("vector_quantize_scan: column_name must be a string")
	}
	columnName := argValues[1].Text()

	// Extract query vector (third argument)
	queryVec, err := extractVectorFromValue(argValues[2])
	if err != nil {
		return nil, nil, fmt.Errorf("vector_quantize_scan: invalid query vector: %w", err)
	}

	// Extract k (fourth argument)
	if argValues[3].Type() != types.TypeInt {
		return nil, nil, fmt.Errorf("vector_quantize_scan: k must be an integer")
	}
	k := int(argValues[3].Int())
	if k <= 0 {
		return nil, nil, fmt.Errorf("vector_quantize_scan: k must be positive")
	}

	// Find the HNSW index for this table/column
	indexName := fmt.Sprintf("hnsw_%s_%s", tableName, columnName)
	idx := e.hnswIndexes[indexName]
	if idx == nil {
		return nil, nil, fmt.Errorf("vector_quantize_scan: no HNSW index found for %s.%s (run vector_quantize first)", tableName, columnName)
	}

	// Execute KNN search
	results, err := idx.SearchKNN(queryVec, k)
	if err != nil {
		return nil, nil, fmt.Errorf("vector_quantize_scan: search failed: %w", err)
	}

	// Build rows from search results
	rows := make([][]types.Value, len(results))
	for i, result := range results {
		rows[i] = []types.Value{
			types.NewInt(result.RowID),
			types.NewFloat(float64(result.Distance)),
		}
	}

	// Return iterator with columns
	columns := []string{"rowid", "distance"}
	return &SliceIterator{rows: rows, pos: 0}, columns, nil
}

// SliceIterator implements RowIterator over a slice of rows
type SliceIterator struct {
	rows [][]types.Value
	pos  int
	val  []types.Value
}

func (it *SliceIterator) Next() bool {
	if it.pos >= len(it.rows) {
		return false
	}
	it.val = it.rows[it.pos]
	it.pos++
	return true
}

func (it *SliceIterator) Value() []types.Value {
	return it.val
}

func (it *SliceIterator) Close() {
	// Nothing to release
}
