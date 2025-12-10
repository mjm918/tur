// pkg/turdb/rows_test.go
package turdb

import (
	"testing"

	"tur/pkg/types"
)

func TestRows_Design(t *testing.T) {
	// Test that Rows struct exists and can be created with result data
	columns := []string{"id", "name", "age"}
	rows := [][]types.Value{
		{types.NewInt(1), types.NewText("Alice"), types.NewInt(30)},
		{types.NewInt(2), types.NewText("Bob"), types.NewInt(25)},
	}

	r := NewRows(columns, rows)

	// Verify columns are accessible
	if len(r.Columns()) != 3 {
		t.Errorf("expected 3 columns, got %d", len(r.Columns()))
	}

	if r.Columns()[0] != "id" {
		t.Errorf("expected first column 'id', got %s", r.Columns()[0])
	}

	if r.Columns()[1] != "name" {
		t.Errorf("expected second column 'name', got %s", r.Columns()[1])
	}

	if r.Columns()[2] != "age" {
		t.Errorf("expected third column 'age', got %s", r.Columns()[2])
	}
}

func TestRows_Next(t *testing.T) {
	columns := []string{"id", "name"}
	rows := [][]types.Value{
		{types.NewInt(1), types.NewText("Alice")},
		{types.NewInt(2), types.NewText("Bob")},
		{types.NewInt(3), types.NewText("Charlie")},
	}

	r := NewRows(columns, rows)

	// First call to Next() should return true and position on first row
	if !r.Next() {
		t.Fatal("expected Next() to return true for first row")
	}

	// Second call should return true for second row
	if !r.Next() {
		t.Fatal("expected Next() to return true for second row")
	}

	// Third call should return true for third row
	if !r.Next() {
		t.Fatal("expected Next() to return true for third row")
	}

	// Fourth call should return false - no more rows
	if r.Next() {
		t.Fatal("expected Next() to return false when no more rows")
	}

	// Further calls should continue to return false
	if r.Next() {
		t.Fatal("expected Next() to return false on subsequent calls")
	}
}

func TestRows_Next_EmptyResultSet(t *testing.T) {
	columns := []string{"id"}
	rows := [][]types.Value{}

	r := NewRows(columns, rows)

	// Next() should return false immediately for empty result set
	if r.Next() {
		t.Fatal("expected Next() to return false for empty result set")
	}
}

func TestRows_Scan(t *testing.T) {
	columns := []string{"id", "name", "age", "score"}
	rows := [][]types.Value{
		{types.NewInt(1), types.NewText("Alice"), types.NewInt(30), types.NewFloat(95.5)},
		{types.NewInt(2), types.NewText("Bob"), types.NewInt(25), types.NewFloat(87.3)},
	}

	r := NewRows(columns, rows)

	// First row
	if !r.Next() {
		t.Fatal("expected Next() to return true")
	}

	var id int64
	var name string
	var age int64
	var score float64

	err := r.Scan(&id, &name, &age, &score)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if id != 1 {
		t.Errorf("expected id=1, got %d", id)
	}
	if name != "Alice" {
		t.Errorf("expected name='Alice', got %s", name)
	}
	if age != 30 {
		t.Errorf("expected age=30, got %d", age)
	}
	if score != 95.5 {
		t.Errorf("expected score=95.5, got %f", score)
	}

	// Second row
	if !r.Next() {
		t.Fatal("expected Next() to return true for second row")
	}

	err = r.Scan(&id, &name, &age, &score)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if id != 2 {
		t.Errorf("expected id=2, got %d", id)
	}
	if name != "Bob" {
		t.Errorf("expected name='Bob', got %s", name)
	}
}

func TestRows_Scan_ColumnCountMismatch(t *testing.T) {
	columns := []string{"id", "name"}
	rows := [][]types.Value{
		{types.NewInt(1), types.NewText("Alice")},
	}

	r := NewRows(columns, rows)
	r.Next()

	var id int64
	// Only one destination when there are two columns
	err := r.Scan(&id)
	if err == nil {
		t.Fatal("expected error for column count mismatch")
	}
}

func TestRows_Scan_BeforeNext(t *testing.T) {
	columns := []string{"id"}
	rows := [][]types.Value{
		{types.NewInt(1)},
	}

	r := NewRows(columns, rows)

	// Scan without calling Next first
	var id int64
	err := r.Scan(&id)
	if err == nil {
		t.Fatal("expected error when Scan is called before Next")
	}
}

func TestRows_Scan_NullValue(t *testing.T) {
	columns := []string{"id", "name"}
	rows := [][]types.Value{
		{types.NewInt(1), types.NewNull()},
	}

	r := NewRows(columns, rows)
	r.Next()

	var id int64
	var name *string // Pointer to handle NULL

	err := r.Scan(&id, &name)
	if err != nil {
		t.Fatalf("Scan failed: %v", err)
	}

	if id != 1 {
		t.Errorf("expected id=1, got %d", id)
	}
	if name != nil {
		t.Errorf("expected name to be nil for NULL value, got %v", name)
	}
}

func TestRows_ColumnInt(t *testing.T) {
	columns := []string{"id", "name", "age"}
	rows := [][]types.Value{
		{types.NewInt(42), types.NewText("Alice"), types.NewInt(30)},
	}

	r := NewRows(columns, rows)
	r.Next()

	// Test ColumnInt by index
	val, ok := r.ColumnInt(0)
	if !ok {
		t.Fatal("expected ColumnInt to return ok=true for integer column")
	}
	if val != 42 {
		t.Errorf("expected 42, got %d", val)
	}

	// Test ColumnInt for non-integer column
	_, ok = r.ColumnInt(1)
	if ok {
		t.Error("expected ColumnInt to return ok=false for text column")
	}
}

func TestRows_ColumnText(t *testing.T) {
	columns := []string{"id", "name"}
	rows := [][]types.Value{
		{types.NewInt(1), types.NewText("Hello World")},
	}

	r := NewRows(columns, rows)
	r.Next()

	val, ok := r.ColumnText(1)
	if !ok {
		t.Fatal("expected ColumnText to return ok=true for text column")
	}
	if val != "Hello World" {
		t.Errorf("expected 'Hello World', got %s", val)
	}

	// Test for non-text column
	_, ok = r.ColumnText(0)
	if ok {
		t.Error("expected ColumnText to return ok=false for integer column")
	}
}

func TestRows_ColumnFloat(t *testing.T) {
	columns := []string{"id", "score"}
	rows := [][]types.Value{
		{types.NewInt(1), types.NewFloat(3.14159)},
	}

	r := NewRows(columns, rows)
	r.Next()

	val, ok := r.ColumnFloat(1)
	if !ok {
		t.Fatal("expected ColumnFloat to return ok=true for float column")
	}
	if val != 3.14159 {
		t.Errorf("expected 3.14159, got %f", val)
	}
}

func TestRows_ColumnBlob(t *testing.T) {
	columns := []string{"id", "data"}
	data := []byte{0x01, 0x02, 0x03, 0x04}
	rows := [][]types.Value{
		{types.NewInt(1), types.NewBlob(data)},
	}

	r := NewRows(columns, rows)
	r.Next()

	val, ok := r.ColumnBlob(1)
	if !ok {
		t.Fatal("expected ColumnBlob to return ok=true for blob column")
	}
	if len(val) != 4 || val[0] != 0x01 {
		t.Errorf("expected blob data, got %v", val)
	}
}

func TestRows_ColumnIsNull(t *testing.T) {
	columns := []string{"id", "name"}
	rows := [][]types.Value{
		{types.NewInt(1), types.NewNull()},
	}

	r := NewRows(columns, rows)
	r.Next()

	if r.ColumnIsNull(0) {
		t.Error("expected ColumnIsNull to return false for non-null column")
	}
	if !r.ColumnIsNull(1) {
		t.Error("expected ColumnIsNull to return true for null column")
	}
}

func TestRows_ColumnValue(t *testing.T) {
	columns := []string{"id", "name"}
	rows := [][]types.Value{
		{types.NewInt(42), types.NewText("Test")},
	}

	r := NewRows(columns, rows)
	r.Next()

	val := r.ColumnValue(0)
	if val.Type() != types.TypeInt || val.Int() != 42 {
		t.Errorf("expected Int(42), got %v", val)
	}

	val = r.ColumnValue(1)
	if val.Type() != types.TypeText || val.Text() != "Test" {
		t.Errorf("expected Text('Test'), got %v", val)
	}
}

func TestRows_Column_OutOfBounds(t *testing.T) {
	columns := []string{"id"}
	rows := [][]types.Value{
		{types.NewInt(1)},
	}

	r := NewRows(columns, rows)
	r.Next()

	// Out of bounds access
	_, ok := r.ColumnInt(5)
	if ok {
		t.Error("expected ColumnInt to return ok=false for out of bounds index")
	}

	_, ok = r.ColumnText(-1)
	if ok {
		t.Error("expected ColumnText to return ok=false for negative index")
	}
}

func TestRows_Close(t *testing.T) {
	columns := []string{"id", "name"}
	rows := [][]types.Value{
		{types.NewInt(1), types.NewText("Alice")},
		{types.NewInt(2), types.NewText("Bob")},
	}

	r := NewRows(columns, rows)

	// Should be able to iterate
	if !r.Next() {
		t.Fatal("expected Next() to return true")
	}

	// Close the result set
	err := r.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}

	// Next should return false after close
	if r.Next() {
		t.Error("expected Next() to return false after Close")
	}

	// Scan should return error after close
	var id int64
	var name string
	err = r.Scan(&id, &name)
	if err == nil {
		t.Error("expected Scan to return error after Close")
	}

	// Double close should not panic or return error
	err = r.Close()
	if err != nil {
		t.Errorf("expected no error on double Close, got: %v", err)
	}
}

func TestRows_Close_MidIteration(t *testing.T) {
	columns := []string{"id"}
	rows := [][]types.Value{
		{types.NewInt(1)},
		{types.NewInt(2)},
		{types.NewInt(3)},
	}

	r := NewRows(columns, rows)

	// Read first row
	r.Next()
	val, _ := r.ColumnInt(0)
	if val != 1 {
		t.Errorf("expected 1, got %d", val)
	}

	// Close mid-iteration
	r.Close()

	// Should not be able to continue
	if r.Next() {
		t.Error("expected Next() to return false after Close mid-iteration")
	}
}

func TestRows_Err(t *testing.T) {
	columns := []string{"id"}
	rows := [][]types.Value{
		{types.NewInt(1)},
	}

	r := NewRows(columns, rows)

	// Initially no error
	if err := r.Err(); err != nil {
		t.Errorf("expected no error initially, got: %v", err)
	}

	// After iteration, should still have no error
	for r.Next() {
	}

	if err := r.Err(); err != nil {
		t.Errorf("expected no error after iteration, got: %v", err)
	}
}
