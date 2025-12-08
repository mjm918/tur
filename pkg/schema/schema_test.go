package schema

import (
	"testing"

	"tur/pkg/types"
)

func TestColumnDef_Basic(t *testing.T) {
	col := ColumnDef{
		Name:       "id",
		Type:       types.TypeInt,
		PrimaryKey: true,
		NotNull:    true,
	}

	if col.Name != "id" {
		t.Errorf("Name: got %q, want 'id'", col.Name)
	}
	if col.Type != types.TypeInt {
		t.Errorf("Type: got %v, want TypeInt", col.Type)
	}
	if !col.PrimaryKey {
		t.Error("PrimaryKey: expected true")
	}
	if !col.NotNull {
		t.Error("NotNull: expected true")
	}
}

func TestTableDef_Basic(t *testing.T) {
	table := TableDef{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: types.TypeInt, PrimaryKey: true, NotNull: true},
			{Name: "name", Type: types.TypeText, NotNull: true},
			{Name: "email", Type: types.TypeText},
		},
		RootPage: 2,
	}

	if table.Name != "users" {
		t.Errorf("Name: got %q, want 'users'", table.Name)
	}
	if len(table.Columns) != 3 {
		t.Fatalf("Columns: got %d, want 3", len(table.Columns))
	}
	if table.RootPage != 2 {
		t.Errorf("RootPage: got %d, want 2", table.RootPage)
	}
}

func TestTableDef_GetColumn(t *testing.T) {
	table := TableDef{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: types.TypeInt},
			{Name: "name", Type: types.TypeText},
		},
	}

	col, idx := table.GetColumn("id")
	if col == nil || idx != 0 {
		t.Errorf("GetColumn('id'): got (%v, %d)", col, idx)
	}

	col, idx = table.GetColumn("name")
	if col == nil || idx != 1 {
		t.Errorf("GetColumn('name'): got (%v, %d)", col, idx)
	}

	col, idx = table.GetColumn("unknown")
	if col != nil || idx != -1 {
		t.Errorf("GetColumn('unknown'): got (%v, %d), want (nil, -1)", col, idx)
	}
}

func TestTableDef_PrimaryKeyColumn(t *testing.T) {
	table := TableDef{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: types.TypeInt, PrimaryKey: true},
			{Name: "name", Type: types.TypeText},
		},
	}

	col, idx := table.PrimaryKeyColumn()
	if col == nil || idx != 0 || col.Name != "id" {
		t.Errorf("PrimaryKeyColumn: got (%v, %d)", col, idx)
	}

	// Table with no primary key
	tableNoPK := TableDef{
		Name: "logs",
		Columns: []ColumnDef{
			{Name: "message", Type: types.TypeText},
		},
	}

	col, idx = tableNoPK.PrimaryKeyColumn()
	if col != nil || idx != -1 {
		t.Errorf("PrimaryKeyColumn (no PK): got (%v, %d)", col, idx)
	}
}

func TestCatalog_CreateTable(t *testing.T) {
	catalog := NewCatalog()

	table := &TableDef{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: types.TypeInt, PrimaryKey: true},
		},
		RootPage: 2,
	}

	err := catalog.CreateTable(table)
	if err != nil {
		t.Fatalf("CreateTable: %v", err)
	}

	// Table should exist
	got := catalog.GetTable("users")
	if got == nil {
		t.Fatal("GetTable: table not found")
	}
	if got.Name != "users" {
		t.Errorf("GetTable: got name %q", got.Name)
	}
}

func TestCatalog_CreateTable_Duplicate(t *testing.T) {
	catalog := NewCatalog()

	table := &TableDef{Name: "users", Columns: []ColumnDef{}}
	catalog.CreateTable(table)

	err := catalog.CreateTable(table)
	if err == nil {
		t.Error("CreateTable: expected error for duplicate table")
	}
}

func TestCatalog_DropTable(t *testing.T) {
	catalog := NewCatalog()

	table := &TableDef{Name: "users", Columns: []ColumnDef{}}
	catalog.CreateTable(table)

	err := catalog.DropTable("users")
	if err != nil {
		t.Fatalf("DropTable: %v", err)
	}

	if catalog.GetTable("users") != nil {
		t.Error("DropTable: table still exists")
	}
}

func TestCatalog_DropTable_NotExists(t *testing.T) {
	catalog := NewCatalog()

	err := catalog.DropTable("nonexistent")
	if err == nil {
		t.Error("DropTable: expected error for nonexistent table")
	}
}

func TestCatalog_ListTables(t *testing.T) {
	catalog := NewCatalog()

	catalog.CreateTable(&TableDef{Name: "users"})
	catalog.CreateTable(&TableDef{Name: "orders"})
	catalog.CreateTable(&TableDef{Name: "products"})

	tables := catalog.ListTables()
	if len(tables) != 3 {
		t.Fatalf("ListTables: got %d tables, want 3", len(tables))
	}

	// Check that all tables are present (order may vary)
	found := make(map[string]bool)
	for _, name := range tables {
		found[name] = true
	}
	if !found["users"] || !found["orders"] || !found["products"] {
		t.Errorf("ListTables: missing tables, got %v", tables)
	}
}

// ========== Constraint System Tests ==========

func TestConstraintType_String(t *testing.T) {
	tests := []struct {
		ct   ConstraintType
		want string
	}{
		{ConstraintPrimaryKey, "PRIMARY KEY"},
		{ConstraintUnique, "UNIQUE"},
		{ConstraintNotNull, "NOT NULL"},
		{ConstraintCheck, "CHECK"},
		{ConstraintForeignKey, "FOREIGN KEY"},
		{ConstraintDefault, "DEFAULT"},
	}

	for _, tt := range tests {
		got := tt.ct.String()
		if got != tt.want {
			t.Errorf("ConstraintType(%d).String() = %q, want %q", tt.ct, got, tt.want)
		}
	}
}

func TestConstraint_Basic(t *testing.T) {
	// Test NOT NULL constraint
	notNullConstraint := Constraint{
		Type: ConstraintNotNull,
		Name: "users_name_nn",
	}

	if notNullConstraint.Type != ConstraintNotNull {
		t.Errorf("Type: got %v, want ConstraintNotNull", notNullConstraint.Type)
	}
	if notNullConstraint.Name != "users_name_nn" {
		t.Errorf("Name: got %q, want 'users_name_nn'", notNullConstraint.Name)
	}
}

func TestConstraint_Check(t *testing.T) {
	// Test CHECK constraint with expression
	checkConstraint := Constraint{
		Type:            ConstraintCheck,
		Name:            "users_age_check",
		CheckExpression: "age >= 0",
	}

	if checkConstraint.Type != ConstraintCheck {
		t.Errorf("Type: got %v, want ConstraintCheck", checkConstraint.Type)
	}
	if checkConstraint.CheckExpression != "age >= 0" {
		t.Errorf("CheckExpression: got %q, want 'age >= 0'", checkConstraint.CheckExpression)
	}
}

func TestConstraint_ForeignKey(t *testing.T) {
	// Test FOREIGN KEY constraint
	fkConstraint := Constraint{
		Type:          ConstraintForeignKey,
		Name:          "orders_user_fk",
		RefTable:      "users",
		RefColumn:     "id",
		OnDelete:      FKActionCascade,
		OnUpdate:      FKActionSetNull,
	}

	if fkConstraint.Type != ConstraintForeignKey {
		t.Errorf("Type: got %v, want ConstraintForeignKey", fkConstraint.Type)
	}
	if fkConstraint.RefTable != "users" {
		t.Errorf("RefTable: got %q, want 'users'", fkConstraint.RefTable)
	}
	if fkConstraint.RefColumn != "id" {
		t.Errorf("RefColumn: got %q, want 'id'", fkConstraint.RefColumn)
	}
	if fkConstraint.OnDelete != FKActionCascade {
		t.Errorf("OnDelete: got %v, want FKActionCascade", fkConstraint.OnDelete)
	}
	if fkConstraint.OnUpdate != FKActionSetNull {
		t.Errorf("OnUpdate: got %v, want FKActionSetNull", fkConstraint.OnUpdate)
	}
}

func TestForeignKeyAction_String(t *testing.T) {
	tests := []struct {
		action ForeignKeyAction
		want   string
	}{
		{FKActionNoAction, "NO ACTION"},
		{FKActionRestrict, "RESTRICT"},
		{FKActionCascade, "CASCADE"},
		{FKActionSetNull, "SET NULL"},
		{FKActionSetDefault, "SET DEFAULT"},
	}

	for _, tt := range tests {
		got := tt.action.String()
		if got != tt.want {
			t.Errorf("ForeignKeyAction(%d).String() = %q, want %q", tt.action, got, tt.want)
		}
	}
}

func TestColumnDef_Constraints(t *testing.T) {
	// Test ColumnDef with multiple constraints
	col := ColumnDef{
		Name: "email",
		Type: types.TypeText,
		Constraints: []Constraint{
			{Type: ConstraintNotNull, Name: "email_nn"},
			{Type: ConstraintUnique, Name: "email_unique"},
		},
	}

	if len(col.Constraints) != 2 {
		t.Fatalf("Constraints: got %d, want 2", len(col.Constraints))
	}
	if col.Constraints[0].Type != ConstraintNotNull {
		t.Errorf("Constraints[0].Type: got %v, want ConstraintNotNull", col.Constraints[0].Type)
	}
	if col.Constraints[1].Type != ConstraintUnique {
		t.Errorf("Constraints[1].Type: got %v, want ConstraintUnique", col.Constraints[1].Type)
	}
}

func TestColumnDef_HasConstraint(t *testing.T) {
	col := ColumnDef{
		Name: "id",
		Type: types.TypeInt,
		Constraints: []Constraint{
			{Type: ConstraintPrimaryKey},
			{Type: ConstraintNotNull},
		},
	}

	if !col.HasConstraint(ConstraintPrimaryKey) {
		t.Error("HasConstraint(ConstraintPrimaryKey): expected true")
	}
	if !col.HasConstraint(ConstraintNotNull) {
		t.Error("HasConstraint(ConstraintNotNull): expected true")
	}
	if col.HasConstraint(ConstraintUnique) {
		t.Error("HasConstraint(ConstraintUnique): expected false")
	}
	if col.HasConstraint(ConstraintForeignKey) {
		t.Error("HasConstraint(ConstraintForeignKey): expected false")
	}
}

func TestColumnDef_GetConstraint(t *testing.T) {
	col := ColumnDef{
		Name: "user_id",
		Type: types.TypeInt,
		Constraints: []Constraint{
			{Type: ConstraintNotNull},
			{Type: ConstraintForeignKey, Name: "fk_user", RefTable: "users", RefColumn: "id"},
		},
	}

	// Get existing constraint
	fk := col.GetConstraint(ConstraintForeignKey)
	if fk == nil {
		t.Fatal("GetConstraint(ConstraintForeignKey): expected non-nil")
	}
	if fk.RefTable != "users" {
		t.Errorf("RefTable: got %q, want 'users'", fk.RefTable)
	}

	// Get non-existing constraint
	check := col.GetConstraint(ConstraintCheck)
	if check != nil {
		t.Errorf("GetConstraint(ConstraintCheck): expected nil, got %v", check)
	}
}

func TestTableDef_TableConstraints(t *testing.T) {
	// Test table-level constraints
	table := TableDef{
		Name: "order_items",
		Columns: []ColumnDef{
			{Name: "order_id", Type: types.TypeInt},
			{Name: "product_id", Type: types.TypeInt},
			{Name: "quantity", Type: types.TypeInt},
		},
		TableConstraints: []TableConstraint{
			{
				Type:    ConstraintPrimaryKey,
				Name:    "pk_order_items",
				Columns: []string{"order_id", "product_id"},
			},
			{
				Type:            ConstraintCheck,
				Name:            "ck_quantity",
				CheckExpression: "quantity > 0",
			},
		},
	}

	if len(table.TableConstraints) != 2 {
		t.Fatalf("TableConstraints: got %d, want 2", len(table.TableConstraints))
	}

	pk := table.TableConstraints[0]
	if pk.Type != ConstraintPrimaryKey {
		t.Errorf("TableConstraints[0].Type: got %v, want ConstraintPrimaryKey", pk.Type)
	}
	if len(pk.Columns) != 2 {
		t.Errorf("TableConstraints[0].Columns: got %d columns, want 2", len(pk.Columns))
	}

	check := table.TableConstraints[1]
	if check.CheckExpression != "quantity > 0" {
		t.Errorf("TableConstraints[1].CheckExpression: got %q, want 'quantity > 0'", check.CheckExpression)
	}
}

func TestTableDef_GetTableConstraint(t *testing.T) {
	table := TableDef{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: types.TypeInt},
		},
		TableConstraints: []TableConstraint{
			{Type: ConstraintPrimaryKey, Name: "pk_users", Columns: []string{"id"}},
		},
	}

	pk := table.GetTableConstraint(ConstraintPrimaryKey)
	if pk == nil {
		t.Fatal("GetTableConstraint(ConstraintPrimaryKey): expected non-nil")
	}
	if pk.Name != "pk_users" {
		t.Errorf("Name: got %q, want 'pk_users'", pk.Name)
	}

	// Non-existing constraint
	fk := table.GetTableConstraint(ConstraintForeignKey)
	if fk != nil {
		t.Errorf("GetTableConstraint(ConstraintForeignKey): expected nil, got %v", fk)
	}
}

// ========== Index Definition Tests ==========

func TestIndexType_String(t *testing.T) {
	tests := []struct {
		it   IndexType
		want string
	}{
		{IndexTypeBTree, "BTREE"},
		{IndexTypeHNSW, "HNSW"},
	}

	for _, tt := range tests {
		got := tt.it.String()
		if got != tt.want {
			t.Errorf("IndexType(%d).String() = %q, want %q", tt.it, got, tt.want)
		}
	}
}

func TestIndexDef_Basic(t *testing.T) {
	// Test basic B-tree index
	idx := IndexDef{
		Name:      "idx_users_email",
		TableName: "users",
		Columns:   []string{"email"},
		Type:      IndexTypeBTree,
		Unique:    true,
		RootPage:  5,
	}

	if idx.Name != "idx_users_email" {
		t.Errorf("Name: got %q, want 'idx_users_email'", idx.Name)
	}
	if idx.TableName != "users" {
		t.Errorf("TableName: got %q, want 'users'", idx.TableName)
	}
	if len(idx.Columns) != 1 || idx.Columns[0] != "email" {
		t.Errorf("Columns: got %v, want ['email']", idx.Columns)
	}
	if idx.Type != IndexTypeBTree {
		t.Errorf("Type: got %v, want IndexTypeBTree", idx.Type)
	}
	if !idx.Unique {
		t.Error("Unique: expected true")
	}
	if idx.RootPage != 5 {
		t.Errorf("RootPage: got %d, want 5", idx.RootPage)
	}
}

func TestIndexDef_MultiColumn(t *testing.T) {
	// Test multi-column index
	idx := IndexDef{
		Name:      "idx_orders_customer_date",
		TableName: "orders",
		Columns:   []string{"customer_id", "order_date"},
		Type:      IndexTypeBTree,
		Unique:    false,
		RootPage:  10,
	}

	if len(idx.Columns) != 2 {
		t.Fatalf("Columns: got %d columns, want 2", len(idx.Columns))
	}
	if idx.Columns[0] != "customer_id" || idx.Columns[1] != "order_date" {
		t.Errorf("Columns: got %v, want ['customer_id', 'order_date']", idx.Columns)
	}
}
