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

// ========== Index Catalog Tests ==========

func TestCatalog_CreateIndex(t *testing.T) {
	catalog := NewCatalog()

	// Create a table first
	table := &TableDef{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: types.TypeInt},
			{Name: "email", Type: types.TypeText},
		},
		RootPage: 2,
	}
	catalog.CreateTable(table)

	// Create an index
	idx := &IndexDef{
		Name:      "idx_users_email",
		TableName: "users",
		Columns:   []string{"email"},
		Type:      IndexTypeBTree,
		Unique:    true,
		RootPage:  5,
	}

	err := catalog.CreateIndex(idx)
	if err != nil {
		t.Fatalf("CreateIndex: %v", err)
	}

	// Index should exist
	got := catalog.GetIndex("idx_users_email")
	if got == nil {
		t.Fatal("GetIndex: index not found")
	}
	if got.Name != "idx_users_email" {
		t.Errorf("GetIndex: got name %q", got.Name)
	}
}

func TestCatalog_CreateIndex_Duplicate(t *testing.T) {
	catalog := NewCatalog()

	// Create a table first
	catalog.CreateTable(&TableDef{Name: "users", Columns: []ColumnDef{{Name: "email", Type: types.TypeText}}})

	idx := &IndexDef{Name: "idx_users_email", TableName: "users", Columns: []string{"email"}}
	catalog.CreateIndex(idx)

	// Try to create duplicate
	err := catalog.CreateIndex(idx)
	if err == nil {
		t.Error("CreateIndex: expected error for duplicate index")
	}
	if err != ErrIndexExists {
		t.Errorf("CreateIndex: got error %v, want ErrIndexExists", err)
	}
}

func TestCatalog_DropIndex(t *testing.T) {
	catalog := NewCatalog()

	catalog.CreateTable(&TableDef{Name: "users", Columns: []ColumnDef{{Name: "email", Type: types.TypeText}}})
	catalog.CreateIndex(&IndexDef{Name: "idx_users_email", TableName: "users", Columns: []string{"email"}})

	err := catalog.DropIndex("idx_users_email")
	if err != nil {
		t.Fatalf("DropIndex: %v", err)
	}

	if catalog.GetIndex("idx_users_email") != nil {
		t.Error("DropIndex: index still exists")
	}
}

func TestCatalog_DropIndex_NotExists(t *testing.T) {
	catalog := NewCatalog()

	err := catalog.DropIndex("nonexistent")
	if err == nil {
		t.Error("DropIndex: expected error for nonexistent index")
	}
	if err != ErrIndexNotFound {
		t.Errorf("DropIndex: got error %v, want ErrIndexNotFound", err)
	}
}

func TestCatalog_ListIndexes(t *testing.T) {
	catalog := NewCatalog()

	catalog.CreateTable(&TableDef{Name: "users", Columns: []ColumnDef{{Name: "email", Type: types.TypeText}, {Name: "name", Type: types.TypeText}}})
	catalog.CreateIndex(&IndexDef{Name: "idx_users_email", TableName: "users", Columns: []string{"email"}})
	catalog.CreateIndex(&IndexDef{Name: "idx_users_name", TableName: "users", Columns: []string{"name"}})

	indexes := catalog.ListIndexes()
	if len(indexes) != 2 {
		t.Fatalf("ListIndexes: got %d indexes, want 2", len(indexes))
	}

	// Check sorted order
	if indexes[0] != "idx_users_email" || indexes[1] != "idx_users_name" {
		t.Errorf("ListIndexes: got %v, want [idx_users_email, idx_users_name]", indexes)
	}
}

func TestCatalog_IndexCount(t *testing.T) {
	catalog := NewCatalog()

	catalog.CreateTable(&TableDef{Name: "users", Columns: []ColumnDef{{Name: "email", Type: types.TypeText}}})

	if catalog.IndexCount() != 0 {
		t.Errorf("IndexCount: got %d, want 0", catalog.IndexCount())
	}

	catalog.CreateIndex(&IndexDef{Name: "idx1", TableName: "users", Columns: []string{"email"}})
	if catalog.IndexCount() != 1 {
		t.Errorf("IndexCount: got %d, want 1", catalog.IndexCount())
	}

	catalog.CreateIndex(&IndexDef{Name: "idx2", TableName: "users", Columns: []string{"email"}})
	if catalog.IndexCount() != 2 {
		t.Errorf("IndexCount: got %d, want 2", catalog.IndexCount())
	}
}

// ========== HNSW Index Parameters Tests ==========

func TestIndexDef_HNSWParams(t *testing.T) {
	// Test HNSW index with parameters
	idx := IndexDef{
		Name:      "idx_embeddings_vec",
		TableName: "embeddings",
		Columns:   []string{"embedding"},
		Type:      IndexTypeHNSW,
		Unique:    false,
		RootPage:  10,
		HNSWParams: &HNSWParams{
			M:              16,
			EfConstruction: 200,
		},
	}

	if idx.Type != IndexTypeHNSW {
		t.Errorf("Type: got %v, want IndexTypeHNSW", idx.Type)
	}
	if idx.HNSWParams == nil {
		t.Fatal("HNSWParams: expected non-nil")
	}
	if idx.HNSWParams.M != 16 {
		t.Errorf("HNSWParams.M: got %d, want 16", idx.HNSWParams.M)
	}
	if idx.HNSWParams.EfConstruction != 200 {
		t.Errorf("HNSWParams.EfConstruction: got %d, want 200", idx.HNSWParams.EfConstruction)
	}
}

func TestIndexDef_HNSWParams_Defaults(t *testing.T) {
	// Test that B-tree index doesn't need HNSW params
	idx := IndexDef{
		Name:      "idx_users_email",
		TableName: "users",
		Columns:   []string{"email"},
		Type:      IndexTypeBTree,
	}

	if idx.HNSWParams != nil {
		t.Error("HNSWParams: expected nil for BTree index")
	}
}

func TestHNSWParams_DefaultValues(t *testing.T) {
	params := DefaultHNSWParams()

	// SQLite vec extension defaults: M=16, efConstruction=200
	if params.M != 16 {
		t.Errorf("M: got %d, want 16", params.M)
	}
	if params.EfConstruction != 200 {
		t.Errorf("EfConstruction: got %d, want 200", params.EfConstruction)
	}
}

// ========== Expression Index Tests ==========

func TestIndexDef_Expression_Single(t *testing.T) {
	// Test expression index on UPPER(name)
	idx := IndexDef{
		Name:        "idx_users_upper_name",
		TableName:   "users",
		Columns:     []string{},              // No plain columns
		Expressions: []string{"UPPER(name)"}, // Expression SQL
		Type:        IndexTypeBTree,
		Unique:      false,
		RootPage:    7,
	}

	if idx.Name != "idx_users_upper_name" {
		t.Errorf("Name: got %q, want 'idx_users_upper_name'", idx.Name)
	}
	if len(idx.Columns) != 0 {
		t.Errorf("Columns: got %v, want empty", idx.Columns)
	}
	if len(idx.Expressions) != 1 {
		t.Fatalf("Expressions: got %d, want 1", len(idx.Expressions))
	}
	if idx.Expressions[0] != "UPPER(name)" {
		t.Errorf("Expressions[0]: got %q, want 'UPPER(name)'", idx.Expressions[0])
	}
	if !idx.IsExpressionIndex() {
		t.Error("IsExpressionIndex: expected true")
	}
}

func TestIndexDef_Expression_Multiple(t *testing.T) {
	// Test index with multiple expressions
	idx := IndexDef{
		Name:        "idx_orders_computed",
		TableName:   "orders",
		Columns:     []string{},
		Expressions: []string{"price * quantity", "LOWER(status)"},
		Type:        IndexTypeBTree,
		Unique:      false,
		RootPage:    8,
	}

	if len(idx.Expressions) != 2 {
		t.Fatalf("Expressions: got %d, want 2", len(idx.Expressions))
	}
	if idx.Expressions[0] != "price * quantity" {
		t.Errorf("Expressions[0]: got %q, want 'price * quantity'", idx.Expressions[0])
	}
	if idx.Expressions[1] != "LOWER(status)" {
		t.Errorf("Expressions[1]: got %q, want 'LOWER(status)'", idx.Expressions[1])
	}
}

func TestIndexDef_Mixed_ColumnsAndExpressions(t *testing.T) {
	// Test index with both plain columns and expressions
	idx := IndexDef{
		Name:        "idx_users_mixed",
		TableName:   "users",
		Columns:     []string{"status"},
		Expressions: []string{"UPPER(name)"},
		Type:        IndexTypeBTree,
		Unique:      false,
		RootPage:    9,
	}

	if len(idx.Columns) != 1 || idx.Columns[0] != "status" {
		t.Errorf("Columns: got %v, want ['status']", idx.Columns)
	}
	if len(idx.Expressions) != 1 || idx.Expressions[0] != "UPPER(name)" {
		t.Errorf("Expressions: got %v, want ['UPPER(name)']", idx.Expressions)
	}
	if !idx.IsExpressionIndex() {
		t.Error("IsExpressionIndex: expected true for mixed index")
	}
}

func TestIndexDef_IsExpressionIndex_False(t *testing.T) {
	// Test plain column index returns false
	idx := IndexDef{
		Name:      "idx_users_email",
		TableName: "users",
		Columns:   []string{"email"},
		Type:      IndexTypeBTree,
	}

	if idx.IsExpressionIndex() {
		t.Error("IsExpressionIndex: expected false for plain column index")
	}
}

// ========== Index Lookup Tests ==========

func TestCatalog_GetIndexesForTable(t *testing.T) {
	catalog := NewCatalog()

	catalog.CreateTable(&TableDef{Name: "users", Columns: []ColumnDef{
		{Name: "id", Type: types.TypeInt},
		{Name: "email", Type: types.TypeText},
		{Name: "name", Type: types.TypeText},
	}})
	catalog.CreateTable(&TableDef{Name: "orders", Columns: []ColumnDef{
		{Name: "id", Type: types.TypeInt},
	}})

	catalog.CreateIndex(&IndexDef{Name: "idx_users_email", TableName: "users", Columns: []string{"email"}})
	catalog.CreateIndex(&IndexDef{Name: "idx_users_name", TableName: "users", Columns: []string{"name"}})
	catalog.CreateIndex(&IndexDef{Name: "idx_orders_id", TableName: "orders", Columns: []string{"id"}})

	// Get indexes for users table
	usersIndexes := catalog.GetIndexesForTable("users")
	if len(usersIndexes) != 2 {
		t.Fatalf("GetIndexesForTable('users'): got %d, want 2", len(usersIndexes))
	}

	// Check names are in sorted order
	if usersIndexes[0].Name != "idx_users_email" || usersIndexes[1].Name != "idx_users_name" {
		t.Errorf("GetIndexesForTable('users'): got %v, want sorted by name", usersIndexes)
	}

	// Get indexes for orders table
	ordersIndexes := catalog.GetIndexesForTable("orders")
	if len(ordersIndexes) != 1 {
		t.Fatalf("GetIndexesForTable('orders'): got %d, want 1", len(ordersIndexes))
	}

	// Get indexes for nonexistent table
	noneIndexes := catalog.GetIndexesForTable("nonexistent")
	if len(noneIndexes) != 0 {
		t.Errorf("GetIndexesForTable('nonexistent'): got %d, want 0", len(noneIndexes))
	}
}

func TestCatalog_GetIndexByColumn(t *testing.T) {
	catalog := NewCatalog()

	catalog.CreateTable(&TableDef{Name: "users", Columns: []ColumnDef{
		{Name: "id", Type: types.TypeInt},
		{Name: "email", Type: types.TypeText},
	}})

	catalog.CreateIndex(&IndexDef{Name: "idx_users_email", TableName: "users", Columns: []string{"email"}})

	// Find index by table and column
	idx := catalog.GetIndexByColumn("users", "email")
	if idx == nil {
		t.Fatal("GetIndexByColumn('users', 'email'): expected non-nil")
	}
	if idx.Name != "idx_users_email" {
		t.Errorf("Name: got %q, want 'idx_users_email'", idx.Name)
	}

	// Column without index
	idx = catalog.GetIndexByColumn("users", "id")
	if idx != nil {
		t.Errorf("GetIndexByColumn('users', 'id'): expected nil, got %v", idx)
	}

	// Nonexistent table
	idx = catalog.GetIndexByColumn("nonexistent", "col")
	if idx != nil {
		t.Errorf("GetIndexByColumn('nonexistent', 'col'): expected nil, got %v", idx)
	}
}

func TestCatalog_GetIndexByColumn_MultiColumn(t *testing.T) {
	catalog := NewCatalog()

	catalog.CreateTable(&TableDef{Name: "orders", Columns: []ColumnDef{
		{Name: "customer_id", Type: types.TypeInt},
		{Name: "order_date", Type: types.TypeText},
	}})

	catalog.CreateIndex(&IndexDef{
		Name:      "idx_orders_composite",
		TableName: "orders",
		Columns:   []string{"customer_id", "order_date"},
	})

	// First column of composite index should be found
	idx := catalog.GetIndexByColumn("orders", "customer_id")
	if idx == nil {
		t.Fatal("GetIndexByColumn('orders', 'customer_id'): expected non-nil for first column of composite index")
	}

	// Second column of composite index may not be directly indexable (only first column is usable for prefix matching)
	// But for simplicity, we still return the index if the column is part of it
	idx = catalog.GetIndexByColumn("orders", "order_date")
	if idx == nil {
		t.Fatal("GetIndexByColumn('orders', 'order_date'): expected non-nil for second column of composite index")
	}
}

// ========== ALTER TABLE Tests ==========

func TestCatalog_AddColumn(t *testing.T) {
	catalog := NewCatalog()
	catalog.CreateTable(&TableDef{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: types.TypeInt, PrimaryKey: true},
			{Name: "name", Type: types.TypeText},
		},
	})

	newCol := ColumnDef{Name: "email", Type: types.TypeText}
	err := catalog.AddColumn("users", newCol)
	if err != nil {
		t.Fatalf("AddColumn: %v", err)
	}

	// Verify column was added
	table := catalog.GetTable("users")
	if len(table.Columns) != 3 {
		t.Fatalf("Columns count = %d, want 3", len(table.Columns))
	}

	col, idx := table.GetColumn("email")
	if col == nil {
		t.Fatal("Column 'email' not found")
	}
	if idx != 2 {
		t.Errorf("Column index = %d, want 2", idx)
	}
	if col.Type != types.TypeText {
		t.Errorf("Column type = %v, want TypeText", col.Type)
	}
}

func TestCatalog_AddColumn_TableNotFound(t *testing.T) {
	catalog := NewCatalog()

	newCol := ColumnDef{Name: "email", Type: types.TypeText}
	err := catalog.AddColumn("nonexistent", newCol)
	if err == nil {
		t.Fatal("Expected error for non-existent table")
	}
	if err != ErrTableNotFound {
		t.Errorf("Error = %v, want ErrTableNotFound", err)
	}
}

func TestCatalog_AddColumn_DuplicateColumn(t *testing.T) {
	catalog := NewCatalog()
	catalog.CreateTable(&TableDef{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: types.TypeInt},
			{Name: "name", Type: types.TypeText},
		},
	})

	newCol := ColumnDef{Name: "name", Type: types.TypeText}
	err := catalog.AddColumn("users", newCol)
	if err == nil {
		t.Fatal("Expected error for duplicate column")
	}
	if err != ErrColumnExists {
		t.Errorf("Error = %v, want ErrColumnExists", err)
	}
}

func TestCatalog_DropColumn(t *testing.T) {
	catalog := NewCatalog()
	catalog.CreateTable(&TableDef{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: types.TypeInt},
			{Name: "name", Type: types.TypeText},
			{Name: "email", Type: types.TypeText},
		},
	})

	err := catalog.DropColumn("users", "email")
	if err != nil {
		t.Fatalf("DropColumn: %v", err)
	}

	// Verify column was removed
	table := catalog.GetTable("users")
	if len(table.Columns) != 2 {
		t.Fatalf("Columns count = %d, want 2", len(table.Columns))
	}

	col, _ := table.GetColumn("email")
	if col != nil {
		t.Error("Column 'email' should not exist after drop")
	}
}

func TestCatalog_DropColumn_TableNotFound(t *testing.T) {
	catalog := NewCatalog()

	err := catalog.DropColumn("nonexistent", "col")
	if err == nil {
		t.Fatal("Expected error for non-existent table")
	}
	if err != ErrTableNotFound {
		t.Errorf("Error = %v, want ErrTableNotFound", err)
	}
}

func TestCatalog_DropColumn_ColumnNotFound(t *testing.T) {
	catalog := NewCatalog()
	catalog.CreateTable(&TableDef{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: types.TypeInt},
		},
	})

	err := catalog.DropColumn("users", "nonexistent")
	if err == nil {
		t.Fatal("Expected error for non-existent column")
	}
	if err != ErrColumnNotFound {
		t.Errorf("Error = %v, want ErrColumnNotFound", err)
	}
}

func TestCatalog_RenameTable(t *testing.T) {
	catalog := NewCatalog()
	catalog.CreateTable(&TableDef{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: types.TypeInt},
		},
	})

	err := catalog.RenameTable("users", "customers")
	if err != nil {
		t.Fatalf("RenameTable: %v", err)
	}

	// Verify old name is gone
	if catalog.GetTable("users") != nil {
		t.Error("Old table name 'users' should not exist")
	}

	// Verify new name exists
	table := catalog.GetTable("customers")
	if table == nil {
		t.Fatal("New table name 'customers' not found")
	}
	if table.Name != "customers" {
		t.Errorf("Table.Name = %q, want 'customers'", table.Name)
	}
}

func TestCatalog_RenameTable_TableNotFound(t *testing.T) {
	catalog := NewCatalog()

	err := catalog.RenameTable("nonexistent", "newname")
	if err == nil {
		t.Fatal("Expected error for non-existent table")
	}
	if err != ErrTableNotFound {
		t.Errorf("Error = %v, want ErrTableNotFound", err)
	}
}

func TestCatalog_RenameTable_TargetExists(t *testing.T) {
	catalog := NewCatalog()
	catalog.CreateTable(&TableDef{
		Name: "users",
		Columns: []ColumnDef{{Name: "id", Type: types.TypeInt}},
	})
	catalog.CreateTable(&TableDef{
		Name: "customers",
		Columns: []ColumnDef{{Name: "id", Type: types.TypeInt}},
	})

	err := catalog.RenameTable("users", "customers")
	if err == nil {
		t.Fatal("Expected error when renaming to existing table")
	}
	if err != ErrTableExists {
		t.Errorf("Error = %v, want ErrTableExists", err)
	}
}

// View tests

func TestCatalog_CreateView(t *testing.T) {
	catalog := NewCatalog()

	view := &ViewDef{
		Name:       "active_users",
		SQL:        "SELECT id, name FROM users WHERE active = 1",
		Columns:    []string{"id", "name"},
	}

	err := catalog.CreateView(view)
	if err != nil {
		t.Fatalf("CreateView error: %v", err)
	}

	// Verify view was stored
	retrieved := catalog.GetView("active_users")
	if retrieved == nil {
		t.Fatal("GetView returned nil")
	}

	if retrieved.Name != "active_users" {
		t.Errorf("Name = %q, want 'active_users'", retrieved.Name)
	}

	if retrieved.SQL != "SELECT id, name FROM users WHERE active = 1" {
		t.Errorf("SQL = %q, unexpected", retrieved.SQL)
	}

	if len(retrieved.Columns) != 2 {
		t.Errorf("Columns count = %d, want 2", len(retrieved.Columns))
	}
}

func TestCatalog_CreateView_AlreadyExists(t *testing.T) {
	catalog := NewCatalog()

	view := &ViewDef{
		Name: "my_view",
		SQL:  "SELECT * FROM t",
	}

	_ = catalog.CreateView(view)

	err := catalog.CreateView(view)
	if err == nil {
		t.Fatal("Expected error for duplicate view")
	}
	if err != ErrViewExists {
		t.Errorf("Error = %v, want ErrViewExists", err)
	}
}

func TestCatalog_DropView(t *testing.T) {
	catalog := NewCatalog()

	view := &ViewDef{
		Name: "temp_view",
		SQL:  "SELECT 1",
	}
	_ = catalog.CreateView(view)

	err := catalog.DropView("temp_view")
	if err != nil {
		t.Fatalf("DropView error: %v", err)
	}

	// Verify view was removed
	if catalog.GetView("temp_view") != nil {
		t.Error("View still exists after DropView")
	}
}

func TestCatalog_DropView_NotFound(t *testing.T) {
	catalog := NewCatalog()

	err := catalog.DropView("nonexistent")
	if err == nil {
		t.Fatal("Expected error for non-existent view")
	}
	if err != ErrViewNotFound {
		t.Errorf("Error = %v, want ErrViewNotFound", err)
	}
}

func TestCatalog_ListViews(t *testing.T) {
	catalog := NewCatalog()

	_ = catalog.CreateView(&ViewDef{Name: "view_b", SQL: "SELECT 1"})
	_ = catalog.CreateView(&ViewDef{Name: "view_a", SQL: "SELECT 2"})
	_ = catalog.CreateView(&ViewDef{Name: "view_c", SQL: "SELECT 3"})

	views := catalog.ListViews()
	if len(views) != 3 {
		t.Fatalf("ListViews count = %d, want 3", len(views))
	}

	// Should be sorted
	if views[0] != "view_a" || views[1] != "view_b" || views[2] != "view_c" {
		t.Errorf("ListViews = %v, expected sorted order", views)
	}
}

// ========== Partial Index Tests ==========

func TestIndexDef_PartialIndex(t *testing.T) {
	// Test partial index with WHERE clause stored as SQL string
	idx := IndexDef{
		Name:            "idx_active_users_email",
		TableName:       "users",
		Columns:         []string{"email"},
		Type:            IndexTypeBTree,
		Unique:          true,
		RootPage:        5,
		WhereClause:     "active = 1",
	}

	if idx.Name != "idx_active_users_email" {
		t.Errorf("Name: got %q, want 'idx_active_users_email'", idx.Name)
	}
	if idx.WhereClause != "active = 1" {
		t.Errorf("WhereClause: got %q, want 'active = 1'", idx.WhereClause)
	}
	if !idx.IsPartial() {
		t.Error("IsPartial(): expected true for index with WHERE clause")
	}
}

func TestIndexDef_NonPartialIndex(t *testing.T) {
	// Test that regular index has no WHERE clause
	idx := IndexDef{
		Name:      "idx_users_email",
		TableName: "users",
		Columns:   []string{"email"},
		Type:      IndexTypeBTree,
	}

	if idx.WhereClause != "" {
		t.Errorf("WhereClause: got %q, want empty string", idx.WhereClause)
	}
	if idx.IsPartial() {
		t.Error("IsPartial(): expected false for index without WHERE clause")
	}
}

func TestCatalog_CreatePartialIndex(t *testing.T) {
	catalog := NewCatalog()

	// Create table first
	table := &TableDef{
		Name: "users",
		Columns: []ColumnDef{
			{Name: "id", Type: types.TypeInt},
			{Name: "email", Type: types.TypeText},
			{Name: "active", Type: types.TypeInt},
		},
	}
	_ = catalog.CreateTable(table)

	// Create partial index
	idx := &IndexDef{
		Name:        "idx_active_users_email",
		TableName:   "users",
		Columns:     []string{"email"},
		Type:        IndexTypeBTree,
		Unique:      true,
		WhereClause: "active = 1",
	}

	err := catalog.CreateIndex(idx)
	if err != nil {
		t.Fatalf("CreateIndex error: %v", err)
	}

	// Retrieve and verify
	retrieved := catalog.GetIndex("idx_active_users_email")
	if retrieved == nil {
		t.Fatal("GetIndex returned nil")
	}
	if retrieved.WhereClause != "active = 1" {
		t.Errorf("WhereClause: got %q, want 'active = 1'", retrieved.WhereClause)
	}
	if !retrieved.IsPartial() {
		t.Error("IsPartial(): expected true")
	}
}

// =============================================================================
// TRIGGER TESTS
// =============================================================================

func TestTriggerDef_Basic(t *testing.T) {
	trigger := TriggerDef{
		Name:      "audit_insert",
		TableName: "users",
		Timing:    TriggerBefore,
		Event:     TriggerInsert,
		SQL:       "CREATE TRIGGER audit_insert BEFORE INSERT ON users BEGIN INSERT INTO log VALUES (1); END",
	}

	if trigger.Name != "audit_insert" {
		t.Errorf("Name: got %q, want 'audit_insert'", trigger.Name)
	}
	if trigger.TableName != "users" {
		t.Errorf("TableName: got %q, want 'users'", trigger.TableName)
	}
	if trigger.Timing != TriggerBefore {
		t.Errorf("Timing: got %v, want TriggerBefore", trigger.Timing)
	}
	if trigger.Event != TriggerInsert {
		t.Errorf("Event: got %v, want TriggerInsert", trigger.Event)
	}
}

func TestCatalog_CreateTrigger(t *testing.T) {
	catalog := NewCatalog()

	trigger := &TriggerDef{
		Name:      "audit_insert",
		TableName: "users",
		Timing:    TriggerBefore,
		Event:     TriggerInsert,
		SQL:       "CREATE TRIGGER audit_insert BEFORE INSERT ON users BEGIN INSERT INTO log VALUES (1); END",
	}

	err := catalog.CreateTrigger(trigger)
	if err != nil {
		t.Fatalf("CreateTrigger error: %v", err)
	}

	// Duplicate should fail
	err = catalog.CreateTrigger(trigger)
	if err != ErrTriggerExists {
		t.Errorf("CreateTrigger duplicate: got %v, want ErrTriggerExists", err)
	}
}

func TestCatalog_GetTrigger(t *testing.T) {
	catalog := NewCatalog()

	trigger := &TriggerDef{
		Name:      "audit_insert",
		TableName: "users",
		Timing:    TriggerBefore,
		Event:     TriggerInsert,
	}
	_ = catalog.CreateTrigger(trigger)

	retrieved := catalog.GetTrigger("audit_insert")
	if retrieved == nil {
		t.Fatal("GetTrigger returned nil")
	}
	if retrieved.Name != "audit_insert" {
		t.Errorf("Name: got %q, want 'audit_insert'", retrieved.Name)
	}

	// Non-existent trigger
	if catalog.GetTrigger("nonexistent") != nil {
		t.Error("GetTrigger for nonexistent should return nil")
	}
}

func TestCatalog_DropTrigger(t *testing.T) {
	catalog := NewCatalog()

	trigger := &TriggerDef{
		Name:      "audit_insert",
		TableName: "users",
	}
	_ = catalog.CreateTrigger(trigger)

	err := catalog.DropTrigger("audit_insert")
	if err != nil {
		t.Fatalf("DropTrigger error: %v", err)
	}

	// Should be gone
	if catalog.GetTrigger("audit_insert") != nil {
		t.Error("Trigger should be deleted")
	}

	// Drop non-existent trigger
	err = catalog.DropTrigger("nonexistent")
	if err != ErrTriggerNotFound {
		t.Errorf("DropTrigger nonexistent: got %v, want ErrTriggerNotFound", err)
	}
}

func TestCatalog_GetTriggersForTable(t *testing.T) {
	catalog := NewCatalog()

	// Create multiple triggers for same table
	_ = catalog.CreateTrigger(&TriggerDef{Name: "before_insert", TableName: "users", Timing: TriggerBefore, Event: TriggerInsert})
	_ = catalog.CreateTrigger(&TriggerDef{Name: "after_insert", TableName: "users", Timing: TriggerAfter, Event: TriggerInsert})
	_ = catalog.CreateTrigger(&TriggerDef{Name: "before_update", TableName: "users", Timing: TriggerBefore, Event: TriggerUpdate})
	_ = catalog.CreateTrigger(&TriggerDef{Name: "other_trigger", TableName: "orders", Timing: TriggerAfter, Event: TriggerDelete})

	// Get all triggers for users table, BEFORE INSERT
	triggers := catalog.GetTriggersForTable("users", TriggerBefore, TriggerInsert)
	if len(triggers) != 1 {
		t.Fatalf("GetTriggersForTable BEFORE INSERT: got %d, want 1", len(triggers))
	}
	if triggers[0].Name != "before_insert" {
		t.Errorf("Trigger name: got %q, want 'before_insert'", triggers[0].Name)
	}

	// Get AFTER INSERT triggers
	triggers = catalog.GetTriggersForTable("users", TriggerAfter, TriggerInsert)
	if len(triggers) != 1 {
		t.Fatalf("GetTriggersForTable AFTER INSERT: got %d, want 1", len(triggers))
	}

	// No matching triggers
	triggers = catalog.GetTriggersForTable("users", TriggerAfter, TriggerDelete)
	if len(triggers) != 0 {
		t.Errorf("GetTriggersForTable AFTER DELETE: got %d, want 0", len(triggers))
	}
}

func TestCatalog_ListTriggers(t *testing.T) {
	catalog := NewCatalog()

	_ = catalog.CreateTrigger(&TriggerDef{Name: "zebra_trigger", TableName: "t1"})
	_ = catalog.CreateTrigger(&TriggerDef{Name: "alpha_trigger", TableName: "t2"})

	names := catalog.ListTriggers()
	if len(names) != 2 {
		t.Fatalf("ListTriggers: got %d, want 2", len(names))
	}
	// Should be sorted
	if names[0] != "alpha_trigger" || names[1] != "zebra_trigger" {
		t.Errorf("ListTriggers: got %v, want [alpha_trigger, zebra_trigger]", names)
	}
}

func TestCatalog_TriggerCount(t *testing.T) {
	catalog := NewCatalog()

	if catalog.TriggerCount() != 0 {
		t.Errorf("TriggerCount empty: got %d, want 0", catalog.TriggerCount())
	}

	_ = catalog.CreateTrigger(&TriggerDef{Name: "trigger1", TableName: "t1"})
	_ = catalog.CreateTrigger(&TriggerDef{Name: "trigger2", TableName: "t2"})

	if catalog.TriggerCount() != 2 {
		t.Errorf("TriggerCount: got %d, want 2", catalog.TriggerCount())
	}
}

func TestCatalog_GetTriggersForTable_MultipleSameTiming(t *testing.T) {
	catalog := NewCatalog()

	// Create TWO triggers for the same table/timing/event
	_ = catalog.CreateTrigger(&TriggerDef{Name: "t1", TableName: "users", Timing: TriggerBefore, Event: TriggerInsert})
	_ = catalog.CreateTrigger(&TriggerDef{Name: "t2", TableName: "users", Timing: TriggerBefore, Event: TriggerInsert})

	triggers := catalog.GetTriggersForTable("users", TriggerBefore, TriggerInsert)
	if len(triggers) != 2 {
		t.Errorf("Expected 2 triggers for same timing/event, got %d", len(triggers))
	}
}
