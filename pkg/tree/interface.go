// pkg/tree/interface.go
// Package tree defines interfaces for B+ tree implementations.
// This allows the database to use either the traditional page-based btree
// or the new Copy-on-Write btree with lock-free reads.
package tree

// Tree is the interface for B+ tree operations.
// Both btree.BTree and cowbtree.CowBTree implement this interface.
type Tree interface {
	// Insert inserts or updates a key-value pair
	Insert(key, value []byte) error

	// Get retrieves the value for a key
	Get(key []byte) ([]byte, error)

	// Delete removes a key from the tree
	Delete(key []byte) error

	// Cursor creates a new cursor for iteration
	Cursor() Cursor
}

// Cursor is the interface for B+ tree iteration.
// Both btree.Cursor and cowbtree.Cursor implement this interface.
type Cursor interface {
	// First moves the cursor to the first entry
	First()

	// Last moves the cursor to the last entry
	Last()

	// Seek moves the cursor to the first entry >= key
	Seek(key []byte)

	// Next moves the cursor to the next entry
	Next()

	// Prev moves the cursor to the previous entry
	Prev()

	// Valid returns true if the cursor points to a valid entry
	Valid() bool

	// Key returns the current key (nil if not valid)
	Key() []byte

	// Value returns the current value (nil if not valid)
	Value() []byte

	// Close releases resources held by the cursor
	Close()
}

// TreeWithRootPage is an extension for page-based trees that track root pages.
// This is implemented by btree.BTree but not cowbtree.CowBTree.
type TreeWithRootPage interface {
	Tree
	RootPage() uint32
}

// TreeWithStats is an extension for trees that provide statistics.
type TreeWithStats interface {
	Tree
	KeyCount() int64
}

// SnapshotableTree is an extension for trees that support snapshots.
// This is implemented by cowbtree.CowBTree.
type SnapshotableTree interface {
	Tree
	Snapshot() TreeSnapshot
}

// TreeSnapshot represents a read-only snapshot of a tree.
type TreeSnapshot interface {
	Get(key []byte) ([]byte, error)
	Release()
}
