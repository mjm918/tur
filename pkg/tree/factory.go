// pkg/tree/factory.go
package tree

import (
	"tur/pkg/btree"
	"tur/pkg/pager"
)

// TreeType specifies which B+ tree implementation to use
type TreeType int

const (
	// TreeTypeClassic uses the traditional page-based B+ tree
	TreeTypeClassic TreeType = iota
	// TreeTypeCow uses the Copy-on-Write B+ tree with lock-free reads
	TreeTypeCow
)

// CowTreeCreator is a function type for creating CoW trees.
// This avoids import cycles by allowing cowbtree to register its creator.
type CowTreeCreator func(p *pager.Pager) (ExtendedTree, error)
type CowTreeAtPageCreator func(p *pager.Pager, pageNo uint32) (ExtendedTree, error)
type CowTreeOpener func(p *pager.Pager, rootPage uint32) (ExtendedTree, error)

var (
	// Registered CoW tree creators (set by cowbtree package init)
	cowTreeCreate       CowTreeCreator
	cowTreeCreateAtPage CowTreeAtPageCreator
	cowTreeOpen         CowTreeOpener
)

// RegisterCowTreeCreators registers the CoW tree creation functions.
// This should be called by the cowbtree package's init function.
func RegisterCowTreeCreators(create CowTreeCreator, createAt CowTreeAtPageCreator, open CowTreeOpener) {
	cowTreeCreate = create
	cowTreeCreateAtPage = createAt
	cowTreeOpen = open
}

// Factory creates B+ tree instances of the configured type
type Factory struct {
	treeType TreeType
	pager    *pager.Pager
}

// NewFactory creates a new tree factory
func NewFactory(p *pager.Pager, treeType TreeType) *Factory {
	return &Factory{
		treeType: treeType,
		pager:    p,
	}
}

// Create creates a new tree
func (f *Factory) Create() (ExtendedTree, error) {
	switch f.treeType {
	case TreeTypeCow:
		if cowTreeCreate == nil {
			// Fall back to classic btree if CoW not registered
			return f.createClassic()
		}
		return cowTreeCreate(f.pager)
	default:
		return f.createClassic()
	}
}

func (f *Factory) createClassic() (ExtendedTree, error) {
	tree, err := btree.Create(f.pager)
	if err != nil {
		return nil, err
	}
	return &btreeAdapter{tree, f.pager}, nil
}

// CreateAtPage creates a new tree at a specific page
func (f *Factory) CreateAtPage(pageNo uint32) (ExtendedTree, error) {
	switch f.treeType {
	case TreeTypeCow:
		if cowTreeCreateAtPage == nil {
			return f.createClassicAtPage(pageNo)
		}
		return cowTreeCreateAtPage(f.pager, pageNo)
	default:
		return f.createClassicAtPage(pageNo)
	}
}

func (f *Factory) createClassicAtPage(pageNo uint32) (ExtendedTree, error) {
	tree, err := btree.CreateAtPage(f.pager, pageNo)
	if err != nil {
		return nil, err
	}
	return &btreeAdapter{tree, f.pager}, nil
}

// Open opens an existing tree at the given root page
func (f *Factory) Open(rootPage uint32) (ExtendedTree, error) {
	switch f.treeType {
	case TreeTypeCow:
		if cowTreeOpen == nil {
			return f.openClassic(rootPage), nil
		}
		return cowTreeOpen(f.pager, rootPage)
	default:
		return f.openClassic(rootPage), nil
	}
}

func (f *Factory) openClassic(rootPage uint32) ExtendedTree {
	tree := btree.Open(f.pager, rootPage)
	return &btreeAdapter{tree, f.pager}
}

// TreeType returns the configured tree type
func (f *Factory) TreeType() TreeType {
	return f.treeType
}

// btreeAdapter adapts btree.BTree to the ExtendedTree interface
type btreeAdapter struct {
	tree  *btree.BTree
	pager *pager.Pager
}

func (a *btreeAdapter) Insert(key, value []byte) error {
	return a.tree.Insert(key, value)
}

func (a *btreeAdapter) Get(key []byte) ([]byte, error) {
	return a.tree.Get(key)
}

func (a *btreeAdapter) Delete(key []byte) error {
	return a.tree.Delete(key)
}

func (a *btreeAdapter) Cursor() Cursor {
	return &btreeCursorAdapter{a.tree.Cursor()}
}

func (a *btreeAdapter) RootPage() uint32 {
	return a.tree.RootPage()
}

func (a *btreeAdapter) Depth() int {
	return a.tree.Depth()
}

func (a *btreeAdapter) CollectPages() []uint32 {
	return a.tree.CollectPages()
}

// btreeCursorAdapter adapts btree.Cursor to the Cursor interface
type btreeCursorAdapter struct {
	cursor *btree.Cursor
}

func (c *btreeCursorAdapter) First()          { c.cursor.First() }
func (c *btreeCursorAdapter) Last()           { c.cursor.Last() }
func (c *btreeCursorAdapter) Seek(key []byte) { c.cursor.Seek(key) }
func (c *btreeCursorAdapter) Next()           { c.cursor.Next() }
func (c *btreeCursorAdapter) Prev()           { c.cursor.Prev() }
func (c *btreeCursorAdapter) Valid() bool     { return c.cursor.Valid() }
func (c *btreeCursorAdapter) Key() []byte     { return c.cursor.Key() }
func (c *btreeCursorAdapter) Value() []byte   { return c.cursor.Value() }
func (c *btreeCursorAdapter) Close()          { c.cursor.Close() }

// ExtendedTree interface with page-related methods
type ExtendedTree interface {
	Tree
	RootPage() uint32
	Depth() int
	CollectPages() []uint32
}

// AsExtended returns the tree as an ExtendedTree, or nil if not supported
func AsExtended(t Tree) ExtendedTree {
	if et, ok := t.(ExtendedTree); ok {
		return et
	}
	return nil
}
