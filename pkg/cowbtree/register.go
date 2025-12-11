// pkg/cowbtree/register.go
package cowbtree

import (
	"tur/pkg/pager"
	"tur/pkg/tree"
)

func init() {
	// Register CoW tree creators with the tree factory
	tree.RegisterCowTreeCreators(
		createPersistentWrapper,
		createPersistentAtPageWrapper,
		openPersistentWrapper,
	)
}

// cowTreeWrapper wraps PersistentCowBTree to implement tree.ExtendedTree
type cowTreeWrapper struct {
	*PersistentCowBTree
}

func (w *cowTreeWrapper) Cursor() tree.Cursor {
	return &cowCursorWrapper{w.PersistentCowBTree.Cursor()}
}

// cowCursorWrapper wraps cowbtree.Cursor to implement tree.Cursor
type cowCursorWrapper struct {
	*Cursor
}

func createPersistentWrapper(p *pager.Pager) (tree.ExtendedTree, error) {
	pt, err := CreatePersistent(p)
	if err != nil {
		return nil, err
	}
	return &cowTreeWrapper{pt}, nil
}

func createPersistentAtPageWrapper(p *pager.Pager, pageNo uint32) (tree.ExtendedTree, error) {
	pt, err := CreatePersistentAtPage(p, pageNo)
	if err != nil {
		return nil, err
	}
	return &cowTreeWrapper{pt}, nil
}

func openPersistentWrapper(p *pager.Pager, rootPage uint32) (tree.ExtendedTree, error) {
	pt, err := OpenPersistent(p, rootPage)
	if err != nil {
		return nil, err
	}
	return &cowTreeWrapper{pt}, nil
}
