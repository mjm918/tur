// pkg/pager/page_test.go
package pager

import "testing"

func TestPageCreate(t *testing.T) {
	p := NewPage(1, 4096)
	if p.PageNo() != 1 {
		t.Errorf("expected page number 1, got %d", p.PageNo())
	}
	if len(p.Data()) != 4096 {
		t.Errorf("expected 4096 bytes, got %d", len(p.Data()))
	}
}

func TestPageDirty(t *testing.T) {
	p := NewPage(1, 4096)
	if p.IsDirty() {
		t.Error("new page should not be dirty")
	}
	p.SetDirty(true)
	if !p.IsDirty() {
		t.Error("page should be dirty after SetDirty(true)")
	}
}

func TestPageReadWrite(t *testing.T) {
	p := NewPage(1, 4096)

	// Write some data
	data := []byte("hello world")
	copy(p.Data()[100:], data)
	p.SetDirty(true)

	// Read it back
	got := p.Data()[100 : 100+len(data)]
	if string(got) != "hello world" {
		t.Errorf("expected 'hello world', got '%s'", string(got))
	}
}

func TestPageType(t *testing.T) {
	p := NewPage(1, 4096)
	p.SetType(PageTypeBTreeLeaf)
	if p.Type() != PageTypeBTreeLeaf {
		t.Errorf("expected PageTypeBTreeLeaf, got %v", p.Type())
	}
}
