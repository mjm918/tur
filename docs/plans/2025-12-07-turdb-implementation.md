# TurDB Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a SQLite-compatible database in Go with native HNSW vector indexing for 10M-100M vectors.

**Architecture:** Layered design inspired by SQLite: OS/mmap layer → Pager (page cache + WAL) → Storage (B-tree + HNSW) → Query Processor (SQL parser + VDBE) → User API. MVCC for concurrent readers/writers.

**Tech Stack:** Pure Go, mmap for file access, no CGo dependencies.

---

## Phase 1: Foundation (Pager Layer)

### Task 1.1: Types Package - Core Value Types

**Files:**
- Create: `pkg/types/value.go`
- Test: `pkg/types/value_test.go`

**Step 1: Write the failing test**

```go
// pkg/types/value_test.go
package types

import "testing"

func TestValueNull(t *testing.T) {
    v := NewNull()
    if v.Type() != TypeNull {
        t.Errorf("expected TypeNull, got %v", v.Type())
    }
    if !v.IsNull() {
        t.Error("expected IsNull to return true")
    }
}

func TestValueInt(t *testing.T) {
    v := NewInt(42)
    if v.Type() != TypeInt {
        t.Errorf("expected TypeInt, got %v", v.Type())
    }
    if v.Int() != 42 {
        t.Errorf("expected 42, got %d", v.Int())
    }
}

func TestValueFloat(t *testing.T) {
    v := NewFloat(3.14)
    if v.Type() != TypeFloat {
        t.Errorf("expected TypeFloat, got %v", v.Type())
    }
    if v.Float() != 3.14 {
        t.Errorf("expected 3.14, got %f", v.Float())
    }
}

func TestValueText(t *testing.T) {
    v := NewText("hello")
    if v.Type() != TypeText {
        t.Errorf("expected TypeText, got %v", v.Type())
    }
    if v.Text() != "hello" {
        t.Errorf("expected 'hello', got %s", v.Text())
    }
}

func TestValueBlob(t *testing.T) {
    data := []byte{0x01, 0x02, 0x03}
    v := NewBlob(data)
    if v.Type() != TypeBlob {
        t.Errorf("expected TypeBlob, got %v", v.Type())
    }
    if string(v.Blob()) != string(data) {
        t.Errorf("expected %v, got %v", data, v.Blob())
    }
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/types/... -v`
Expected: FAIL - package not found or types not defined

**Step 3: Write minimal implementation**

```go
// pkg/types/value.go
package types

// ValueType represents the type of a database value
type ValueType int

const (
    TypeNull ValueType = iota
    TypeInt
    TypeFloat
    TypeText
    TypeBlob
    TypeVector
)

// Value represents a database value (like SQLite's Mem structure)
type Value struct {
    typ     ValueType
    intVal  int64
    floatVal float64
    textVal string
    blobVal []byte
}

func NewNull() Value {
    return Value{typ: TypeNull}
}

func NewInt(i int64) Value {
    return Value{typ: TypeInt, intVal: i}
}

func NewFloat(f float64) Value {
    return Value{typ: TypeFloat, floatVal: f}
}

func NewText(s string) Value {
    return Value{typ: TypeText, textVal: s}
}

func NewBlob(b []byte) Value {
    return Value{typ: TypeBlob, blobVal: b}
}

func (v Value) Type() ValueType { return v.typ }
func (v Value) IsNull() bool    { return v.typ == TypeNull }
func (v Value) Int() int64      { return v.intVal }
func (v Value) Float() float64  { return v.floatVal }
func (v Value) Text() string    { return v.textVal }
func (v Value) Blob() []byte    { return v.blobVal }
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/types/... -v`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/types/
git commit -m "feat(types): add core Value type with NULL, INT, FLOAT, TEXT, BLOB support"
```

---

### Task 1.2: Types Package - Vector Type

**Files:**
- Modify: `pkg/types/value.go`
- Create: `pkg/types/vector.go`
- Test: `pkg/types/vector_test.go`

**Step 1: Write the failing test**

```go
// pkg/types/vector_test.go
package types

import (
    "math"
    "testing"
)

func TestVectorCreate(t *testing.T) {
    data := []float32{0.1, 0.2, 0.3}
    v := NewVector(data)
    if v.Dimension() != 3 {
        t.Errorf("expected dimension 3, got %d", v.Dimension())
    }
    if v.Data()[0] != 0.1 {
        t.Errorf("expected 0.1, got %f", v.Data()[0])
    }
}

func TestVectorNormalize(t *testing.T) {
    v := NewVector([]float32{3, 4})
    v.Normalize()
    // magnitude should be 1.0
    mag := float32(math.Sqrt(float64(v.Data()[0]*v.Data()[0] + v.Data()[1]*v.Data()[1])))
    if math.Abs(float64(mag-1.0)) > 0.0001 {
        t.Errorf("expected magnitude 1.0, got %f", mag)
    }
}

func TestVectorCosineDistance(t *testing.T) {
    v1 := NewVector([]float32{1, 0})
    v2 := NewVector([]float32{0, 1})
    v1.Normalize()
    v2.Normalize()
    dist := v1.CosineDistance(v2)
    // orthogonal vectors: cosine similarity = 0, distance = 1
    if math.Abs(float64(dist-1.0)) > 0.0001 {
        t.Errorf("expected distance 1.0, got %f", dist)
    }
}

func TestVectorCosineDistanceSame(t *testing.T) {
    v1 := NewVector([]float32{1, 2, 3})
    v2 := NewVector([]float32{1, 2, 3})
    v1.Normalize()
    v2.Normalize()
    dist := v1.CosineDistance(v2)
    // same vectors: cosine similarity = 1, distance = 0
    if math.Abs(float64(dist)) > 0.0001 {
        t.Errorf("expected distance 0.0, got %f", dist)
    }
}

func TestVectorToFromBytes(t *testing.T) {
    original := NewVector([]float32{1.5, 2.5, 3.5})
    bytes := original.ToBytes()
    restored, err := VectorFromBytes(bytes)
    if err != nil {
        t.Fatalf("unexpected error: %v", err)
    }
    if restored.Dimension() != original.Dimension() {
        t.Errorf("dimension mismatch")
    }
    for i := 0; i < original.Dimension(); i++ {
        if original.Data()[i] != restored.Data()[i] {
            t.Errorf("data mismatch at %d", i)
        }
    }
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/types/... -v -run Vector`
Expected: FAIL - Vector type not defined

**Step 3: Write minimal implementation**

```go
// pkg/types/vector.go
package types

import (
    "encoding/binary"
    "errors"
    "math"
)

// Vector represents a float32 vector for similarity search
type Vector struct {
    data []float32
}

// NewVector creates a new vector from float32 slice
func NewVector(data []float32) *Vector {
    // Copy to avoid external mutation
    copied := make([]float32, len(data))
    copy(copied, data)
    return &Vector{data: copied}
}

// Dimension returns the number of dimensions
func (v *Vector) Dimension() int {
    return len(v.data)
}

// Data returns the underlying float32 slice
func (v *Vector) Data() []float32 {
    return v.data
}

// Normalize normalizes the vector to unit length (for cosine similarity)
func (v *Vector) Normalize() {
    var sum float32
    for _, val := range v.data {
        sum += val * val
    }
    if sum == 0 {
        return
    }
    mag := float32(math.Sqrt(float64(sum)))
    for i := range v.data {
        v.data[i] /= mag
    }
}

// CosineDistance returns 1 - dot_product (assumes normalized vectors)
func (v *Vector) CosineDistance(other *Vector) float32 {
    if len(v.data) != len(other.data) {
        return 2.0 // max distance for mismatched dimensions
    }
    var dot float32
    for i := range v.data {
        dot += v.data[i] * other.data[i]
    }
    return 1.0 - dot
}

// ToBytes serializes vector to bytes (little-endian float32)
func (v *Vector) ToBytes() []byte {
    buf := make([]byte, 4+len(v.data)*4) // 4 bytes for dimension + data
    binary.LittleEndian.PutUint32(buf[0:4], uint32(len(v.data)))
    for i, val := range v.data {
        binary.LittleEndian.PutUint32(buf[4+i*4:], math.Float32bits(val))
    }
    return buf
}

// VectorFromBytes deserializes vector from bytes
func VectorFromBytes(data []byte) (*Vector, error) {
    if len(data) < 4 {
        return nil, errors.New("invalid vector data: too short")
    }
    dim := binary.LittleEndian.Uint32(data[0:4])
    if len(data) < 4+int(dim)*4 {
        return nil, errors.New("invalid vector data: incomplete")
    }
    vec := make([]float32, dim)
    for i := range vec {
        vec[i] = math.Float32frombits(binary.LittleEndian.Uint32(data[4+i*4:]))
    }
    return &Vector{data: vec}, nil
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/types/... -v -run Vector`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/types/
git commit -m "feat(types): add Vector type with normalization and cosine distance"
```

---

### Task 1.3: Internal Encoding - Varint

**Files:**
- Create: `internal/encoding/varint.go`
- Test: `internal/encoding/varint_test.go`

**Step 1: Write the failing test**

```go
// internal/encoding/varint_test.go
package encoding

import "testing"

func TestPutVarint(t *testing.T) {
    tests := []struct {
        value    uint64
        expected []byte
    }{
        {0, []byte{0x00}},
        {1, []byte{0x01}},
        {127, []byte{0x7f}},
        {128, []byte{0x81, 0x00}},
        {255, []byte{0x81, 0x7f}},
        {16383, []byte{0xff, 0x7f}},
        {16384, []byte{0x81, 0x80, 0x00}},
    }
    for _, tt := range tests {
        buf := make([]byte, 10)
        n := PutVarint(buf, tt.value)
        if n != len(tt.expected) {
            t.Errorf("PutVarint(%d): expected %d bytes, got %d", tt.value, len(tt.expected), n)
        }
        for i := 0; i < n; i++ {
            if buf[i] != tt.expected[i] {
                t.Errorf("PutVarint(%d): byte %d expected %02x, got %02x", tt.value, i, tt.expected[i], buf[i])
            }
        }
    }
}

func TestGetVarint(t *testing.T) {
    tests := []struct {
        input    []byte
        expected uint64
        size     int
    }{
        {[]byte{0x00}, 0, 1},
        {[]byte{0x01}, 1, 1},
        {[]byte{0x7f}, 127, 1},
        {[]byte{0x81, 0x00}, 128, 2},
        {[]byte{0x81, 0x7f}, 255, 2},
        {[]byte{0xff, 0x7f}, 16383, 2},
        {[]byte{0x81, 0x80, 0x00}, 16384, 3},
    }
    for _, tt := range tests {
        val, n := GetVarint(tt.input)
        if val != tt.expected {
            t.Errorf("GetVarint(%v): expected %d, got %d", tt.input, tt.expected, val)
        }
        if n != tt.size {
            t.Errorf("GetVarint(%v): expected size %d, got %d", tt.input, tt.size, n)
        }
    }
}

func TestVarintRoundTrip(t *testing.T) {
    values := []uint64{0, 1, 127, 128, 255, 256, 16383, 16384, 1<<20, 1<<30, 1<<40}
    for _, v := range values {
        buf := make([]byte, 10)
        n := PutVarint(buf, v)
        got, m := GetVarint(buf[:n])
        if got != v || m != n {
            t.Errorf("roundtrip failed for %d: got %d, sizes %d vs %d", v, got, n, m)
        }
    }
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./internal/encoding/... -v`
Expected: FAIL - package not found

**Step 3: Write minimal implementation**

```go
// internal/encoding/varint.go
package encoding

// PutVarint encodes a uint64 as a variable-length integer (SQLite format)
// Returns the number of bytes written
// Format: each byte has 7 bits of data, high bit set if more bytes follow
func PutVarint(buf []byte, v uint64) int {
    if v <= 127 {
        buf[0] = byte(v)
        return 1
    }

    // Calculate how many bytes we need
    n := 0
    temp := v
    for temp > 0 {
        n++
        temp >>= 7
    }

    // Write bytes in big-endian order with continuation bits
    for i := n - 1; i >= 0; i-- {
        b := byte(v >> (uint(i) * 7) & 0x7f)
        if i > 0 {
            b |= 0x80 // set continuation bit
        }
        buf[n-1-i] = b
    }
    return n
}

// GetVarint decodes a variable-length integer from buf
// Returns the value and the number of bytes read
func GetVarint(buf []byte) (uint64, int) {
    var v uint64
    var n int
    for n = 0; n < len(buf) && n < 9; n++ {
        b := buf[n]
        v = (v << 7) | uint64(b&0x7f)
        if b&0x80 == 0 {
            return v, n + 1
        }
    }
    return v, n
}

// VarintLen returns the number of bytes needed to encode v
func VarintLen(v uint64) int {
    if v <= 127 {
        return 1
    }
    n := 0
    for v > 0 {
        n++
        v >>= 7
    }
    return n
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./internal/encoding/... -v`
Expected: PASS

**Step 5: Commit**

```bash
git add internal/encoding/
git commit -m "feat(encoding): add varint encoding/decoding (SQLite-compatible)"
```

---

### Task 1.4: Pager - Page Structure

**Files:**
- Create: `pkg/pager/page.go`
- Test: `pkg/pager/page_test.go`

**Step 1: Write the failing test**

```go
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
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/pager/... -v`
Expected: FAIL - package not found

**Step 3: Write minimal implementation**

```go
// pkg/pager/page.go
package pager

import "sync"

// PageType identifies the type of data stored in a page
type PageType byte

const (
    PageTypeUnknown       PageType = 0x00
    PageTypeBTreeInterior PageType = 0x01
    PageTypeBTreeLeaf     PageType = 0x02
    PageTypeHNSWNode      PageType = 0x10
    PageTypeHNSWMeta      PageType = 0x11
    PageTypeOverflow      PageType = 0x20
    PageTypeFreeList      PageType = 0x30
)

// Page represents an in-memory database page
type Page struct {
    mu      sync.RWMutex
    pageNo  uint32
    data    []byte
    dirty   bool
    pinned  int // reference count
}

// NewPage creates a new page with the given page number and size
func NewPage(pageNo uint32, pageSize int) *Page {
    return &Page{
        pageNo: pageNo,
        data:   make([]byte, pageSize),
    }
}

// NewPageWithData creates a page with existing data (for loading from disk)
func NewPageWithData(pageNo uint32, data []byte) *Page {
    return &Page{
        pageNo: pageNo,
        data:   data,
    }
}

// PageNo returns the page number
func (p *Page) PageNo() uint32 {
    return p.pageNo
}

// Data returns the raw page data (caller should hold appropriate lock)
func (p *Page) Data() []byte {
    return p.data
}

// IsDirty returns whether the page has been modified
func (p *Page) IsDirty() bool {
    p.mu.RLock()
    defer p.mu.RUnlock()
    return p.dirty
}

// SetDirty marks the page as dirty (modified)
func (p *Page) SetDirty(dirty bool) {
    p.mu.Lock()
    defer p.mu.Unlock()
    p.dirty = dirty
}

// Type returns the page type (stored in first byte)
func (p *Page) Type() PageType {
    if len(p.data) == 0 {
        return PageTypeUnknown
    }
    return PageType(p.data[0])
}

// SetType sets the page type (stored in first byte)
func (p *Page) SetType(t PageType) {
    if len(p.data) > 0 {
        p.data[0] = byte(t)
    }
}

// Pin increments the reference count
func (p *Page) Pin() {
    p.mu.Lock()
    defer p.mu.Unlock()
    p.pinned++
}

// Unpin decrements the reference count
func (p *Page) Unpin() {
    p.mu.Lock()
    defer p.mu.Unlock()
    if p.pinned > 0 {
        p.pinned--
    }
}

// IsPinned returns whether the page is currently in use
func (p *Page) IsPinned() bool {
    p.mu.RLock()
    defer p.mu.RUnlock()
    return p.pinned > 0
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/pager/... -v`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/pager/
git commit -m "feat(pager): add Page structure with type, dirty flag, and pinning"
```

---

### Task 1.5: Pager - Mmap File Access

**Files:**
- Create: `pkg/pager/mmap.go`
- Test: `pkg/pager/mmap_test.go`

**Step 1: Write the failing test**

```go
// pkg/pager/mmap_test.go
package pager

import (
    "os"
    "path/filepath"
    "testing"
)

func TestMmapFileCreate(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "test.db")

    mf, err := OpenMmapFile(path, 4096)
    if err != nil {
        t.Fatalf("failed to create mmap file: %v", err)
    }
    defer mf.Close()

    if mf.Size() != 4096 {
        t.Errorf("expected size 4096, got %d", mf.Size())
    }
}

func TestMmapFileReadWrite(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "test.db")

    mf, err := OpenMmapFile(path, 4096)
    if err != nil {
        t.Fatalf("failed to create mmap file: %v", err)
    }

    // Write data
    data := mf.Slice(100, 11)
    copy(data, []byte("hello world"))

    // Sync and close
    if err := mf.Sync(); err != nil {
        t.Fatalf("sync failed: %v", err)
    }
    mf.Close()

    // Reopen and verify
    mf2, err := OpenMmapFile(path, 0) // 0 = use existing size
    if err != nil {
        t.Fatalf("failed to reopen: %v", err)
    }
    defer mf2.Close()

    got := mf2.Slice(100, 11)
    if string(got) != "hello world" {
        t.Errorf("expected 'hello world', got '%s'", string(got))
    }
}

func TestMmapFileGrow(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "test.db")

    mf, err := OpenMmapFile(path, 4096)
    if err != nil {
        t.Fatalf("failed to create mmap file: %v", err)
    }
    defer mf.Close()

    // Write to first page
    copy(mf.Slice(0, 5), []byte("page1"))

    // Grow the file
    if err := mf.Grow(8192); err != nil {
        t.Fatalf("grow failed: %v", err)
    }

    if mf.Size() != 8192 {
        t.Errorf("expected size 8192 after grow, got %d", mf.Size())
    }

    // Original data should still be there
    if string(mf.Slice(0, 5)) != "page1" {
        t.Error("data lost after grow")
    }
}

func TestMmapFileExisting(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "test.db")

    // Create file with regular IO first
    f, err := os.Create(path)
    if err != nil {
        t.Fatal(err)
    }
    f.Write([]byte("existing data"))
    f.Close()

    // Open with mmap
    mf, err := OpenMmapFile(path, 0)
    if err != nil {
        t.Fatalf("failed to open existing file: %v", err)
    }
    defer mf.Close()

    if string(mf.Slice(0, 13)) != "existing data" {
        t.Error("existing data not preserved")
    }
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/pager/... -v -run Mmap`
Expected: FAIL - OpenMmapFile not defined

**Step 3: Write minimal implementation**

```go
// pkg/pager/mmap.go
package pager

import (
    "errors"
    "os"
    "syscall"
)

// MmapFile provides memory-mapped file access
type MmapFile struct {
    file *os.File
    data []byte
    size int64
}

// OpenMmapFile opens or creates a memory-mapped file
// If initialSize > 0 and file doesn't exist or is smaller, it will be extended
func OpenMmapFile(path string, initialSize int64) (*MmapFile, error) {
    // Open or create file
    f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
    if err != nil {
        return nil, err
    }

    // Get current size
    stat, err := f.Stat()
    if err != nil {
        f.Close()
        return nil, err
    }

    size := stat.Size()
    if initialSize > size {
        // Extend file to initial size
        if err := f.Truncate(initialSize); err != nil {
            f.Close()
            return nil, err
        }
        size = initialSize
    }

    if size == 0 {
        // Can't mmap empty file
        f.Close()
        return nil, errors.New("cannot mmap empty file")
    }

    // Memory map the file
    data, err := syscall.Mmap(int(f.Fd()), 0, int(size),
        syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
    if err != nil {
        f.Close()
        return nil, err
    }

    return &MmapFile{
        file: f,
        data: data,
        size: size,
    }, nil
}

// Size returns the current file size
func (m *MmapFile) Size() int64 {
    return m.size
}

// Slice returns a slice of the mapped memory at the given offset and length
func (m *MmapFile) Slice(offset, length int) []byte {
    if offset+length > len(m.data) {
        return nil
    }
    return m.data[offset : offset+length]
}

// Sync flushes changes to disk
func (m *MmapFile) Sync() error {
    return syscall.Msync(m.data, syscall.MS_SYNC)
}

// Grow extends the file and remaps it
func (m *MmapFile) Grow(newSize int64) error {
    if newSize <= m.size {
        return nil
    }

    // Unmap current mapping
    if err := syscall.Munmap(m.data); err != nil {
        return err
    }

    // Extend file
    if err := m.file.Truncate(newSize); err != nil {
        return err
    }

    // Remap with new size
    data, err := syscall.Mmap(int(m.file.Fd()), 0, int(newSize),
        syscall.PROT_READ|syscall.PROT_WRITE, syscall.MAP_SHARED)
    if err != nil {
        return err
    }

    m.data = data
    m.size = newSize
    return nil
}

// Close unmaps and closes the file
func (m *MmapFile) Close() error {
    var firstErr error

    if m.data != nil {
        if err := syscall.Munmap(m.data); err != nil && firstErr == nil {
            firstErr = err
        }
        m.data = nil
    }

    if m.file != nil {
        if err := m.file.Close(); err != nil && firstErr == nil {
            firstErr = err
        }
        m.file = nil
    }

    return firstErr
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/pager/... -v -run Mmap`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/pager/
git commit -m "feat(pager): add MmapFile for memory-mapped file access"
```

---

### Task 1.6: Pager - Core Pager

**Files:**
- Create: `pkg/pager/pager.go`
- Test: `pkg/pager/pager_test.go`

**Step 1: Write the failing test**

```go
// pkg/pager/pager_test.go
package pager

import (
    "path/filepath"
    "testing"
)

func TestPagerCreate(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "test.db")

    p, err := Open(path, Options{PageSize: 4096})
    if err != nil {
        t.Fatalf("failed to open pager: %v", err)
    }
    defer p.Close()

    if p.PageSize() != 4096 {
        t.Errorf("expected page size 4096, got %d", p.PageSize())
    }
}

func TestPagerAllocatePage(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "test.db")

    p, err := Open(path, Options{PageSize: 4096})
    if err != nil {
        t.Fatalf("failed to open pager: %v", err)
    }
    defer p.Close()

    // Allocate first page (page 1, since page 0 is header)
    page, err := p.Allocate()
    if err != nil {
        t.Fatalf("failed to allocate page: %v", err)
    }
    if page.PageNo() != 1 {
        t.Errorf("expected page number 1, got %d", page.PageNo())
    }

    // Allocate second page
    page2, err := p.Allocate()
    if err != nil {
        t.Fatalf("failed to allocate second page: %v", err)
    }
    if page2.PageNo() != 2 {
        t.Errorf("expected page number 2, got %d", page2.PageNo())
    }
}

func TestPagerGetPage(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "test.db")

    p, err := Open(path, Options{PageSize: 4096})
    if err != nil {
        t.Fatalf("failed to open pager: %v", err)
    }

    // Allocate and write to a page
    page, _ := p.Allocate()
    pageNo := page.PageNo()
    copy(page.Data()[10:], []byte("test data"))
    page.SetDirty(true)
    p.Release(page)

    // Close and reopen
    p.Close()

    p2, err := Open(path, Options{PageSize: 4096})
    if err != nil {
        t.Fatalf("failed to reopen: %v", err)
    }
    defer p2.Close()

    // Get the page back
    page2, err := p2.Get(pageNo)
    if err != nil {
        t.Fatalf("failed to get page: %v", err)
    }

    if string(page2.Data()[10:19]) != "test data" {
        t.Errorf("data not persisted correctly")
    }
}

func TestPagerHeader(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "test.db")

    p, err := Open(path, Options{PageSize: 4096})
    if err != nil {
        t.Fatalf("failed to open pager: %v", err)
    }
    p.Close()

    // Reopen and check header
    p2, err := Open(path, Options{})
    if err != nil {
        t.Fatalf("failed to reopen: %v", err)
    }
    defer p2.Close()

    if p2.PageSize() != 4096 {
        t.Errorf("page size not persisted, got %d", p2.PageSize())
    }
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/pager/... -v -run TestPager`
Expected: FAIL - Open function not defined

**Step 3: Write minimal implementation**

```go
// pkg/pager/pager.go
package pager

import (
    "encoding/binary"
    "errors"
    "sync"
)

const (
    // Database header constants
    headerSize    = 100
    magicString   = "TurDB format 1\x00"
    defaultPageSize = 4096
)

var (
    ErrInvalidHeader = errors.New("invalid database header")
    ErrPageNotFound  = errors.New("page not found")
)

// Options configures the pager
type Options struct {
    PageSize  int  // Page size in bytes (default 4096)
    CacheSize int  // Number of pages to cache (default 1000)
    ReadOnly  bool // Open in read-only mode
}

// Pager manages database pages and caching
type Pager struct {
    mu        sync.RWMutex
    mmap      *MmapFile
    pageSize  int
    pageCount uint32
    cache     map[uint32]*Page
    cacheSize int
}

// Open opens or creates a database file
func Open(path string, opts Options) (*Pager, error) {
    pageSize := opts.PageSize
    if pageSize == 0 {
        pageSize = defaultPageSize
    }

    cacheSize := opts.CacheSize
    if cacheSize == 0 {
        cacheSize = 1000
    }

    // Try to open existing file first
    mf, err := OpenMmapFile(path, int64(pageSize))
    if err != nil {
        return nil, err
    }

    p := &Pager{
        mmap:      mf,
        pageSize:  pageSize,
        cache:     make(map[uint32]*Page),
        cacheSize: cacheSize,
    }

    // Check if this is a new file or existing database
    header := mf.Slice(0, headerSize)
    if string(header[0:16]) == magicString {
        // Existing database - read header
        p.pageSize = int(binary.LittleEndian.Uint32(header[16:20]))
        p.pageCount = binary.LittleEndian.Uint32(header[20:24])
    } else {
        // New database - initialize header
        p.pageCount = 1 // Header page is page 0
        p.writeHeader()
    }

    return p, nil
}

// writeHeader writes the database header to page 0
func (p *Pager) writeHeader() {
    header := p.mmap.Slice(0, headerSize)
    copy(header[0:16], magicString)
    binary.LittleEndian.PutUint32(header[16:20], uint32(p.pageSize))
    binary.LittleEndian.PutUint32(header[20:24], p.pageCount)
}

// PageSize returns the page size
func (p *Pager) PageSize() int {
    return p.pageSize
}

// PageCount returns the number of pages
func (p *Pager) PageCount() uint32 {
    p.mu.RLock()
    defer p.mu.RUnlock()
    return p.pageCount
}

// Allocate creates a new page
func (p *Pager) Allocate() (*Page, error) {
    p.mu.Lock()
    defer p.mu.Unlock()

    pageNo := p.pageCount
    p.pageCount++

    // Ensure file is large enough
    requiredSize := int64(p.pageCount) * int64(p.pageSize)
    if requiredSize > p.mmap.Size() {
        // Grow by at least 10% or to required size
        newSize := p.mmap.Size() + p.mmap.Size()/10
        if newSize < requiredSize {
            newSize = requiredSize
        }
        if err := p.mmap.Grow(newSize); err != nil {
            return nil, err
        }
    }

    // Update header with new page count
    p.writeHeader()

    // Create page backed by mmap
    offset := int(pageNo) * p.pageSize
    data := p.mmap.Slice(offset, p.pageSize)
    page := NewPageWithData(pageNo, data)
    page.Pin()

    p.cache[pageNo] = page

    return page, nil
}

// Get retrieves a page by number
func (p *Pager) Get(pageNo uint32) (*Page, error) {
    p.mu.Lock()
    defer p.mu.Unlock()

    // Check cache first
    if page, ok := p.cache[pageNo]; ok {
        page.Pin()
        return page, nil
    }

    // Check bounds
    if pageNo >= p.pageCount {
        return nil, ErrPageNotFound
    }

    // Load from mmap
    offset := int(pageNo) * p.pageSize
    data := p.mmap.Slice(offset, p.pageSize)
    if data == nil {
        return nil, ErrPageNotFound
    }

    page := NewPageWithData(pageNo, data)
    page.Pin()

    // Add to cache (simple implementation, no eviction yet)
    p.cache[pageNo] = page

    return page, nil
}

// Release unpins a page
func (p *Pager) Release(page *Page) {
    page.Unpin()
}

// Sync flushes all changes to disk
func (p *Pager) Sync() error {
    p.mu.Lock()
    defer p.mu.Unlock()

    p.writeHeader()
    return p.mmap.Sync()
}

// Close closes the pager
func (p *Pager) Close() error {
    p.mu.Lock()
    defer p.mu.Unlock()

    // Write header before closing
    p.writeHeader()

    // Sync and close mmap
    if err := p.mmap.Sync(); err != nil {
        p.mmap.Close()
        return err
    }

    return p.mmap.Close()
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/pager/... -v -run TestPager`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/pager/
git commit -m "feat(pager): add core Pager with page allocation, caching, and persistence"
```

---

## Phase 2: B-Tree Storage

### Task 2.1: B-Tree Node Structure

**Files:**
- Create: `pkg/btree/node.go`
- Test: `pkg/btree/node_test.go`

**Step 1: Write the failing test**

```go
// pkg/btree/node_test.go
package btree

import (
    "testing"
)

func TestNodeCreate(t *testing.T) {
    data := make([]byte, 4096)
    node := NewNode(data, true) // leaf node

    if !node.IsLeaf() {
        t.Error("expected leaf node")
    }
    if node.CellCount() != 0 {
        t.Errorf("expected 0 cells, got %d", node.CellCount())
    }
}

func TestNodeInsertCell(t *testing.T) {
    data := make([]byte, 4096)
    node := NewNode(data, true)

    key := []byte("key1")
    value := []byte("value1")

    if err := node.InsertCell(0, key, value); err != nil {
        t.Fatalf("insert failed: %v", err)
    }

    if node.CellCount() != 1 {
        t.Errorf("expected 1 cell, got %d", node.CellCount())
    }

    gotKey, gotValue := node.GetCell(0)
    if string(gotKey) != "key1" {
        t.Errorf("expected key 'key1', got '%s'", string(gotKey))
    }
    if string(gotValue) != "value1" {
        t.Errorf("expected value 'value1', got '%s'", string(gotValue))
    }
}

func TestNodeMultipleCells(t *testing.T) {
    data := make([]byte, 4096)
    node := NewNode(data, true)

    // Insert multiple cells
    cells := []struct{ key, value string }{
        {"apple", "red"},
        {"banana", "yellow"},
        {"cherry", "red"},
    }

    for i, c := range cells {
        if err := node.InsertCell(i, []byte(c.key), []byte(c.value)); err != nil {
            t.Fatalf("insert %d failed: %v", i, err)
        }
    }

    if node.CellCount() != 3 {
        t.Errorf("expected 3 cells, got %d", node.CellCount())
    }

    // Verify all cells
    for i, c := range cells {
        gotKey, gotValue := node.GetCell(i)
        if string(gotKey) != c.key || string(gotValue) != c.value {
            t.Errorf("cell %d: expected (%s, %s), got (%s, %s)",
                i, c.key, c.value, string(gotKey), string(gotValue))
        }
    }
}

func TestNodeFreeSpace(t *testing.T) {
    data := make([]byte, 4096)
    node := NewNode(data, true)

    initialFree := node.FreeSpace()

    node.InsertCell(0, []byte("key"), []byte("value"))

    afterInsert := node.FreeSpace()
    if afterInsert >= initialFree {
        t.Error("free space should decrease after insert")
    }
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/btree/... -v`
Expected: FAIL - package not found

**Step 3: Write minimal implementation**

```go
// pkg/btree/node.go
package btree

import (
    "encoding/binary"
    "errors"

    "tur/internal/encoding"
)

/*
Node Page Layout (SQLite-inspired):
+------------------+
| Header (12 bytes)|
|   - flags (1)    |
|   - cell count(2)|
|   - free start(2)|
|   - free end (2) |
|   - frag bytes(1)|
|   - right child(4)| (interior only)
+------------------+
| Cell Pointers    |
| (2 bytes each)   |
+------------------+
| Free Space       |
+------------------+
| Cell Content     |
| (grows upward)   |
+------------------+
*/

const (
    nodeHeaderSize       = 12
    cellPointerSize      = 2
    flagLeaf        byte = 0x01
)

var (
    ErrNodeFull     = errors.New("node is full")
    ErrCellNotFound = errors.New("cell not found")
)

// Node represents a B-tree node backed by a page
type Node struct {
    data []byte
}

// NewNode creates a new node, initializing the header
func NewNode(data []byte, isLeaf bool) *Node {
    n := &Node{data: data}

    // Initialize header
    if isLeaf {
        data[0] = flagLeaf
    } else {
        data[0] = 0
    }

    // Cell count = 0
    binary.LittleEndian.PutUint16(data[1:3], 0)

    // Free space starts after header
    binary.LittleEndian.PutUint16(data[3:5], nodeHeaderSize)

    // Free space ends at page end
    binary.LittleEndian.PutUint16(data[5:7], uint16(len(data)))

    // Fragmented bytes = 0
    data[7] = 0

    return n
}

// LoadNode loads an existing node from page data
func LoadNode(data []byte) *Node {
    return &Node{data: data}
}

// IsLeaf returns true if this is a leaf node
func (n *Node) IsLeaf() bool {
    return n.data[0]&flagLeaf != 0
}

// CellCount returns the number of cells in this node
func (n *Node) CellCount() int {
    return int(binary.LittleEndian.Uint16(n.data[1:3]))
}

func (n *Node) setCellCount(count int) {
    binary.LittleEndian.PutUint16(n.data[1:3], uint16(count))
}

// freeStart returns the offset where cell pointers end
func (n *Node) freeStart() int {
    return int(binary.LittleEndian.Uint16(n.data[3:5]))
}

func (n *Node) setFreeStart(offset int) {
    binary.LittleEndian.PutUint16(n.data[3:5], uint16(offset))
}

// freeEnd returns the offset where cell content starts
func (n *Node) freeEnd() int {
    return int(binary.LittleEndian.Uint16(n.data[5:7]))
}

func (n *Node) setFreeEnd(offset int) {
    binary.LittleEndian.PutUint16(n.data[5:7], uint16(offset))
}

// FreeSpace returns the amount of free space available
func (n *Node) FreeSpace() int {
    return n.freeEnd() - n.freeStart()
}

// cellPointer returns the offset of cell i's pointer
func (n *Node) cellPointerOffset(i int) int {
    return nodeHeaderSize + i*cellPointerSize
}

// getCellOffset returns the offset of cell i's content
func (n *Node) getCellOffset(i int) int {
    ptrOffset := n.cellPointerOffset(i)
    return int(binary.LittleEndian.Uint16(n.data[ptrOffset:]))
}

func (n *Node) setCellOffset(i, offset int) {
    ptrOffset := n.cellPointerOffset(i)
    binary.LittleEndian.PutUint16(n.data[ptrOffset:], uint16(offset))
}

// InsertCell inserts a key-value cell at position i
func (n *Node) InsertCell(i int, key, value []byte) error {
    // Calculate cell size: key_len(varint) + key + value_len(varint) + value
    cellSize := encoding.VarintLen(uint64(len(key))) + len(key) +
                encoding.VarintLen(uint64(len(value))) + len(value)

    // Check if we have enough space
    spaceNeeded := cellSize + cellPointerSize
    if n.FreeSpace() < spaceNeeded {
        return ErrNodeFull
    }

    count := n.CellCount()

    // Shift cell pointers to make room at position i
    for j := count; j > i; j-- {
        n.setCellOffset(j, n.getCellOffset(j-1))
    }

    // Allocate space for cell content (grows from end of page backward)
    newFreeEnd := n.freeEnd() - cellSize
    n.setFreeEnd(newFreeEnd)

    // Write cell content
    offset := newFreeEnd
    offset += encoding.PutVarint(n.data[offset:], uint64(len(key)))
    copy(n.data[offset:], key)
    offset += len(key)
    offset += encoding.PutVarint(n.data[offset:], uint64(len(value)))
    copy(n.data[offset:], value)

    // Set cell pointer
    n.setCellOffset(i, newFreeEnd)

    // Update cell count and free start
    n.setCellCount(count + 1)
    n.setFreeStart(n.freeStart() + cellPointerSize)

    return nil
}

// GetCell returns the key and value at position i
func (n *Node) GetCell(i int) (key, value []byte) {
    if i < 0 || i >= n.CellCount() {
        return nil, nil
    }

    offset := n.getCellOffset(i)

    // Read key
    keyLen, sz := encoding.GetVarint(n.data[offset:])
    offset += sz
    key = n.data[offset : offset+int(keyLen)]
    offset += int(keyLen)

    // Read value
    valueLen, sz := encoding.GetVarint(n.data[offset:])
    offset += sz
    value = n.data[offset : offset+int(valueLen)]

    return key, value
}

// SetRightChild sets the right child page number (interior nodes only)
func (n *Node) SetRightChild(pageNo uint32) {
    binary.LittleEndian.PutUint32(n.data[8:12], pageNo)
}

// RightChild returns the right child page number
func (n *Node) RightChild() uint32 {
    return binary.LittleEndian.Uint32(n.data[8:12])
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/btree/... -v`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/btree/
git commit -m "feat(btree): add Node structure with cell insert/get operations"
```

---

### Task 2.2: B-Tree Core Operations

**Files:**
- Create: `pkg/btree/btree.go`
- Test: `pkg/btree/btree_test.go`

**Step 1: Write the failing test**

```go
// pkg/btree/btree_test.go
package btree

import (
    "path/filepath"
    "testing"

    "tur/pkg/pager"
)

func TestBTreeCreate(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "test.db")

    p, err := pager.Open(path, pager.Options{PageSize: 4096})
    if err != nil {
        t.Fatalf("failed to open pager: %v", err)
    }
    defer p.Close()

    bt, err := Create(p)
    if err != nil {
        t.Fatalf("failed to create btree: %v", err)
    }

    if bt.RootPage() == 0 {
        t.Error("root page should not be 0")
    }
}

func TestBTreeInsertAndGet(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "test.db")

    p, err := pager.Open(path, pager.Options{PageSize: 4096})
    if err != nil {
        t.Fatalf("failed to open pager: %v", err)
    }
    defer p.Close()

    bt, err := Create(p)
    if err != nil {
        t.Fatalf("failed to create btree: %v", err)
    }

    // Insert a key-value pair
    if err := bt.Insert([]byte("hello"), []byte("world")); err != nil {
        t.Fatalf("insert failed: %v", err)
    }

    // Get it back
    value, err := bt.Get([]byte("hello"))
    if err != nil {
        t.Fatalf("get failed: %v", err)
    }
    if string(value) != "world" {
        t.Errorf("expected 'world', got '%s'", string(value))
    }
}

func TestBTreeMultipleInserts(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "test.db")

    p, err := pager.Open(path, pager.Options{PageSize: 4096})
    if err != nil {
        t.Fatalf("failed to open pager: %v", err)
    }
    defer p.Close()

    bt, _ := Create(p)

    // Insert multiple keys
    keys := []string{"banana", "apple", "cherry", "date", "elderberry"}
    for _, k := range keys {
        if err := bt.Insert([]byte(k), []byte("value_"+k)); err != nil {
            t.Fatalf("insert %s failed: %v", k, err)
        }
    }

    // Verify all keys
    for _, k := range keys {
        value, err := bt.Get([]byte(k))
        if err != nil {
            t.Fatalf("get %s failed: %v", k, err)
        }
        expected := "value_" + k
        if string(value) != expected {
            t.Errorf("key %s: expected '%s', got '%s'", k, expected, string(value))
        }
    }
}

func TestBTreeNotFound(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "test.db")

    p, _ := pager.Open(path, pager.Options{PageSize: 4096})
    defer p.Close()

    bt, _ := Create(p)
    bt.Insert([]byte("exists"), []byte("yes"))

    _, err := bt.Get([]byte("notfound"))
    if err != ErrKeyNotFound {
        t.Errorf("expected ErrKeyNotFound, got %v", err)
    }
}

func TestBTreeUpdate(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "test.db")

    p, _ := pager.Open(path, pager.Options{PageSize: 4096})
    defer p.Close()

    bt, _ := Create(p)

    // Insert
    bt.Insert([]byte("key"), []byte("value1"))

    // Update (insert same key)
    bt.Insert([]byte("key"), []byte("value2"))

    // Should get updated value
    value, _ := bt.Get([]byte("key"))
    if string(value) != "value2" {
        t.Errorf("expected 'value2', got '%s'", string(value))
    }
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/btree/... -v -run TestBTree`
Expected: FAIL - Create function not defined

**Step 3: Write minimal implementation**

```go
// pkg/btree/btree.go
package btree

import (
    "bytes"
    "errors"

    "tur/pkg/pager"
)

var (
    ErrKeyNotFound = errors.New("key not found")
)

// BTree represents a B-tree index
type BTree struct {
    pager    *pager.Pager
    rootPage uint32
}

// Create creates a new B-tree, allocating a root page
func Create(p *pager.Pager) (*BTree, error) {
    // Allocate root page
    page, err := p.Allocate()
    if err != nil {
        return nil, err
    }

    // Initialize as empty leaf node
    NewNode(page.Data(), true)
    page.SetDirty(true)
    page.SetType(pager.PageTypeBTreeLeaf)

    rootPage := page.PageNo()
    p.Release(page)

    return &BTree{
        pager:    p,
        rootPage: rootPage,
    }, nil
}

// Open opens an existing B-tree with the given root page
func Open(p *pager.Pager, rootPage uint32) *BTree {
    return &BTree{
        pager:    p,
        rootPage: rootPage,
    }
}

// RootPage returns the root page number
func (bt *BTree) RootPage() uint32 {
    return bt.rootPage
}

// Insert inserts or updates a key-value pair
func (bt *BTree) Insert(key, value []byte) error {
    page, err := bt.pager.Get(bt.rootPage)
    if err != nil {
        return err
    }
    defer bt.pager.Release(page)

    node := LoadNode(page.Data())

    // Find insertion position (sorted order)
    pos := bt.findPosition(node, key)

    // Check if key already exists at this position
    if pos < node.CellCount() {
        existingKey, _ := node.GetCell(pos)
        if bytes.Equal(existingKey, key) {
            // Update: for now, we'll do a simple delete + insert
            // A more sophisticated implementation would update in place
            bt.deleteAt(node, pos)
            pos = bt.findPosition(node, key)
        }
    }

    // Insert at position
    err = node.InsertCell(pos, key, value)
    if err == ErrNodeFull {
        // TODO: implement page splitting
        return errors.New("page split not implemented yet")
    }
    if err != nil {
        return err
    }

    page.SetDirty(true)
    return nil
}

// Get retrieves the value for a key
func (bt *BTree) Get(key []byte) ([]byte, error) {
    page, err := bt.pager.Get(bt.rootPage)
    if err != nil {
        return nil, err
    }
    defer bt.pager.Release(page)

    node := LoadNode(page.Data())

    // Binary search for key
    pos := bt.findPosition(node, key)

    if pos < node.CellCount() {
        foundKey, value := node.GetCell(pos)
        if bytes.Equal(foundKey, key) {
            // Return a copy to avoid issues with mmap
            result := make([]byte, len(value))
            copy(result, value)
            return result, nil
        }
    }

    return nil, ErrKeyNotFound
}

// findPosition returns the position where key should be inserted (binary search)
func (bt *BTree) findPosition(node *Node, key []byte) int {
    count := node.CellCount()
    lo, hi := 0, count

    for lo < hi {
        mid := (lo + hi) / 2
        midKey, _ := node.GetCell(mid)
        cmp := bytes.Compare(midKey, key)
        if cmp < 0 {
            lo = mid + 1
        } else {
            hi = mid
        }
    }

    return lo
}

// deleteAt removes the cell at position i
func (bt *BTree) deleteAt(node *Node, i int) {
    count := node.CellCount()
    if i < 0 || i >= count {
        return
    }

    // Shift cell pointers
    for j := i; j < count-1; j++ {
        node.setCellOffset(j, node.getCellOffset(j+1))
    }

    node.setCellCount(count - 1)
    // Note: we don't reclaim the cell content space in this simple implementation
    // A full implementation would track fragmentation and defragment when needed
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/btree/... -v -run TestBTree`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/btree/
git commit -m "feat(btree): add BTree with Insert and Get operations"
```

---

### Task 2.3: B-Tree Cursor

**Files:**
- Create: `pkg/btree/cursor.go`
- Test: `pkg/btree/cursor_test.go`

**Step 1: Write the failing test**

```go
// pkg/btree/cursor_test.go
package btree

import (
    "path/filepath"
    "testing"

    "tur/pkg/pager"
)

func TestCursorIterate(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "test.db")

    p, _ := pager.Open(path, pager.Options{PageSize: 4096})
    defer p.Close()

    bt, _ := Create(p)

    // Insert keys (will be stored sorted)
    keys := []string{"cherry", "apple", "banana"}
    for _, k := range keys {
        bt.Insert([]byte(k), []byte("v_"+k))
    }

    // Iterate and collect keys
    cursor := bt.Cursor()
    var collected []string

    for cursor.First(); cursor.Valid(); cursor.Next() {
        key, _ := cursor.Key(), cursor.Value()
        collected = append(collected, string(key))
    }

    // Should be in sorted order
    expected := []string{"apple", "banana", "cherry"}
    if len(collected) != len(expected) {
        t.Fatalf("expected %d keys, got %d", len(expected), len(collected))
    }
    for i, k := range expected {
        if collected[i] != k {
            t.Errorf("position %d: expected '%s', got '%s'", i, k, collected[i])
        }
    }
}

func TestCursorSeek(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "test.db")

    p, _ := pager.Open(path, pager.Options{PageSize: 4096})
    defer p.Close()

    bt, _ := Create(p)

    bt.Insert([]byte("a"), []byte("1"))
    bt.Insert([]byte("c"), []byte("3"))
    bt.Insert([]byte("e"), []byte("5"))

    cursor := bt.Cursor()

    // Seek to existing key
    cursor.Seek([]byte("c"))
    if !cursor.Valid() {
        t.Fatal("cursor should be valid after seek")
    }
    if string(cursor.Key()) != "c" {
        t.Errorf("expected 'c', got '%s'", string(cursor.Key()))
    }

    // Seek to non-existing key (should land on next key)
    cursor.Seek([]byte("b"))
    if string(cursor.Key()) != "c" {
        t.Errorf("expected 'c' after seeking 'b', got '%s'", string(cursor.Key()))
    }

    // Seek past all keys
    cursor.Seek([]byte("z"))
    if cursor.Valid() {
        t.Error("cursor should be invalid after seeking past all keys")
    }
}

func TestCursorEmpty(t *testing.T) {
    dir := t.TempDir()
    path := filepath.Join(dir, "test.db")

    p, _ := pager.Open(path, pager.Options{PageSize: 4096})
    defer p.Close()

    bt, _ := Create(p)

    cursor := bt.Cursor()
    cursor.First()

    if cursor.Valid() {
        t.Error("cursor should be invalid on empty tree")
    }
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/btree/... -v -run TestCursor`
Expected: FAIL - Cursor not defined

**Step 3: Write minimal implementation**

```go
// pkg/btree/cursor.go
package btree

import (
    "bytes"

    "tur/pkg/pager"
)

// Cursor provides iteration over B-tree entries
type Cursor struct {
    btree *BTree
    page  *pager.Page
    node  *Node
    pos   int
    valid bool
}

// Cursor creates a new cursor for this B-tree
func (bt *BTree) Cursor() *Cursor {
    return &Cursor{
        btree: bt,
        valid: false,
    }
}

// First moves the cursor to the first entry
func (c *Cursor) First() {
    c.release()

    page, err := c.btree.pager.Get(c.btree.rootPage)
    if err != nil {
        c.valid = false
        return
    }

    c.page = page
    c.node = LoadNode(page.Data())
    c.pos = 0
    c.valid = c.node.CellCount() > 0
}

// Last moves the cursor to the last entry
func (c *Cursor) Last() {
    c.release()

    page, err := c.btree.pager.Get(c.btree.rootPage)
    if err != nil {
        c.valid = false
        return
    }

    c.page = page
    c.node = LoadNode(page.Data())
    count := c.node.CellCount()
    if count > 0 {
        c.pos = count - 1
        c.valid = true
    } else {
        c.valid = false
    }
}

// Seek moves the cursor to the first entry >= key
func (c *Cursor) Seek(key []byte) {
    c.release()

    page, err := c.btree.pager.Get(c.btree.rootPage)
    if err != nil {
        c.valid = false
        return
    }

    c.page = page
    c.node = LoadNode(page.Data())

    // Binary search
    count := c.node.CellCount()
    lo, hi := 0, count

    for lo < hi {
        mid := (lo + hi) / 2
        midKey, _ := c.node.GetCell(mid)
        if bytes.Compare(midKey, key) < 0 {
            lo = mid + 1
        } else {
            hi = mid
        }
    }

    c.pos = lo
    c.valid = lo < count
}

// Next moves the cursor to the next entry
func (c *Cursor) Next() {
    if !c.valid {
        return
    }

    c.pos++
    if c.pos >= c.node.CellCount() {
        c.valid = false
    }
}

// Prev moves the cursor to the previous entry
func (c *Cursor) Prev() {
    if !c.valid {
        return
    }

    c.pos--
    if c.pos < 0 {
        c.valid = false
    }
}

// Valid returns true if the cursor points to a valid entry
func (c *Cursor) Valid() bool {
    return c.valid
}

// Key returns the current key (only valid if Valid() is true)
func (c *Cursor) Key() []byte {
    if !c.valid {
        return nil
    }
    key, _ := c.node.GetCell(c.pos)
    // Return a copy
    result := make([]byte, len(key))
    copy(result, key)
    return result
}

// Value returns the current value (only valid if Valid() is true)
func (c *Cursor) Value() []byte {
    if !c.valid {
        return nil
    }
    _, value := c.node.GetCell(c.pos)
    // Return a copy
    result := make([]byte, len(value))
    copy(result, value)
    return result
}

// Close releases resources held by the cursor
func (c *Cursor) Close() {
    c.release()
}

func (c *Cursor) release() {
    if c.page != nil {
        c.btree.pager.Release(c.page)
        c.page = nil
        c.node = nil
    }
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/btree/... -v -run TestCursor`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/btree/
git commit -m "feat(btree): add Cursor for iteration and seeking"
```

---

## Phase 3: HNSW Vector Index

### Task 3.1: HNSW Node Structure

**Files:**
- Create: `pkg/hnsw/node.go`
- Test: `pkg/hnsw/node_test.go`

**Step 1: Write the failing test**

```go
// pkg/hnsw/node_test.go
package hnsw

import (
    "testing"

    "tur/pkg/types"
)

func TestHNSWNodeCreate(t *testing.T) {
    vec := types.NewVector([]float32{1.0, 2.0, 3.0})
    node := NewHNSWNode(1, 42, vec, 2) // id=1, rowID=42, level=2

    if node.ID() != 1 {
        t.Errorf("expected ID 1, got %d", node.ID())
    }
    if node.RowID() != 42 {
        t.Errorf("expected RowID 42, got %d", node.RowID())
    }
    if node.Level() != 2 {
        t.Errorf("expected level 2, got %d", node.Level())
    }
    if node.Vector().Dimension() != 3 {
        t.Errorf("expected dimension 3, got %d", node.Vector().Dimension())
    }
}

func TestHNSWNodeNeighbors(t *testing.T) {
    vec := types.NewVector([]float32{1.0, 2.0})
    node := NewHNSWNode(1, 1, vec, 2)

    // Add neighbors at level 0
    node.AddNeighbor(0, 10)
    node.AddNeighbor(0, 20)
    node.AddNeighbor(0, 30)

    // Add neighbors at level 1
    node.AddNeighbor(1, 100)

    neighbors0 := node.Neighbors(0)
    if len(neighbors0) != 3 {
        t.Errorf("expected 3 neighbors at level 0, got %d", len(neighbors0))
    }

    neighbors1 := node.Neighbors(1)
    if len(neighbors1) != 1 {
        t.Errorf("expected 1 neighbor at level 1, got %d", len(neighbors1))
    }

    // Check invalid level
    neighbors2 := node.Neighbors(5)
    if neighbors2 != nil {
        t.Error("expected nil for invalid level")
    }
}

func TestHNSWNodeSetNeighbors(t *testing.T) {
    vec := types.NewVector([]float32{1.0})
    node := NewHNSWNode(1, 1, vec, 1)

    // Set all neighbors at once
    node.SetNeighbors(0, []uint64{5, 10, 15})

    neighbors := node.Neighbors(0)
    if len(neighbors) != 3 {
        t.Fatalf("expected 3 neighbors, got %d", len(neighbors))
    }
    if neighbors[0] != 5 || neighbors[1] != 10 || neighbors[2] != 15 {
        t.Errorf("unexpected neighbors: %v", neighbors)
    }
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/hnsw/... -v`
Expected: FAIL - package not found

**Step 3: Write minimal implementation**

```go
// pkg/hnsw/node.go
package hnsw

import (
    "tur/pkg/types"
)

// HNSWNode represents a node in the HNSW graph
type HNSWNode struct {
    id        uint64
    rowID     int64          // Foreign key to B-tree row
    vector    *types.Vector
    level     int            // Maximum level this node exists at
    neighbors [][]uint64     // neighbors[level] = list of neighbor IDs
}

// NewHNSWNode creates a new HNSW node
func NewHNSWNode(id uint64, rowID int64, vector *types.Vector, level int) *HNSWNode {
    neighbors := make([][]uint64, level+1)
    for i := range neighbors {
        neighbors[i] = make([]uint64, 0)
    }

    return &HNSWNode{
        id:        id,
        rowID:     rowID,
        vector:    vector,
        level:     level,
        neighbors: neighbors,
    }
}

// ID returns the node's unique identifier
func (n *HNSWNode) ID() uint64 {
    return n.id
}

// RowID returns the associated B-tree row ID
func (n *HNSWNode) RowID() int64 {
    return n.rowID
}

// Vector returns the node's vector
func (n *HNSWNode) Vector() *types.Vector {
    return n.vector
}

// Level returns the maximum level this node exists at
func (n *HNSWNode) Level() int {
    return n.level
}

// Neighbors returns the neighbor IDs at the given level
func (n *HNSWNode) Neighbors(level int) []uint64 {
    if level < 0 || level > n.level {
        return nil
    }
    return n.neighbors[level]
}

// AddNeighbor adds a neighbor at the given level
func (n *HNSWNode) AddNeighbor(level int, neighborID uint64) {
    if level < 0 || level > n.level {
        return
    }
    n.neighbors[level] = append(n.neighbors[level], neighborID)
}

// SetNeighbors sets all neighbors at a given level
func (n *HNSWNode) SetNeighbors(level int, neighborIDs []uint64) {
    if level < 0 || level > n.level {
        return
    }
    n.neighbors[level] = make([]uint64, len(neighborIDs))
    copy(n.neighbors[level], neighborIDs)
}

// RemoveNeighbor removes a neighbor at the given level
func (n *HNSWNode) RemoveNeighbor(level int, neighborID uint64) {
    if level < 0 || level > n.level {
        return
    }
    neighbors := n.neighbors[level]
    for i, id := range neighbors {
        if id == neighborID {
            n.neighbors[level] = append(neighbors[:i], neighbors[i+1:]...)
            return
        }
    }
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/hnsw/... -v`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/hnsw/
git commit -m "feat(hnsw): add HNSWNode structure with neighbor management"
```

---

### Task 3.2: HNSW Index Configuration and Structure

**Files:**
- Create: `pkg/hnsw/config.go`
- Create: `pkg/hnsw/index.go`
- Test: `pkg/hnsw/index_test.go`

**Step 1: Write the failing test**

```go
// pkg/hnsw/index_test.go
package hnsw

import (
    "testing"

    "tur/pkg/types"
)

func TestIndexCreate(t *testing.T) {
    config := DefaultConfig(128) // 128 dimensions
    idx := NewIndex(config)

    if idx.Len() != 0 {
        t.Errorf("expected empty index, got %d nodes", idx.Len())
    }
    if idx.Dimension() != 128 {
        t.Errorf("expected dimension 128, got %d", idx.Dimension())
    }
}

func TestIndexInsertOne(t *testing.T) {
    config := DefaultConfig(3)
    idx := NewIndex(config)

    vec := types.NewVector([]float32{1.0, 0.0, 0.0})
    vec.Normalize()

    err := idx.Insert(1, vec)
    if err != nil {
        t.Fatalf("insert failed: %v", err)
    }

    if idx.Len() != 1 {
        t.Errorf("expected 1 node, got %d", idx.Len())
    }
}

func TestIndexInsertMultiple(t *testing.T) {
    config := DefaultConfig(3)
    idx := NewIndex(config)

    vectors := [][]float32{
        {1.0, 0.0, 0.0},
        {0.0, 1.0, 0.0},
        {0.0, 0.0, 1.0},
        {1.0, 1.0, 0.0},
        {1.0, 0.0, 1.0},
    }

    for i, v := range vectors {
        vec := types.NewVector(v)
        vec.Normalize()
        if err := idx.Insert(int64(i+1), vec); err != nil {
            t.Fatalf("insert %d failed: %v", i, err)
        }
    }

    if idx.Len() != 5 {
        t.Errorf("expected 5 nodes, got %d", idx.Len())
    }
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/hnsw/... -v -run TestIndex`
Expected: FAIL - DefaultConfig, NewIndex not defined

**Step 3: Write minimal implementation**

```go
// pkg/hnsw/config.go
package hnsw

import "math"

// Config holds HNSW index parameters
type Config struct {
    // M is the maximum number of connections per node at layers > 0
    M int

    // MMax0 is the maximum number of connections at layer 0
    MMax0 int

    // EfConstruction is the size of the dynamic candidate list during construction
    EfConstruction int

    // EfSearch is the default size of the dynamic candidate list during search
    EfSearch int

    // Dimension is the vector dimension
    Dimension int

    // ML is the level generation factor (1/ln(M))
    ML float64
}

// DefaultConfig returns a Config with sensible defaults
func DefaultConfig(dimension int) Config {
    m := 16
    return Config{
        M:              m,
        MMax0:          m * 2,
        EfConstruction: 200,
        EfSearch:       50,
        Dimension:      dimension,
        ML:             1.0 / math.Log(float64(m)),
    }
}
```

```go
// pkg/hnsw/index.go
package hnsw

import (
    "errors"
    "math/rand"
    "sync"

    "tur/pkg/types"
)

var (
    ErrDimensionMismatch = errors.New("vector dimension mismatch")
)

// Index is an HNSW index for approximate nearest neighbor search
type Index struct {
    mu         sync.RWMutex
    config     Config
    nodes      map[uint64]*HNSWNode  // nodeID -> node
    entryPoint uint64                // entry point node ID
    maxLevel   int                   // current maximum level
    nextID     uint64                // next node ID to assign
}

// NewIndex creates a new empty HNSW index
func NewIndex(config Config) *Index {
    return &Index{
        config: config,
        nodes:  make(map[uint64]*HNSWNode),
    }
}

// Len returns the number of nodes in the index
func (idx *Index) Len() int {
    idx.mu.RLock()
    defer idx.mu.RUnlock()
    return len(idx.nodes)
}

// Dimension returns the vector dimension
func (idx *Index) Dimension() int {
    return idx.config.Dimension
}

// Config returns the index configuration
func (idx *Index) Config() Config {
    return idx.config
}

// randomLevel generates a random level for a new node
func (idx *Index) randomLevel() int {
    level := 0
    for rand.Float64() < idx.config.ML && level < 32 {
        level++
    }
    return level
}

// Insert adds a vector to the index
func (idx *Index) Insert(rowID int64, vector *types.Vector) error {
    if vector.Dimension() != idx.config.Dimension {
        return ErrDimensionMismatch
    }

    idx.mu.Lock()
    defer idx.mu.Unlock()

    // Assign node ID
    nodeID := idx.nextID
    idx.nextID++

    // Generate random level for this node
    level := idx.randomLevel()

    // Create node
    node := NewHNSWNode(nodeID, rowID, vector, level)

    // If this is the first node, it becomes the entry point
    if len(idx.nodes) == 0 {
        idx.nodes[nodeID] = node
        idx.entryPoint = nodeID
        idx.maxLevel = level
        return nil
    }

    // Find entry point and insert
    ep := idx.entryPoint
    currentLevel := idx.maxLevel

    // Phase 1: Traverse from top to node's level, finding closest node at each level
    for l := currentLevel; l > level; l-- {
        ep = idx.searchLayerClosest(vector, ep, l)
    }

    // Phase 2: Insert at each level from node's level down to 0
    for l := min(level, currentLevel); l >= 0; l-- {
        // Find neighbors at this level
        neighbors := idx.searchLayer(vector, ep, idx.config.EfConstruction, l)

        // Select M best neighbors
        maxNeighbors := idx.config.M
        if l == 0 {
            maxNeighbors = idx.config.MMax0
        }
        selectedNeighbors := idx.selectNeighbors(vector, neighbors, maxNeighbors)

        // Connect node to neighbors bidirectionally
        node.SetNeighbors(l, selectedNeighbors)
        for _, neighborID := range selectedNeighbors {
            neighbor := idx.nodes[neighborID]
            neighbor.AddNeighbor(l, nodeID)

            // Prune neighbor's connections if needed
            idx.pruneConnections(neighbor, l, maxNeighbors)
        }

        // Use closest neighbor as entry point for next level
        if len(selectedNeighbors) > 0 {
            ep = selectedNeighbors[0]
        }
    }

    // Store node
    idx.nodes[nodeID] = node

    // Update entry point if this node has higher level
    if level > idx.maxLevel {
        idx.entryPoint = nodeID
        idx.maxLevel = level
    }

    return nil
}

// searchLayerClosest finds the closest node to query at the given level
func (idx *Index) searchLayerClosest(query *types.Vector, ep uint64, level int) uint64 {
    current := ep
    currentDist := query.CosineDistance(idx.nodes[current].Vector())

    for {
        improved := false
        for _, neighborID := range idx.nodes[current].Neighbors(level) {
            dist := query.CosineDistance(idx.nodes[neighborID].Vector())
            if dist < currentDist {
                current = neighborID
                currentDist = dist
                improved = true
            }
        }
        if !improved {
            break
        }
    }

    return current
}

// searchLayer finds ef closest nodes to query at the given level
func (idx *Index) searchLayer(query *types.Vector, ep uint64, ef int, level int) []uint64 {
    visited := make(map[uint64]bool)
    visited[ep] = true

    // candidates: nodes to explore (sorted by distance, closest first)
    // results: current best results (sorted by distance, furthest first for easy removal)
    candidates := []distNode{{id: ep, dist: query.CosineDistance(idx.nodes[ep].Vector())}}
    results := []distNode{{id: ep, dist: candidates[0].dist}}

    for len(candidates) > 0 {
        // Get closest candidate
        closest := candidates[0]
        candidates = candidates[1:]

        // If closest candidate is further than furthest result, we're done
        if len(results) >= ef && closest.dist > results[len(results)-1].dist {
            break
        }

        // Explore neighbors
        for _, neighborID := range idx.nodes[closest.id].Neighbors(level) {
            if visited[neighborID] {
                continue
            }
            visited[neighborID] = true

            dist := query.CosineDistance(idx.nodes[neighborID].Vector())

            // Add to results if better than worst result or not enough results yet
            if len(results) < ef || dist < results[len(results)-1].dist {
                results = insertSorted(results, distNode{id: neighborID, dist: dist})
                if len(results) > ef {
                    results = results[:ef]
                }
                candidates = insertSorted(candidates, distNode{id: neighborID, dist: dist})
            }
        }
    }

    // Extract IDs from results
    ids := make([]uint64, len(results))
    for i, r := range results {
        ids[i] = r.id
    }
    return ids
}

// selectNeighbors selects the M best neighbors (simple heuristic)
func (idx *Index) selectNeighbors(query *types.Vector, candidates []uint64, m int) []uint64 {
    if len(candidates) <= m {
        return candidates
    }
    return candidates[:m]
}

// pruneConnections ensures a node doesn't exceed max connections
func (idx *Index) pruneConnections(node *HNSWNode, level int, maxConnections int) {
    neighbors := node.Neighbors(level)
    if len(neighbors) <= maxConnections {
        return
    }

    // Keep only the closest maxConnections neighbors
    type nd struct {
        id   uint64
        dist float32
    }
    nds := make([]nd, len(neighbors))
    for i, nid := range neighbors {
        nds[i] = nd{id: nid, dist: node.Vector().CosineDistance(idx.nodes[nid].Vector())}
    }

    // Sort by distance
    for i := 0; i < len(nds)-1; i++ {
        for j := i + 1; j < len(nds); j++ {
            if nds[j].dist < nds[i].dist {
                nds[i], nds[j] = nds[j], nds[i]
            }
        }
    }

    // Keep only maxConnections
    selected := make([]uint64, maxConnections)
    for i := 0; i < maxConnections; i++ {
        selected[i] = nds[i].id
    }
    node.SetNeighbors(level, selected)
}

// distNode pairs a node ID with its distance
type distNode struct {
    id   uint64
    dist float32
}

// insertSorted inserts a distNode into a sorted slice (by distance, ascending)
func insertSorted(slice []distNode, node distNode) []distNode {
    i := 0
    for i < len(slice) && slice[i].dist < node.dist {
        i++
    }
    slice = append(slice, distNode{})
    copy(slice[i+1:], slice[i:])
    slice[i] = node
    return slice
}

func min(a, b int) int {
    if a < b {
        return a
    }
    return b
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/hnsw/... -v -run TestIndex`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/hnsw/
git commit -m "feat(hnsw): add HNSW index with Insert operation"
```

---

### Task 3.3: HNSW Search

**Files:**
- Modify: `pkg/hnsw/index.go`
- Create: `pkg/hnsw/search.go`
- Test: `pkg/hnsw/search_test.go`

**Step 1: Write the failing test**

```go
// pkg/hnsw/search_test.go
package hnsw

import (
    "math"
    "testing"

    "tur/pkg/types"
)

func TestSearchKNN(t *testing.T) {
    config := DefaultConfig(3)
    idx := NewIndex(config)

    // Insert some vectors
    vectors := [][]float32{
        {1.0, 0.0, 0.0},  // rowID 1
        {0.9, 0.1, 0.0},  // rowID 2 - close to first
        {0.0, 1.0, 0.0},  // rowID 3
        {0.0, 0.0, 1.0},  // rowID 4
        {0.8, 0.2, 0.0},  // rowID 5 - close to first
    }

    for i, v := range vectors {
        vec := types.NewVector(v)
        vec.Normalize()
        idx.Insert(int64(i+1), vec)
    }

    // Search for vectors similar to [1, 0, 0]
    query := types.NewVector([]float32{1.0, 0.0, 0.0})
    query.Normalize()

    results, err := idx.SearchKNN(query, 3)
    if err != nil {
        t.Fatalf("search failed: %v", err)
    }

    if len(results) != 3 {
        t.Fatalf("expected 3 results, got %d", len(results))
    }

    // First result should be rowID 1 (exact match)
    if results[0].RowID != 1 {
        t.Errorf("expected rowID 1 as first result, got %d", results[0].RowID)
    }

    // Distance should be ~0 for exact match
    if results[0].Distance > 0.01 {
        t.Errorf("expected distance ~0, got %f", results[0].Distance)
    }

    // Results should be sorted by distance
    for i := 1; i < len(results); i++ {
        if results[i].Distance < results[i-1].Distance {
            t.Errorf("results not sorted by distance")
        }
    }
}

func TestSearchKNNWithEf(t *testing.T) {
    config := DefaultConfig(3)
    idx := NewIndex(config)

    // Insert 100 random-ish vectors
    for i := 0; i < 100; i++ {
        v := []float32{
            float32(math.Sin(float64(i))),
            float32(math.Cos(float64(i))),
            float32(math.Sin(float64(i) * 2)),
        }
        vec := types.NewVector(v)
        vec.Normalize()
        idx.Insert(int64(i+1), vec)
    }

    query := types.NewVector([]float32{1.0, 0.0, 0.0})
    query.Normalize()

    // Search with different ef values
    results10, _ := idx.SearchKNNWithEf(query, 5, 10)
    results100, _ := idx.SearchKNNWithEf(query, 5, 100)

    if len(results10) != 5 || len(results100) != 5 {
        t.Error("expected 5 results each")
    }

    // Higher ef should generally give better (or equal) results
    // (not always guaranteed due to HNSW approximation)
    t.Logf("ef=10: first result distance = %f", results10[0].Distance)
    t.Logf("ef=100: first result distance = %f", results100[0].Distance)
}

func TestSearchEmpty(t *testing.T) {
    config := DefaultConfig(3)
    idx := NewIndex(config)

    query := types.NewVector([]float32{1.0, 0.0, 0.0})
    results, err := idx.SearchKNN(query, 5)

    if err != nil {
        t.Fatalf("search on empty index should not error: %v", err)
    }
    if len(results) != 0 {
        t.Errorf("expected 0 results on empty index, got %d", len(results))
    }
}
```

**Step 2: Run test to verify it fails**

Run: `go test ./pkg/hnsw/... -v -run TestSearch`
Expected: FAIL - SearchKNN not defined

**Step 3: Write minimal implementation**

```go
// pkg/hnsw/search.go
package hnsw

import (
    "tur/pkg/types"
)

// SearchResult represents a single search result
type SearchResult struct {
    RowID    int64
    Distance float32
}

// SearchKNN finds the k nearest neighbors to the query vector
func (idx *Index) SearchKNN(query *types.Vector, k int) ([]SearchResult, error) {
    return idx.SearchKNNWithEf(query, k, idx.config.EfSearch)
}

// SearchKNNWithEf finds the k nearest neighbors with a custom ef parameter
func (idx *Index) SearchKNNWithEf(query *types.Vector, k int, ef int) ([]SearchResult, error) {
    if query.Dimension() != idx.config.Dimension {
        return nil, ErrDimensionMismatch
    }

    idx.mu.RLock()
    defer idx.mu.RUnlock()

    if len(idx.nodes) == 0 {
        return []SearchResult{}, nil
    }

    // Start at entry point
    ep := idx.entryPoint

    // Phase 1: Greedily descend from top level to level 1
    for l := idx.maxLevel; l > 0; l-- {
        ep = idx.searchLayerClosest(query, ep, l)
    }

    // Phase 2: Search at level 0 with ef candidates
    candidates := idx.searchLayer(query, ep, ef, 0)

    // Take top k results
    if len(candidates) > k {
        candidates = candidates[:k]
    }

    // Convert to SearchResult
    results := make([]SearchResult, len(candidates))
    for i, nodeID := range candidates {
        node := idx.nodes[nodeID]
        results[i] = SearchResult{
            RowID:    node.RowID(),
            Distance: query.CosineDistance(node.Vector()),
        }
    }

    // Sort by distance (should already be sorted, but ensure)
    for i := 0; i < len(results)-1; i++ {
        for j := i + 1; j < len(results); j++ {
            if results[j].Distance < results[i].Distance {
                results[i], results[j] = results[j], results[i]
            }
        }
    }

    return results, nil
}
```

**Step 4: Run test to verify it passes**

Run: `go test ./pkg/hnsw/... -v -run TestSearch`
Expected: PASS

**Step 5: Commit**

```bash
git add pkg/hnsw/
git commit -m "feat(hnsw): add SearchKNN and SearchKNNWithEf methods"
```

---

## Phase 4 and Beyond

The implementation plan continues with these phases:

### Phase 4: SQL Parser (Tasks 4.1-4.5)
- Lexer/tokenizer
- Parser for CREATE TABLE, INSERT, SELECT
- AST structures
- Query compiler to bytecode

### Phase 5: VDBE Execution Engine (Tasks 5.1-5.4)
- Opcode definitions
- Virtual machine
- Cursor operations
- Expression evaluation

### Phase 6: MVCC Transactions (Tasks 6.1-6.3)
- Transaction manager
- Version chains
- Conflict detection

### Phase 7: WAL (Tasks 7.1-7.2)
- Write-ahead log structure
- Checkpoint and recovery

### Phase 8: Vector SQL Functions (Tasks 8.1-8.3)
- VECTOR column type
- vector_quantize function
- vector_quantize_scan virtual table

### Phase 9: Database API (Tasks 9.1-9.2)
- High-level Database type
- Query/Exec interface

### Phase 10: CLI Tool (Task 10.1)
- Command-line interface

---

## Summary

This plan covers the foundational layers (Phase 1-3) in detail with TDD-style tasks. Each task is:
- 2-5 minutes of focused work
- Test first, then implement
- Commit after each task passes

Continue with Phases 4-10 following the same pattern. The full implementation is estimated at 150-200 tasks.
