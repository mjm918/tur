// pkg/hnsw/serialize.go
package hnsw

import (
	"encoding/binary"
	"errors"
	"io"
	"math"

	"tur/pkg/types"
)

var (
	ErrInvalidMagic   = errors.New("invalid HNSW magic number")
	ErrInvalidVersion = errors.New("unsupported HNSW version")
	ErrCorruptedData  = errors.New("corrupted HNSW data")
)

const (
	hnswMagic   uint32 = 0x48535748 // "HSWH" (HNSW reversed)
	hnswVersion uint32 = 1
)

// Header layout:
// [0-3]   Magic (4 bytes)
// [4-7]   Version (4 bytes)
// [8-11]  M (4 bytes)
// [12-15] MMax0 (4 bytes)
// [16-19] EfConstruction (4 bytes)
// [20-23] EfSearch (4 bytes)
// [24-27] Dimension (4 bytes)
// [28-35] ML (8 bytes, float64)
// [36-43] EntryPoint (8 bytes)
// [44-47] MaxLevel (4 bytes)
// [48-55] NextID (8 bytes)
// [56-63] NodeCount (8 bytes)
// [64]    Flags (1 byte: bit 0 = UseHeuristic, bit 1 = ExtendCandidates)
// [65-71] Reserved (7 bytes for future use)
// Total header: 72 bytes

const headerSize = 72

// Serialize writes the index to a writer
func (idx *Index) Serialize(w io.Writer) error {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	// Write header
	header := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(header[0:4], hnswMagic)
	binary.LittleEndian.PutUint32(header[4:8], hnswVersion)
	binary.LittleEndian.PutUint32(header[8:12], uint32(idx.config.M))
	binary.LittleEndian.PutUint32(header[12:16], uint32(idx.config.MMax0))
	binary.LittleEndian.PutUint32(header[16:20], uint32(idx.config.EfConstruction))
	binary.LittleEndian.PutUint32(header[20:24], uint32(idx.config.EfSearch))
	binary.LittleEndian.PutUint32(header[24:28], uint32(idx.config.Dimension))
	binary.LittleEndian.PutUint64(header[28:36], math.Float64bits(idx.config.ML))
	binary.LittleEndian.PutUint64(header[36:44], idx.entryPoint)
	binary.LittleEndian.PutUint32(header[44:48], uint32(idx.maxLevel))
	binary.LittleEndian.PutUint64(header[48:56], idx.nextID)
	binary.LittleEndian.PutUint64(header[56:64], uint64(len(idx.nodes)))

	// Write flags
	var flags byte
	if idx.config.UseHeuristic {
		flags |= 0x01
	}
	if idx.config.ExtendCandidates {
		flags |= 0x02
	}
	header[64] = flags
	// bytes 65-71 are reserved

	if _, err := w.Write(header); err != nil {
		return err
	}

	// Write nodes
	for _, node := range idx.nodes {
		if err := serializeNode(w, node); err != nil {
			return err
		}
	}

	return nil
}

// serializeNode writes a single node to the writer
// Node layout:
// [0-7]   NodeID (8 bytes)
// [8-15]  RowID (8 bytes)
// [16-19] Level (4 bytes)
// [20-23] VectorSize (4 bytes) - number of bytes for vector data
// [24...] Vector data (variable)
// For each level 0..Level:
//
//	[4 bytes] NeighborCount
//	[8 bytes * count] Neighbor IDs
func serializeNode(w io.Writer, node *HNSWNode) error {
	// Write node ID and RowID
	buf := make([]byte, 24)
	binary.LittleEndian.PutUint64(buf[0:8], node.id)
	binary.LittleEndian.PutUint64(buf[8:16], uint64(node.rowID))
	binary.LittleEndian.PutUint32(buf[16:20], uint32(node.level))

	// Serialize vector
	vecBytes := node.vector.ToBytes()
	binary.LittleEndian.PutUint32(buf[20:24], uint32(len(vecBytes)))

	if _, err := w.Write(buf); err != nil {
		return err
	}
	if _, err := w.Write(vecBytes); err != nil {
		return err
	}

	// Write neighbors for each level
	for l := 0; l <= node.level; l++ {
		neighbors := node.Neighbors(l)
		countBuf := make([]byte, 4)
		binary.LittleEndian.PutUint32(countBuf, uint32(len(neighbors)))
		if _, err := w.Write(countBuf); err != nil {
			return err
		}

		for _, nid := range neighbors {
			neighborBuf := make([]byte, 8)
			binary.LittleEndian.PutUint64(neighborBuf, nid)
			if _, err := w.Write(neighborBuf); err != nil {
				return err
			}
		}
	}

	return nil
}

// Deserialize reads an index from a reader
func Deserialize(r io.Reader) (*Index, error) {
	// Read header
	header := make([]byte, headerSize)
	if _, err := io.ReadFull(r, header); err != nil {
		return nil, err
	}

	// Validate magic and version
	magic := binary.LittleEndian.Uint32(header[0:4])
	if magic != hnswMagic {
		return nil, ErrInvalidMagic
	}

	version := binary.LittleEndian.Uint32(header[4:8])
	if version != hnswVersion {
		return nil, ErrInvalidVersion
	}

	// Read flags
	flags := header[64]

	// Read config
	config := Config{
		M:                int(binary.LittleEndian.Uint32(header[8:12])),
		MMax0:            int(binary.LittleEndian.Uint32(header[12:16])),
		EfConstruction:   int(binary.LittleEndian.Uint32(header[16:20])),
		EfSearch:         int(binary.LittleEndian.Uint32(header[20:24])),
		Dimension:        int(binary.LittleEndian.Uint32(header[24:28])),
		ML:               math.Float64frombits(binary.LittleEndian.Uint64(header[28:36])),
		UseHeuristic:     flags&0x01 != 0,
		ExtendCandidates: flags&0x02 != 0,
	}

	entryPoint := binary.LittleEndian.Uint64(header[36:44])
	maxLevel := int(binary.LittleEndian.Uint32(header[44:48]))
	nextID := binary.LittleEndian.Uint64(header[48:56])
	nodeCount := binary.LittleEndian.Uint64(header[56:64])

	// Create index
	idx := &Index{
		config:     config,
		nodes:      make(map[uint64]*HNSWNode, nodeCount),
		entryPoint: entryPoint,
		maxLevel:   maxLevel,
		nextID:     nextID,
	}

	// Read nodes
	for i := uint64(0); i < nodeCount; i++ {
		node, err := deserializeNode(r)
		if err != nil {
			return nil, err
		}
		idx.nodes[node.id] = node
	}

	return idx, nil
}

// deserializeNode reads a single node from the reader
func deserializeNode(r io.Reader) (*HNSWNode, error) {
	// Read node header
	buf := make([]byte, 24)
	if _, err := io.ReadFull(r, buf); err != nil {
		return nil, err
	}

	nodeID := binary.LittleEndian.Uint64(buf[0:8])
	rowID := int64(binary.LittleEndian.Uint64(buf[8:16]))
	level := int(binary.LittleEndian.Uint32(buf[16:20]))
	vecSize := binary.LittleEndian.Uint32(buf[20:24])

	// Read vector
	vecBytes := make([]byte, vecSize)
	if _, err := io.ReadFull(r, vecBytes); err != nil {
		return nil, err
	}
	vector, err := types.VectorFromBytes(vecBytes)
	if err != nil {
		return nil, err
	}

	// Create node
	node := &HNSWNode{
		id:        nodeID,
		rowID:     rowID,
		vector:    vector,
		level:     level,
		neighbors: make([][]uint64, level+1),
	}

	// Read neighbors for each level
	for l := 0; l <= level; l++ {
		countBuf := make([]byte, 4)
		if _, err := io.ReadFull(r, countBuf); err != nil {
			return nil, err
		}
		count := binary.LittleEndian.Uint32(countBuf)

		neighbors := make([]uint64, count)
		for i := uint32(0); i < count; i++ {
			neighborBuf := make([]byte, 8)
			if _, err := io.ReadFull(r, neighborBuf); err != nil {
				return nil, err
			}
			neighbors[i] = binary.LittleEndian.Uint64(neighborBuf)
		}
		node.neighbors[l] = neighbors
	}

	return node, nil
}

// SerializeToBytes serializes the index to a byte slice
func (idx *Index) SerializeToBytes() ([]byte, error) {
	// Estimate size
	estimatedSize := headerSize + len(idx.nodes)*(32+idx.config.Dimension*4+100)
	buf := make([]byte, 0, estimatedSize)

	// Use a bytes.Buffer for efficient writing
	writer := &bytesWriter{buf: buf}
	if err := idx.Serialize(writer); err != nil {
		return nil, err
	}

	return writer.buf, nil
}

// DeserializeFromBytes deserializes an index from a byte slice
func DeserializeFromBytes(data []byte) (*Index, error) {
	reader := &bytesReader{data: data}
	return Deserialize(reader)
}

// bytesWriter implements io.Writer for a byte slice
type bytesWriter struct {
	buf []byte
}

func (w *bytesWriter) Write(p []byte) (n int, err error) {
	w.buf = append(w.buf, p...)
	return len(p), nil
}

// bytesReader implements io.Reader for a byte slice
type bytesReader struct {
	data []byte
	pos  int
}

func (r *bytesReader) Read(p []byte) (n int, err error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n = copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}
