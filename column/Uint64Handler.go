package column

import (
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

type Uint64Handler struct {
	field      arrow.Field
	index      int
	column     *arrow.Column
	chunkIndex int
	pos        int
}

func NewUint64Handler(name string, index int, nullable bool) *Uint64Handler {
	return &Uint64Handler{
		field: arrow.Field{
			Name:     name,
			Type:     arrow.PrimitiveTypes.Uint64,
			Nullable: nullable,
		},
		index: index,
	}
}

func (h *Uint64Handler) SetColumn(column *arrow.Column) {
	h.column = column
	h.Reset()
}

func (h *Uint64Handler) Next() bool {
	h.pos++
	chunks := h.column.Data().Chunks()
	// 如果 position 超了，但是 chunkIndex 还有，改到下一个
	if h.pos >= chunks[h.chunkIndex].Len() {
		if h.chunkIndex < len(chunks)-1 {
			h.chunkIndex++
			h.pos = 0
			return true
		} else {
			return false
		}
	}
	return h.pos < chunks[h.chunkIndex].Len()
}

func (h *Uint64Handler) Value() any {
	chunks := h.column.Data().Chunks()
	chunk := chunks[h.chunkIndex].(*array.Uint64)
	if chunk.IsNull(h.pos) {
		return nil
	}
	return chunk.Value(h.pos)
}
func (h *Uint64Handler) Reset() {
	h.chunkIndex = 0
	h.pos = -1
}

func (h *Uint64Handler) GetScanType() any {
	return new(uint64)
}

func (h *Uint64Handler) GetArrowField() arrow.Field {
	return h.field
}
