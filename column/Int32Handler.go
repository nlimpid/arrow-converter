package column

import (
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

type Int32Handler struct {
	field      arrow.Field
	items      []int32
	valid      []bool
	index      int
	column     *arrow.Column
	chunkIndex int
	pos        int
}

func NewInt32Handler(name string, index int, nullable bool) *Int32Handler {
	return &Int32Handler{
		field: arrow.Field{
			Name:     name,
			Type:     arrow.PrimitiveTypes.Int32,
			Nullable: nullable,
		},
		index: index,
	}
}

func (h *Int32Handler) Build(builder *array.RecordBuilder) {
	builder.Field(h.index).(*array.Int32Builder).AppendValues(h.items, h.valid)
}

func (h *Int32Handler) Add(v any) error {
	if v == nil {
		h.items = append(h.items, 0)
		h.valid = append(h.valid, false)
		return nil
	}
	h.valid = append(h.valid, true)
	switch val := v.(type) {
	case int32:
		h.items = append(h.items, val)
	case int:
		h.items = append(h.items, int32(val))
	default:
		return fmt.Errorf("cannot convert %v of type %T to float64", v, v)
	}
	return nil
}

func (h *Int32Handler) SetColumn(column *arrow.Column) {
	h.column = column
	h.Reset()
}

func (h *Int32Handler) Next() bool {
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

func (h *Int32Handler) Value() any {
	chunks := h.column.Data().Chunks()
	chunk := chunks[h.chunkIndex].(*array.Int32)
	if chunk.IsNull(h.pos) {
		return nil
	}
	return chunk.Value(h.pos)
}

func (h *Int32Handler) Reset() {
	h.chunkIndex = 0
	h.pos = -1
}

func (h *Int32Handler) GetScanType() any {
	return new(int32)
}

func (h *Int32Handler) GetArrowField() arrow.Field {
	return h.field
}
