package column

import (
	"fmt"
	"log/slog"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

type Int64Handler struct {
	field      arrow.Field
	items      []int64
	valid      []bool
	index      int
	column     *arrow.Column
	chunkIndex int
	pos        int
}

func NewInt64Handler(name string, index int, nullable bool) *Int64Handler {
	return &Int64Handler{
		field: arrow.Field{
			Name:     name,
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: nullable,
		},
		index: index,
	}
}

func (h *Int64Handler) Build(builder *array.RecordBuilder) {
	builder.Field(h.index).(*array.Int64Builder).AppendValues(h.items, h.valid)
}

func (h *Int64Handler) Add(v any) error {
	if v == nil {
		h.items = append(h.items, 0)
		h.valid = append(h.valid, false)
		return nil
	}
	h.valid = append(h.valid, true)
	switch val := v.(type) {
	case int64:
		h.items = append(h.items, val)
	case *int64:
		h.items = append(h.items, *val)
	default:
		return fmt.Errorf("cannot convert %v of type %T to float64", v, v)
	}
	return nil
}
func (h *Int64Handler) SetColumn(column *arrow.Column) {
	h.column = column
	slog.Info("int64 handler set column", "index", h.column.Name(), "column", h.column.Data())
	h.Reset()
}

func (h *Int64Handler) Next() bool {
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

func (h *Int64Handler) Value() any {
	chunks := h.column.Data().Chunks()
	chunk := chunks[h.chunkIndex].(*array.Int64)
	if chunk.IsNull(h.pos) {
		return nil
	}
	return chunk.Value(h.pos)
}

func (h *Int64Handler) Reset() {
	h.chunkIndex = 0
	h.pos = -1
}

func (h *Int64Handler) GetScanType() any {
	return new(int64)
}

func (h *Int64Handler) GetArrowField() arrow.Field {
	return h.field
}
