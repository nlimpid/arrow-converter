package column

import (
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

type BoolHandler struct {
	field arrow.Field
	items []bool
	valid []bool
	index int

	column     *arrow.Column
	chunkIndex int
	pos        int
}

func NewBoolHandler(name string, index int, nullable bool) *BoolHandler {
	field := arrow.Field{
		Name:     name,
		Type:     arrow.FixedWidthTypes.Boolean,
		Nullable: nullable,
	}

	return &BoolHandler{
		field: field,
		items: make([]bool, 0),
		valid: make([]bool, 0),
		index: index,
	}
}

func (h *BoolHandler) Add(v any) error {
	if v == nil {
		h.items = append(h.items, false)
		h.valid = append(h.valid, false)
		return nil
	}
	h.valid = append(h.valid, true)
	switch val := v.(type) {
	case bool:
		h.items = append(h.items, val)
	case *bool:
		if val == nil {
			h.items = append(h.items, false)
			h.valid[len(h.valid)-1] = false
		} else {
			h.items = append(h.items, *val)
		}
	default:
		return fmt.Errorf("cannot convert %v of type %T to bool", v, v)
	}
	return nil
}

func (h *BoolHandler) Build(builder *array.RecordBuilder) {
	builder.Field(h.index).(*array.BooleanBuilder).AppendValues(h.items, h.valid)
}

func (h *BoolHandler) GetScanType() any {
	return new(bool)
}

func (h *BoolHandler) SetColumn(column *arrow.Column) {
	h.column = column
	h.Reset()
}
func (h *BoolHandler) Next() bool {
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

func (h *BoolHandler) Value() any {
	chunks := h.column.Data().Chunks()
	chunk := chunks[h.chunkIndex].(*array.Boolean)
	if chunk.IsNull(h.pos) {
		return nil
	}
	return chunk.Value(h.pos)
}
func (h *BoolHandler) Reset() {
	h.chunkIndex = 0
	h.pos = -1
}

func (h *BoolHandler) GetArrowField() arrow.Field {
	return h.field
}
