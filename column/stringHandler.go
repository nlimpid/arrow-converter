package column

import (
	"fmt"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

type StringHandler struct {
	field arrow.Field
	index int
	items []string
	valid []bool

	column     *arrow.Column
	chunkIndex int
	pos        int
}

func NewStringHandler(name string, index int, nullable bool) *StringHandler {
	field := arrow.Field{
		Name:     name,
		Type:     arrow.BinaryTypes.String,
		Nullable: nullable,
	}

	return &StringHandler{
		field: field,
		index: index,
	}
}

func (h *StringHandler) Add(v any) error {
	if v == nil {
		h.items = append(h.items, "")
		h.valid = append(h.valid, false)
		return nil
	}
	h.valid = append(h.valid, true)
	switch val := v.(type) {
	case string:
		h.items = append(h.items, val)
	case []byte:
		h.items = append(h.items, string(val))
	case *[]byte:
		h.items = append(h.items, string((*val)))
	case *string:
		h.items = append(h.items, *val)
	default:
		return fmt.Errorf("cannot convert %v of type %T to float64", v, v)
	}
	return nil
}

func (h *StringHandler) Build(builder *array.RecordBuilder) {
	builder.Field(h.index).(*array.StringBuilder).AppendValues(h.items, h.valid)
}

func (h *StringHandler) GetScanType() any {
	return new(string)
}

func (h *StringHandler) SetColumn(column *arrow.Column) {
	h.column = column

	if h.column == nil || (h.column.Data() == nil) || len(h.column.Data().Chunks()) == 0 {
		panic(h.column)
	}

	h.Reset()
}

func (h *StringHandler) Next() bool {
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

func (h *StringHandler) Value() any {
	chunks := h.column.Data().Chunks()
	chunk := chunks[h.chunkIndex].(*array.String)
	if chunk.IsNull(h.pos) {
		return nil
	}
	return chunk.Value(h.pos)
}

func (h *StringHandler) Reset() {
	h.chunkIndex = 0
	h.pos = -1
}

func (h *StringHandler) GetArrowField() arrow.Field {
	return h.field
}
