package column

import (
	"fmt"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
)

type Uint64Handler struct {
	field arrow.Field
	items *[]int64
	index int
}

func (h *Uint64Handler) GetIndex() int {
	return h.index
}

func (h *Uint64Handler) GetValue() any {
	return new(int64)
}

func (h *Uint64Handler) Add(v any) error {
	if v == nil {
		*h.items = append(*h.items, 0)
		return nil
	}
	var res int64
	if item, ok := v.(*int64); ok {
		res = *item
	} else {
		return fmt.Errorf("cannot convert %v to int64", v)
	}

	*h.items = append(*h.items, res)
	return nil
}

func NewUint64Handler(name string, index int, nullable bool) *Uint64Handler {
	field := arrow.Field{
		Name:     name,
		Type:     arrow.PrimitiveTypes.Int64,
		Nullable: nullable,
	}

	items := make([]int64, 0)

	return &Uint64Handler{
		field: field,
		items: &items,
		index: index,
	}
}

func (h *Uint64Handler) Build(builder *array.RecordBuilder) {
	builder.Field(h.index).(*array.Int64Builder).AppendValues(*h.items, nil)
}

func (h *Uint64Handler) GetArrowField() arrow.Field {
	return h.field
}