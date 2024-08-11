package column

import (
	"fmt"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

type Int64Handler struct {
	field arrow.Field
	items *[]int64
	index int
}

func (h *Int64Handler) GetIndex() int {
	return h.index
}

func (h *Int64Handler) GetValue() any {
	return new(int64)
}

func (h *Int64Handler) Add(v any) error {
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

func NewInt64Handler(name string, index int, nullable bool) *Int64Handler {
	field := arrow.Field{
		Name:     name,
		Type:     arrow.PrimitiveTypes.Int64,
		Nullable: nullable,
	}

	items := make([]int64, 0)

	return &Int64Handler{
		field: field,
		items: &items,
		index: index,
	}
}

func (h *Int64Handler) Build(builder *array.RecordBuilder) {
	builder.Field(h.index).(*array.Int64Builder).AppendValues(*h.items, nil)
}

func (h *Int64Handler) GetArrowField() arrow.Field {
	return h.field
}
