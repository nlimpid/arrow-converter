package column

import (
	"fmt"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
)

type Int32Handler struct {
	field arrow.Field
	items *[]int32
	index int
}

func (h *Int32Handler) GetIndex() int {
	return h.index
}

func (h *Int32Handler) GetValue() any {
	return new(int32)
}

func (h *Int32Handler) Add(v any) error {
	if v == nil {
		*h.items = append(*h.items, 0)
		return nil
	}
	var res int32
	if item, ok := v.(*int32); ok {
		res = *item
	} else {
		return fmt.Errorf("cannot convert %v to int32", v)
	}

	*h.items = append(*h.items, res)
	return nil
}

func NewInt32Handler(name string, index int, nullable bool) *Int32Handler {
	field := arrow.Field{
		Name:     name,
		Type:     arrow.PrimitiveTypes.Int32,
		Nullable: nullable,
	}

	items := make([]int32, 0)

	return &Int32Handler{
		field: field,
		items: &items,
		index: index,
	}
}

func (h *Int32Handler) Build(builder *array.RecordBuilder) {
	builder.Field(h.index).(*array.Int32Builder).AppendValues(*h.items, nil)
}

func (h *Int32Handler) GetArrowField() arrow.Field {
	return h.field
}