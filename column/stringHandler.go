package column

import (
	"fmt"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
)

type StringHandler struct {
	field arrow.Field
	items *[]string
	index int
}

func NewStringHandler(name string, index int, nullable bool) *StringHandler {
	field := arrow.Field{
		Name:     name,
		Type:     arrow.BinaryTypes.String,
		Nullable: nullable,
	}

	items := make([]string, 0)

	return &StringHandler{
		field: field,
		items: &items,
		index: index,
	}
}

func (h *StringHandler) GetIndex() int {
	return h.index
}

func (h *StringHandler) Add(v any) error {
	if v == nil {
		*h.items = append(*h.items, "")
		return nil
	}
	xx, ok := v.(*[]byte)
	if ok {
		*h.items = append(*h.items, string(*xx))
		return nil
	}

	item, ok := v.(*string)
	if !ok {
		return fmt.Errorf("cannot convert %p to string ", v)
	}

	*h.items = append(*h.items, *item)
	return nil
}

func (h *StringHandler) Build(builder *array.RecordBuilder) {
	builder.Field(h.index).(*array.StringBuilder).AppendValues(*h.items, nil)
}

func (h *StringHandler) GetArrowField() arrow.Field {
	return h.field
}

func (h *StringHandler) GetValue() any {
	return new(string)
}
