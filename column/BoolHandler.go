package column

import (
	"fmt"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
)

type BoolHandler struct {
	field arrow.Field
	items *[]bool
	index int
}

func (h *BoolHandler) GetIndex() int {
	return h.index
}

func (h *BoolHandler) GetValue() any {
	return new(bool)
}

func (h *BoolHandler) Add(v any) error {
	if v == nil {
		*h.items = append(*h.items, false)
		return nil
	}
	var res bool
	if item, ok := v.(*bool); ok {
		res = *item
	} else {
		return fmt.Errorf("cannot convert %v to bool", v)
	}

	*h.items = append(*h.items, res)
	return nil
}

func NewBoolHandler(name string, index int, nullable bool) *BoolHandler {
	field := arrow.Field{
		Name:     name,
		Type:     arrow.FixedWidthTypes.Boolean,
		Nullable: nullable,
	}

	items := make([]bool, 0)

	return &BoolHandler{
		field: field,
		items: &items,
		index: index,
	}
}

func (h *BoolHandler) Build(builder *array.RecordBuilder) {
	builder.Field(h.index).(*array.BooleanBuilder).AppendValues(*h.items, nil)
}

func (h *BoolHandler) GetArrowField() arrow.Field {
	return h.field
}
