package column

import (
	"fmt"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
)

type Float64Handler struct {
	field arrow.Field
	items *[]float64
	index int
}

func (h *Float64Handler) GetIndex() int {
	return h.index
}

func (h *Float64Handler) GetValue() any {
	return new(float64)
}

func (h *Float64Handler) Add(v any) error {
	if v == nil {
		*h.items = append(*h.items, 0)
		return nil
	}
	var res float64
	if item, ok := v.(*float64); ok {
		res = *item
	} else {
		return fmt.Errorf("cannot convert %v to float64", v)
	}

	*h.items = append(*h.items, res)
	return nil
}

func NewFloat64Handler(name string, index int, nullable bool) *Float64Handler {
	field := arrow.Field{
		Name:     name,
		Type:     arrow.PrimitiveTypes.Float64,
		Nullable: nullable,
	}

	items := make([]float64, 0)

	return &Float64Handler{
		field: field,
		items: &items,
		index: index,
	}
}

func (h *Float64Handler) Build(builder *array.RecordBuilder) {
	builder.Field(h.index).(*array.Float64Builder).AppendValues(*h.items, nil)
}

func (h *Float64Handler) GetArrowField() arrow.Field {
	return h.field
}