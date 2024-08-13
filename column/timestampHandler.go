package column

import (
	"fmt"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"time"
)

type TimeHandler struct {
	field arrow.Field
	items *[]arrow.Timestamp
	index int
}

func (h *TimeHandler) GetIndex() int {
	return h.index
}

func (h *TimeHandler) GetValue() any {
	return new(time.Time)
}

func (h *TimeHandler) Add(v any) error {
	if v == nil {
		*h.items = append(*h.items, 0)
		return nil
	}
	var res time.Time
	if item, ok := v.(*time.Time); ok {
		res = *item
	} else {
		return fmt.Errorf("cannot convert %v to time.Time ", v)
	}
	it, _ := arrow.TimestampFromTime(res, arrow.Second)
	*h.items = append(*h.items, it)
	return nil
}

func NewTimeHandler(name string, index int, nullable bool) *TimeHandler {
	field := arrow.Field{
		Name:     name,
		Type:     arrow.FixedWidthTypes.Timestamp_s,
		Nullable: nullable,
	}
	items := make([]arrow.Timestamp, 0)
	return &TimeHandler{
		field: field,
		items: &items,
		index: index,
	}
}

func (h *TimeHandler) Build(builder *array.RecordBuilder) {
	builder.Field(h.index).(*array.TimestampBuilder).AppendValues(*h.items, nil)
}

func (h *TimeHandler) GetArrowField() arrow.Field {
	return h.field
}
