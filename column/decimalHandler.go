package column

import (
	"fmt"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/decimal128"
	"log/slog"
)

type DecimalHandler struct {
	field arrow.Field
	items *[]decimal128.Num
	valid []bool
	index int
	prec  int32
	scal  int32
}

func (h *DecimalHandler) GetIndex() int {
	return h.index
}

func (h *DecimalHandler) GetValue() any {
	return new([]byte)
}

func (h *DecimalHandler) Add(v any) error {
	if v == nil {
		*h.items = append(*h.items, decimal128.FromI64(0))
		h.valid = append(h.valid, false)
		return nil
	}
	var res []byte
	if item, ok := v.(*[]byte); ok {
		res = *item
	} else {
		return fmt.Errorf("cannot convert %v to []byte ", v)
	}
	n, _ := decimal128.FromString(string(res), h.prec, h.scal)

	*h.items = append(*h.items, n)
	h.valid = append(h.valid, true)

	return nil
}

func NewDecimalHandler(name string, index int, nullable bool, prec, scal int32) *DecimalHandler {
	slog.Debug("new decimal handler", "name", name, "nullable", nullable, "scale", scal)
	decimalType, _ := arrow.NewDecimalType(arrow.DECIMAL128, prec, scal)

	field := arrow.Field{
		Name:     name,
		Type:     decimalType,
		Nullable: true,
	}

	items := make([]decimal128.Num, 0)

	return &DecimalHandler{
		field: field,
		items: &items,
		index: index,
		prec:  prec,
		scal:  scal,
	}
}

func (h *DecimalHandler) Build(builder *array.RecordBuilder) {
	builder.Field(h.index).(*array.Decimal128Builder).AppendValues(*h.items, h.valid)
}

func (h *DecimalHandler) GetArrowField() arrow.Field {
	return h.field
}
