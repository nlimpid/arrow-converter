package column

import (
	"fmt"
	"log/slog"
	"math/big"

	"github.com/apache/arrow/go/v17/arrow/decimal128"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/shopspring/decimal"
)

type DecimalHandler struct {
	field arrow.Field
	items []*big.Int
	valid []bool
	index int

	column     *arrow.Column
	chunkIndex int
	pos        int
	precision  int32
	scale      int32
}

func NewDecimalHandler(name string, index int, nullable bool, precision, scale int32) *DecimalHandler {
	slog.Info("column.NewDecimalHandler", "precision", precision, "scale", scale)
	field := arrow.Field{
		Name:     name,
		Type:     &arrow.Decimal128Type{Precision: precision, Scale: scale},
		Nullable: nullable,
	}

	return &DecimalHandler{
		field:     field,
		items:     make([]*big.Int, 0),
		valid:     make([]bool, 0),
		index:     index,
		precision: precision,
		scale:     scale,
	}
}

func (h *DecimalHandler) Add(v any) error {
	if v == nil {
		h.items = append(h.items, new(big.Int))
		h.valid = append(h.valid, false)
		return nil
	}
	h.valid = append(h.valid, true)
	switch val := v.(type) {
	case decimal.Decimal:
		h.items = append(h.items, val.Coefficient())
	case *decimal.Decimal:
		h.items = append(h.items, val.Coefficient())
	default:
		return fmt.Errorf("cannot convert %v of type %T to Decimal", v, v)
	}
	return nil
}

func (h *DecimalHandler) Build(builder *array.RecordBuilder) {
	decimalBuilder := builder.Field(h.index).(*array.Decimal128Builder)
	for i, item := range h.items {
		if h.valid[i] {
			decimalBuilder.Append(decimal128.FromBigInt(item))
		} else {
			decimalBuilder.AppendNull()
		}
	}
}

func (h *DecimalHandler) GetScanType() any {
	return new(decimal.Decimal)
}

func (h *DecimalHandler) SetColumn(column *arrow.Column) {
	h.column = column
	h.Reset()
}

func (h *DecimalHandler) Next() bool {
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

func (h *DecimalHandler) Value() any {
	chunks := h.column.Data().Chunks()
	chunk := chunks[h.chunkIndex].(*array.Decimal128)
	if chunk.IsNull(h.pos) {
		return nil
	}
	arrowVal := chunk.Value(h.pos)
	arrowVal.ToFloat64(h.scale)
	slog.Info("decimal value", "value", arrowVal.ToString(h.scale))
	return decimal.NewFromFloat(arrowVal.ToFloat64(h.scale))
	//return decimal.NewFromBigInt(arrowVal.BigInt(), h.scale)
}

func (h *DecimalHandler) Reset() {
	h.chunkIndex = 0
	h.pos = -1
}

func (h *DecimalHandler) GetArrowField() arrow.Field {
	return h.field
}
