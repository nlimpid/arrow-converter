package column

import (
	"fmt"
	"time"

	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/apache/arrow/go/v17/arrow"
)

type TimestampHandler struct {
	field      arrow.Field
	items      []arrow.Timestamp
	valid      []bool
	index      int
	column     *arrow.Column
	chunkIndex int
	pos        int
	unit       arrow.TimeUnit
}

func NewTimestampHandler(name string, index int, nullable bool, unit arrow.TimeUnit) *TimestampHandler {
	return &TimestampHandler{
		field: arrow.Field{
			Name:     name,
			Type:     arrow.FixedWidthTypes.Timestamp_us,
			Nullable: nullable,
		},
		index: index,
		unit:  unit,
	}
}

func (h *TimestampHandler) Build(builder *array.RecordBuilder) {
	builder.Field(h.index).(*array.TimestampBuilder).AppendValues(h.items, h.valid)
}

func (h *TimestampHandler) Add(v any) error {
	if v == nil {
		h.items = append(h.items, arrow.Timestamp(0))
		h.valid = append(h.valid, false)
		return nil
	}
	h.valid = append(h.valid, true)
	switch val := v.(type) {
	case time.Time:
		timeVal, err := arrow.TimestampFromTime(val, h.unit)
		if err != nil {
			return err
		}
		h.items = append(h.items, timeVal)
	case *time.Time:
		timeVal, err := arrow.TimestampFromTime(*val, h.unit)
		if err != nil {
			return err
		}
		h.items = append(h.items, timeVal)
	default:
		return fmt.Errorf("cannot convert %v of type %T to timestamp", v, v)
	}
	return nil
}

func (h *TimestampHandler) SetColumn(column *arrow.Column) {
	h.column = column
	h.Reset()
}

func (h *TimestampHandler) Next() bool {
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

func (h *TimestampHandler) Value() any {
	chunks := h.column.Data().Chunks()
	chunk := chunks[h.chunkIndex].(*array.Timestamp)
	if chunk.IsNull(h.pos) {
		return nil
	}
	arrowVal := chunk.Value(h.pos)

	return arrowVal.ToTime(h.unit)
}
func (h *TimestampHandler) Reset() {
	h.chunkIndex = 0
	h.pos = -1
}

func (h *TimestampHandler) GetScanType() any {
	return new(time.Time)
}

func (h *TimestampHandler) GetArrowField() arrow.Field {
	return h.field
}
