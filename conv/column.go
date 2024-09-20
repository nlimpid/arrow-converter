package conv

import (
	"fmt"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"google.golang.org/genproto/googleapis/type/decimal"
	"log/slog"
	"reflect"
	"time"
)

type ArrowHandlerManager struct {
	handlers []ArrowHandler
}

// AddHandler 向管理器中添加一个新的 ArrowHandler
func (m *ArrowHandlerManager) AddHandler(handler ArrowHandler) {
	m.handlers = append(m.handlers, handler)
}

// BuildRecord 构建 Arrow RecordBuilder 中的所有字段
func (m *ArrowHandlerManager) BuildRecord(builder *array.RecordBuilder) {
	for _, handler := range m.handlers {
		handler.Build(builder)
	}
}

// GetSchema 获取所有字段的 Arrow Schema
func (m *ArrowHandlerManager) GetSchema() *arrow.Schema {
	fields := make([]arrow.Field, len(m.handlers))
	for i, handler := range m.handlers {
		fields[i] = handler.GetArrowField()
	}
	return arrow.NewSchema(fields, nil)
}

// ArrowHandler 是一个泛型接口，用于处理不同类型的 Arrow 字段
type ArrowHandler interface {
	GetArrowField() arrow.Field
	GetFieldName() string
	GetIndex() int
	Add(v any) error
	Build(builder *array.RecordBuilder)
}

// GenericArrowHandler 是一个泛型实现，适用于各种基础数据类型
type GenericArrowHandler[T any] struct {
	field     arrow.Field
	items     []T
	index     int
	FieldName string
}

// GetArrowField 返回 Arrow 字段信息
func (h *GenericArrowHandler[T]) GetArrowField() arrow.Field {
	return h.field
}

// GetArrowField 返回 Arrow 字段信息
func (h *GenericArrowHandler[T]) GetFieldName() string {
	return h.FieldName
}

// GetIndex 返回字段索引
func (h *GenericArrowHandler[T]) GetIndex() int {
	return h.index
}

// Add 向处理器添加数据
func (h *GenericArrowHandler[T]) Add(v any) error {
	if v == nil {
		var zero T
		h.items = append(h.items, zero)
		return nil
	}
	if val, ok := v.(T); ok {
		h.items = append(h.items, val)
		return nil
	}

	val, ok := v.(*T)
	if !ok {
		return fmt.Errorf("cannot convert %v to %T", v, new(T))
	}
	h.items = append(h.items, *val)

	return nil
}

// Build 向 Arrow RecordBuilder 添加数据
func (h *GenericArrowHandler[T]) Build(builder *array.RecordBuilder) {
	switch h.field.Type.ID() {
	case arrow.INT32:
		b := builder.Field(h.index).(*array.Int32Builder)
		ints, ok := any(h.items).([]int32)
		if !ok {
			return
		}
		b.AppendValues(ints, nil)
	case arrow.FLOAT64:
		b := builder.Field(h.index).(*array.Float64Builder)
		floats, ok := any(h.items).([]float64)
		if !ok {
			return
		}
		b.AppendValues(floats, nil)
	case arrow.BOOL:
		b := builder.Field(h.index).(*array.BooleanBuilder)
		bools, ok := any(h.items).([]bool)
		if !ok {
			return
		}
		b.AppendValues(bools, nil)
	case arrow.STRING:
		b := builder.Field(h.index).(*array.StringBuilder)
		strs, ok := any(h.items).([]string)
		if !ok {
			return
		}
		b.AppendValues(strs, nil)
	case arrow.DATE32:
		b := builder.Field(h.index).(*array.Date32Builder)
		strs, ok := any(h.items).([]time.Time)
		if ok {
			vv := make([]arrow.Date32, 0, len(strs))
			for _, v := range strs {
				vv = append(vv, arrow.Date32FromTime(v))
			}
			b.AppendValues(vv, nil)

		}
	// 根据需要添加更多类型的处理
	default:
	}
}

// NewGenericArrowHandler 创建一个新的泛型 ArrowHandler
func NewGenericArrowHandler[T any](info *StructArrowInfo, index int, nullable bool) (*GenericArrowHandler[T], error) {
	fieldType, err := getArrowDataType(reflect.TypeOf(new(T)), info.ArrowTypeName)
	if err != nil {
		return nil, err
	}

	field := arrow.Field{
		Name:     info.ArrowName,
		Type:     fieldType,
		Nullable: nullable,
	}

	return &GenericArrowHandler[T]{
		FieldName: info.StructFieldName,
		field:     field,
		items:     make([]T, 0),
		index:     index,
	}, nil
}

// getArrowDataType 根据反射类型返回对应的 Arrow 类型
func getArrowDataType(t reflect.Type, arrowType string) (arrow.DataType, error) {
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}
	switch t.Kind() {
	case reflect.Bool:
		return arrow.FixedWidthTypes.Boolean, nil
	case reflect.Int8:
		return arrow.PrimitiveTypes.Int8, nil
	case reflect.Int16:
		return arrow.PrimitiveTypes.Int16, nil
	case reflect.Int32:
		return arrow.PrimitiveTypes.Int32, nil
	case reflect.Int64:
		return arrow.PrimitiveTypes.Int64, nil
	case reflect.Uint8:
		return arrow.PrimitiveTypes.Uint8, nil
	case reflect.Uint16:
		return arrow.PrimitiveTypes.Uint16, nil
	case reflect.Uint32:
		return arrow.PrimitiveTypes.Uint32, nil
	case reflect.Uint64:
		return arrow.PrimitiveTypes.Uint64, nil
	case reflect.Float32:
		return arrow.PrimitiveTypes.Float32, nil
	case reflect.Float64:
		return arrow.PrimitiveTypes.Float64, nil
	case reflect.String:
		return arrow.BinaryTypes.String, nil
	case reflect.Struct:
		if t == reflect.TypeOf(time.Time{}) {
			switch arrowType {
			case "date_32":
				return arrow.FixedWidthTypes.Date32, nil
			}

			slog.Info("timestamp", "time", t)
			return arrow.FixedWidthTypes.Timestamp_us, nil
		}
		if t == reflect.TypeOf(decimal.Decimal{}) {
			// 默认使用 128 位精度，可以根据需要调整
			return &arrow.Decimal128Type{Precision: 38, Scale: 10}, nil
		}
	case reflect.Slice:
		if t.Elem().Kind() == reflect.Uint8 {
			return arrow.BinaryTypes.Binary, nil
		}
	default:
		slog.Error("data is", "kind", t.Kind())
	}

	return nil, fmt.Errorf("unsupported field type: %s", t.Kind())
}
