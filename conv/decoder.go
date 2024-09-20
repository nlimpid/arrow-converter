package conv

import (
	"context"
	"fmt"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	_ "github.com/apache/arrow/go/v18/arrow/ipc"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/nlimpid/arrow-converter/column"
	"io"
	"reflect"
)

type Decoder[T any] struct {
	w             io.Writer
	arrowHandlers []ArrowHandler
}

func NewDecoder[T any](w io.Writer) *Decoder[T] {
	enc := Decoder[T]{
		w:             w,
		arrowHandlers: make([]ArrowHandler, 0),
	}

	return &enc
}

// getFieldIndex 获取字段在 StructArrowInfo 切片中的索引
func getFieldIndex(info []StructArrowInfo, fieldName string) int {
	for i, f := range info {
		if f.StructFieldName == fieldName {
			return i
		}
	}
	return -1
}

// 提供方法添加 ArrowHandler
func (e *Decoder[T]) AddHandler(handler ArrowHandler) {
	e.arrowHandlers = append(e.arrowHandlers, handler)
}

// Decode 方法的实现
func (e *Decoder[T]) Decode(ctx context.Context, v []T) error {
	if err := e.decodeArrow(ctx, v); err != nil {
		return fmt.Errorf("decoding arrow: %w", err)
	}

	return e.buildFile(ctx)
}

// Decode 方法的实现
func (e *Decoder[T]) decodeArrow(ctx context.Context, v []T) error {
	if len(v) == 0 {
		return fmt.Errorf("input slice is empty")
	}

	// 获取结构体字段的 Arrow 信息
	structInfo, err := getStructArrowInfo(&v[0])
	if err != nil {
		return err
	}
	// 初始化 ArrowHandlers
	for _, info := range structInfo {
		var handler ArrowHandler
		switch info.FieldType.ID() {
		case arrow.PrimitiveTypes.Int64.ID():
			handler = column.NewInt64Handler(info.StructFieldName, getFieldIndex(structInfo, info.StructFieldName), false)
		case arrow.PrimitiveTypes.Int32.ID():
			handler = column.NewInt32Handler(info.StructFieldName, getFieldIndex(structInfo, info.StructFieldName), false)
		case arrow.PrimitiveTypes.Float64.ID():
			handler = column.NewFloat64Handler(info.StructFieldName, getFieldIndex(structInfo, info.StructFieldName), false)
		case arrow.FixedWidthTypes.Boolean.ID():
			handler = column.NewBoolHandler(info.StructFieldName, getFieldIndex(structInfo, info.StructFieldName), false)
		case arrow.BinaryTypes.String.ID():
			handler = column.NewStringHandler(info.StructFieldName, getFieldIndex(structInfo, info.StructFieldName), false)
		//case arrow.TimestampType.Timestamp_ms.ID():
		//	handler = column.NewTimestampHandler(info.StructFieldName, getFieldIndex(structInfo, info.StructFieldName), info.FieldType.(*arrow.TimestampType).Nullable)
		//case arrow.Date32.ID():
		//	handler = column.NewDate32Handler(info.StructFieldName, getFieldIndex(structInfo, info.StructFieldName), info.FieldType.(*arrow.Date32Type).Nullable)
		// 根据需要添加更多类型处理
		default:
			return fmt.Errorf("unsupported Arrow data type: %s", info.FieldType.Name())
		}
		e.AddHandler(handler)
	}

	// 遍历结构体切片并添加数据到 ArrowHandlers
	for _, record := range v {
		rv := reflect.ValueOf(record)
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}

		if rv.Kind() != reflect.Struct {
			return fmt.Errorf("slice elements must be structs")
		}

		for _, handler := range e.arrowHandlers {
			fieldValue := rv.FieldByName(handler.GetArrowField().Name)
			//if fieldValue.Kind() == reflect.Ptr && fieldValue.IsNil() {
			//	value = nil
			//}
			//value := fieldValue

			if err := handler.Add(&fieldValue); err != nil {
				return fmt.Errorf("failed to add value for field %s: %v", handler.GetArrowField().Name, err)
			}
		}
	}

	return nil

}

func (e *Decoder[T]) buildFile(ctx context.Context) error {
	arrowHandlers := e.arrowHandlers

	fields := make([]arrow.Field, len(arrowHandlers))
	for i, h := range arrowHandlers {
		fields[i] = h.GetArrowField()
	}
	alloc := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(alloc, arrow.NewSchema(fields, nil))
	for _, h := range arrowHandlers {
		h.Build(builder)
	}
	r := builder.NewRecord()
	fw, err := pqarrow.NewFileWriter(arrow.NewSchema(fields, nil), e.w, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		return fmt.Errorf("new pqarrow writer err: %v", err)
	}

	err = fw.WriteBuffered(r)
	if err != nil {
		return fmt.Errorf("write buffered err: %v", err)
	}
	if err = fw.Close(); err != nil {
		return fmt.Errorf("close write buffered err: %v", err)
	}
	return nil

}

type ArrowHandler interface {
	GetArrowField() arrow.Field
	GetIndex() int
	Add(v any) error
	GetValue() any
	Build(builder *array.RecordBuilder)
}
