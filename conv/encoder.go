package conv

import (
	"database/sql"
	"fmt"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
	"github.com/nlimpid/arrow-converter/column"
	"io"
	"log/slog"
	"reflect"
	"time"
)

type Encoder[T any] struct {
	arrowManager *ArrowHandlerManager
	schema       *arrow.Schema
}

// NewEncoder create a memory encoder
func NewEncoder[T any]() (*Encoder[T], error) {

	// get struct info
	structInfo, err := getStructArrowInfo[T]()
	if err != nil {
		return nil, err
	}
	enc := &Encoder[T]{
		arrowManager: &ArrowHandlerManager{
			handlers: make([]ArrowHandler, 0, len(structInfo)),
		},
	}

	for idx, info := range structInfo {
		handler, err := CreateGenericHandler(info, idx)
		if err != nil {
			return nil, err
		}
		enc.arrowManager.AddHandler(handler)
	}

	enc.schema = enc.arrowManager.GetSchema()
	return enc, nil
}

// Encode encode data to Writer(parquet file)
func (e *Encoder[T]) Encode(data []T, w io.Writer) error {
	// add data to handlers
	for _, record := range data {
		rv := reflect.ValueOf(record)
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}
		if rv.Kind() != reflect.Struct {
			return fmt.Errorf("slice elements must be structs")
		}

		for _, handler := range e.arrowManager.handlers {
			field := rv.FieldByName(handler.GetFieldName())
			if !field.IsValid() {
				return fmt.Errorf("no such field: %s in struct", handler.GetArrowField().Name)
			}
			if !field.CanAddr() {
				slog.Info("handler add data", "name", handler.GetArrowField().Name, "val", field.Interface())
				if err := handler.Add(field.Interface()); err != nil {
					return fmt.Errorf("cannot add field %s to array", handler.GetArrowField().Name)
				}
				continue
			}
			if err := handler.Add(field.Addr().Interface()); err != nil {
				return fmt.Errorf("failed to add value for field %s: %v", handler.GetArrowField().Name, err)
			}
		}
	}

	// 构建 Arrow Record
	builder := array.NewRecordBuilder(memory.DefaultAllocator, e.schema)
	defer builder.Release()
	e.arrowManager.BuildRecord(builder)
	record := builder.NewRecord()
	defer record.Release()

	// 写入 Parquet 文件
	fw, err := pqarrow.NewFileWriter(e.schema, w, nil, pqarrow.DefaultWriterProps())
	if err != nil {
		return fmt.Errorf("new pqarrow writer err: %v", err)
	}

	if err := fw.Write(record); err != nil {
		return fmt.Errorf("write record err: %v", err)
	}

	if err := fw.Close(); err != nil {
		return fmt.Errorf("close writer err: %v", err)
	}
	return nil
}

// CreateGenericHandler 根据 StructArrowInfo 创建对应的 ArrowHandler
func CreateGenericHandler(info *StructArrowInfo, index int) (ArrowHandler, error) {
	slog.Info("create generic handler", "info", info.ArrowType.Name(), "id", info.ArrowType.ID())
	switch info.ArrowType.ID() {
	case arrow.PrimitiveTypes.Int32.ID():
		return NewGenericArrowHandler[int32](info, index, false)
	case arrow.PrimitiveTypes.Int64.ID():
		return NewGenericArrowHandler[int64](info, index, false)
	case arrow.PrimitiveTypes.Float64.ID():
		return NewGenericArrowHandler[float64](info, index, false)
	case arrow.FixedWidthTypes.Boolean.ID():
		return NewGenericArrowHandler[bool](info, index, false)
	case arrow.BinaryTypes.String.ID():
		return NewGenericArrowHandler[string](info, index, false)
	case arrow.FixedWidthTypes.Date32.ID():
		return NewGenericArrowHandler[time.Time](info, index, false)
		// 时间类型
	case arrow.FixedWidthTypes.Timestamp_ms.ID():
		return NewGenericArrowHandler[arrow.Timestamp](info, index, false)
	case arrow.FixedWidthTypes.Timestamp_us.ID():
		return NewGenericArrowHandler[arrow.Timestamp](info, index, false)
	case arrow.FixedWidthTypes.Timestamp_ns.ID():
		return NewGenericArrowHandler[arrow.Timestamp](info, index, false)
	case arrow.FixedWidthTypes.Timestamp_s.ID():
		return NewGenericArrowHandler[arrow.Timestamp](info, index, false)

	// 日期类型
	case arrow.PrimitiveTypes.Date32.ID():
		return NewGenericArrowHandler[int32](info, index, false)
	case arrow.PrimitiveTypes.Date64.ID():
		return NewGenericArrowHandler[int64](info, index, false)

	default:
		return nil, fmt.Errorf("unsupported Arrow type: %s", info.ArrowType.Name())
	}
}

func GetPGColumnHandler(index int, col *sql.ColumnType) (ArrowHandler, error) {
	columnName := col.Name()
	columnNullable, _ := col.Nullable()
	varInt := reflect.New(col.ScanType()).Elem().Interface()

	slog.Debug("pg column type", "name", columnName, "type", col.DatabaseTypeName())

	// https://github.com/lib/pq/blob/master/oid/types.go
	switch col.DatabaseTypeName() {
	case "INT8", "INT2":
		return NewGenericArrowHandler[int32](info, index, false)
	case "INT4":
		return column.NewInt32Handler(columnName, index, columnNullable), nil
	case "VARCHAR", "CHAR":
		return column.NewStringHandler(columnName, index, columnNullable), nil
		// as string then to decimal
	case "NUMERIC":
		precision, scale, ok := col.DecimalSize()
		if !ok {
			return nil, fmt.Errorf("column decimal size is invalid")
		}
		// TODO: FixME
		if scale >= 39 {
			scale = 38
		}
		if precision >= 65535 {
			precision = 38
		}
		slog.Debug("pg column type", "name", columnName, "type", col.DatabaseTypeName(), "precision", precision, "scale", scale, "ok", ok)
		return column.NewDecimalHandler(columnName, index, columnNullable, int32(precision), int32(scale)), nil
	case "TIMESTAMP", "TIMESTAMPTZ", "DATE", "TIME":
		return column.NewTimeHandler(columnName, index, columnNullable), nil
	}

	return nil, fmt.Errorf("unsupported col type: %s", col.DatabaseTypeName())
}
