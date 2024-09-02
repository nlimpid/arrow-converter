package conv

import (
	"context"
	"fmt"
	"google.golang.org/genproto/googleapis/type/decimal"
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet/file"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
)

type Enc struct {
}

func NewEnc() *Enc {
	return &Enc{}
}

type StructArrowInfo struct {
	// ArrowName the name in arrow
	ArrowName string
	FieldType arrow.DataType
	// StructFieldName the name in struct
	StructFieldName string
}

func getStructArrowInfo(instance any) ([]StructArrowInfo, error) {
	var fieldMappings []StructArrowInfo
	t := reflect.TypeOf(instance).Elem()
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("arrow")

		if tag == "" {
			return nil, fmt.Errorf("missing `arrow` tag in field: %s", field.Name)
		}

		parts := strings.Split(tag, "=")
		if len(parts) != 2 {
			return nil, fmt.Errorf("invalid `arrow` tag format in field: %s", field.Name)
		}

		var arrowName string
		switch parts[0] {
		case "index":
			index, err := strconv.Atoi(parts[1])
			if err != nil {
				return nil, fmt.Errorf("invalid index in `arrow` tag for field: %s", field.Name)
			}
			arrowName = strconv.Itoa(index)
		case "name":
			arrowName = parts[1]
		default:
			return nil, fmt.Errorf("invalid `arrow` tag key in field: %s, expected 'index' or 'name'", field.Name)
		}

		fieldType, err := getArrowDataType(field.Type)
		if err != nil {
			return nil, err
		}

		fieldMappings = append(fieldMappings, StructArrowInfo{
			ArrowName:       arrowName,
			StructFieldName: field.Name,
			FieldType:       fieldType,
		})
	}

	return fieldMappings, nil
}

func getArrowDataType(t reflect.Type) (arrow.DataType, error) {
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
		panic("unhandled default case")
	}

	return nil, fmt.Errorf("unsupported field type: %s", t.Kind())
}

func ParquetToStructsDynamic[T any](ctx context.Context, enc *Enc, filename string) ([]T, error) {
	fieldMappings, err := getStructArrowInfo(new(T))
	if err != nil {
		return nil, err
	}
	f, err := file.OpenParquetFile(filename, true)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	reader, err := pqarrow.NewFileReader(f, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}

	table, err := reader.ReadTable(context.Background())
	if err != nil {
		return nil, err
	}
	defer table.Release()

	fieldMappingsMap := make(map[ArrowName]StructArrowInfo)
	for _, v := range fieldMappings {
		fieldMappingsMap[ArrowName(v.ArrowName)] = v
	}

	return extractStructs[T](ctx, enc, table, fieldMappingsMap)
}

type ArrowName string

func extractStructs[T any](ctx context.Context, enc *Enc, table arrow.Table, structArrowInfo map[ArrowName]StructArrowInfo) ([]T, error) {
	// parquet name to index mapping
	arrowName2Index := make(map[string]int, table.Schema().NumFields())
	for i, field := range table.Schema().Fields() {
		arrowName2Index[field.Name] = i
	}

	for k, v := range arrowName2Index {
		slog.Debug("arrowName2Index", k, v)
	}

	var structs []T
	for i := 0; i < int(table.NumRows()); i++ {
		var structInstance T
		val := reflect.ValueOf(&structInstance).Elem()
		for fieldName, colIndex := range arrowName2Index {
			mapping, ok := structArrowInfo[ArrowName(fieldName)]
			if !ok {
				continue
			}
			field := val.FieldByName(mapping.StructFieldName)
			if !field.IsValid() {
				return nil, fmt.Errorf("no such field: %s in struct", mapping.StructFieldName)
			}
			if err := setFieldValue(field, table.Column(colIndex), i); err != nil {
				return nil, err
			}
		}

		structs = append(structs, structInstance)
	}

	return structs, nil
}

func setFieldValue(field reflect.Value, col *arrow.Column, index int) error {
	for _, v := range col.Data().Chunks() {
		if err := setChunkValue(field, v, index); err != nil {
			return err
		}
	}
	return nil
}

func setChunkValue(field reflect.Value, col arrow.Array, index int) error {
	switch field.Kind() {
	case reflect.Int32:
		field.SetInt(int64(col.(*array.Int32).Value(index)))
	case reflect.String:
		field.SetString(col.(*array.String).Value(index))
	case reflect.Float64:
		field.SetFloat(col.(*array.Float64).Value(index))
	default:
		return fmt.Errorf("unsupported field type: %s", field.Kind())
	}
	return nil
}

type FieldSetter interface {
	SetFieldValue(fieldName string, value any) error
}

// 通用的 setField 函数，处理两种情况：实现了 FieldSetter 接口或使用反射
func setField(instance any, fieldName string, value any, reflectCache map[string]reflect.Value) error {
	// 检查实例是否实现了 FieldSetter 接口
	if setter, ok := instance.(FieldSetter); ok {
		return setter.SetFieldValue(fieldName, value)
	}

	// 使用反射操作设置字段值
	val := reflect.ValueOf(instance).Elem()
	val.FieldByName(fieldName).Set(reflect.ValueOf(value))
	return nil
}
