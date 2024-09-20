package conv

import (
	"context"
	"fmt"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/apache/arrow/go/v18/parquet"
	"reflect"

	"github.com/apache/arrow/go/v18/arrow/memory"
	"github.com/apache/arrow/go/v18/parquet/file"
	"github.com/apache/arrow/go/v18/parquet/pqarrow"
)

// Decoder 结构体
type Decoder[T any] struct {
	arrowManager *ArrowHandlerManager
	schema       *arrow.Schema
	r            parquet.ReaderAtSeeker
}

// NewDecoder 创建一个新的 Decoder
func NewDecoder[T any](r parquet.ReaderAtSeeker) (*Decoder[T], error) {

	// 获取结构体字段的 Arrow 信息
	structInfo, err := getStructArrowInfo[T]()
	if err != nil {
		return nil, err
	}
	manager := &ArrowHandlerManager{
		handlers: make([]ArrowHandler, 0, len(structInfo)),
	}
	// 初始化 ArrowHandlers
	for idx, info := range structInfo {
		handler, err := CreateGenericHandler(info, idx)
		if err != nil {
			return nil, err
		}
		manager.AddHandler(handler)
	}

	schema := manager.GetSchema()

	return &Decoder[T]{
		arrowManager: manager,
		schema:       schema,
		r:            r,
	}, nil
}

// Decode 从 Parquet 文件解码为结构体切片
func (d *Decoder[T]) Decode(ctx context.Context) ([]T, error) {
	// 打开 Parquet 文件读取器
	parquetReader, err := file.NewParquetReader(d.r)
	if err != nil {
		return nil, err
	}
	defer parquetReader.Close()

	// 使用 pqarrow 读取 Parquet 文件为 Arrow 表
	pqarrowReader, err := pqarrow.NewFileReader(parquetReader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return nil, err
	}

	table, err := pqarrowReader.ReadTable(ctx)
	if err != nil {
		return nil, err
	}
	defer table.Release()

	// 提取结构体切片
	return extractStructs[T](ctx, table, d.arrowManager)
}

// extractStructs 从 Arrow 表提取结构体切片
func extractStructs[T any](ctx context.Context, table arrow.Table, manager *ArrowHandlerManager) ([]T, error) {
	numRows := table.NumRows()
	result := make([]T, 0, numRows)

	schemaMap := make(map[string]int)
	for i := 0; i < int(table.NumCols()); i++ {
		c := table.Column(i)
		schemaMap[c.Name()] = i
	}

	for rowIndex := 0; rowIndex < int(numRows); rowIndex++ {
		var structInstance T
		val := reflect.ValueOf(&structInstance).Elem()

		for _, handler := range manager.handlers {
			fieldName := handler.GetFieldName()
			field := val.FieldByName(fieldName)
			if !field.IsValid() || !field.CanSet() {
				return nil, fmt.Errorf("cannot set field: %s", fieldName)
			}

			// 获取列的数据
			arrowColumnIndex := schemaMap[handler.GetArrowField().Name]
			columnData := table.Column(arrowColumnIndex).Data()
			// 找到对应的块和本地行索引
			chunkIndex, localRowIndex := findChunk(rowIndex, columnData)

			if chunkIndex == -1 {
				return nil, fmt.Errorf("row index %d out of bounds for column %s", rowIndex, fieldName)
			}

			// 获取值
			value, err := getValueFromArray(columnData.Chunks()[chunkIndex], localRowIndex, handler.GetArrowField().Type)
			if err != nil {
				return nil, fmt.Errorf("failed to get value for field %s at row %d: %w", fieldName, rowIndex, err)
			}

			// 设置值
			if value != nil {
				// 确保类型匹配
				valToSet := reflect.ValueOf(value)
				if valToSet.Type().ConvertibleTo(field.Type()) {
					field.Set(valToSet.Convert(field.Type()))
				} else {
					return nil, fmt.Errorf("cannot convert value of type %T to field %s of type %s", value, fieldName, field.Type())
				}
			} else {
				field.Set(reflect.Zero(field.Type()))
			}
		}

		result = append(result, structInstance)
	}

	return result, nil
}

// findChunk 找到全局 rowIndex 对应的块索引和本地行索引
func findChunk(rowIndex int, columnData *arrow.Chunked) (chunkIndex int, localRowIndex int) {
	cumulativeRows := 0
	for idx, chunk := range columnData.Chunks() {
		chunkLen := chunk.Len()
		if rowIndex < cumulativeRows+chunkLen {
			return idx, rowIndex - cumulativeRows
		}
		cumulativeRows += chunkLen
	}
	return -1, -1 // 不存在对应的块
}

// getValueFromArray 从 Arrow 数组中获取值
func getValueFromArray(arr arrow.Array, row int, dtype arrow.DataType) (any, error) {
	if arr.IsNull(row) {
		return nil, nil
	}

	switch dtype.ID() {
	case arrow.BOOL:
		return arr.(*array.Boolean).Value(row), nil
	case arrow.INT8:
		return arr.(*array.Int8).Value(row), nil
	case arrow.INT16:
		return arr.(*array.Int16).Value(row), nil
	case arrow.INT32:
		return arr.(*array.Int32).Value(row), nil
	case arrow.INT64:
		return arr.(*array.Int64).Value(row), nil
	case arrow.UINT8:
		return arr.(*array.Uint8).Value(row), nil
	case arrow.UINT16:
		return arr.(*array.Uint16).Value(row), nil
	case arrow.UINT32:
		return arr.(*array.Uint32).Value(row), nil
	case arrow.UINT64:
		return arr.(*array.Uint64).Value(row), nil
	case arrow.FLOAT32:
		return arr.(*array.Float32).Value(row), nil
	case arrow.FLOAT64:
		return arr.(*array.Float64).Value(row), nil
	case arrow.STRING:
		return arr.(*array.String).Value(row), nil
	case arrow.FIXED_SIZE_BINARY:
		return arr.(*array.FixedSizeBinary).Value(row), nil
	//case arrow.DECIMAL128:
	//	// 处理 Decimal128
	//	decimalArr := arr.(*array.Decimal128)
	//	decimalVal := decimalArr.Value(row)
	//	return decimal.NewFromBigInt(decimalVal.Unscaled, int32(decimalVal.Scale)), nil
	case arrow.TIMESTAMP:
		// TODO: maybe not all is arrow second
		timestampArr := arr.(*array.Timestamp).Value(row).ToTime(arrow.Second)
		return timestampArr, nil
		// 都转换成 time.Time了
	case arrow.DATE32:
		return arr.(*array.Date32).Value(row).ToTime(), nil
	case arrow.DATE64:
		return arr.(*array.Date64).Value(row).ToTime(), nil

	default:
		return nil, fmt.Errorf("unsupported arrow data type: %s", dtype.Name())
	}
}
