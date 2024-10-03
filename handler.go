package arrow_conv

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/nlimpid/arrow-conv/column"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
)

type ArrowHandler interface {
	ArrowReaderHandler
	ArrowWriterHandler
}

type ArrowReaderHandler interface {
	GetArrowField() arrow.Field
	SetColumn(column *arrow.Column)
	Next() bool
	Value() any
	Reset()
}

type ArrowWriterHandler interface {
	GetArrowField() arrow.Field
	GetScanType() any
	Add(v any) error
	Build(builder *array.RecordBuilder)
}

type HandlerManager struct {
	Handlers []ArrowHandler
}

func NewHandlerManager() *HandlerManager {
	return &HandlerManager{
		Handlers: make([]ArrowHandler, 0),
	}
}

func (m *HandlerManager) AddHandler(handler ArrowHandler) {
	m.Handlers = append(m.Handlers, handler)
}

func (m *HandlerManager) WriteToParquet(ctx context.Context, w io.Writer) error {
	arrowHandlerMap := m.Handlers
	fields := make([]arrow.Field, len(arrowHandlerMap))
	for i, h := range arrowHandlerMap {
		fields[i] = h.GetArrowField()
	}
	alloc := memory.NewGoAllocator()
	builder := array.NewRecordBuilder(alloc, arrow.NewSchema(fields, nil))
	for _, h := range arrowHandlerMap {
		h.Build(builder)
	}
	r := builder.NewRecord()
	fw, err := pqarrow.NewFileWriter(arrow.NewSchema(fields, nil), w, nil, pqarrow.DefaultWriterProps())
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

func (m *HandlerManager) Release(record arrow.Table) func() {
	return func() {
		record.Release()
	}
}

func (m *HandlerManager) ReadFromParquet(ctx context.Context, filename string) error {
	slog.Info("new reader")
	f, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer f.Close()

	reader, err := file.NewParquetReader(f)
	if err != nil {
		return fmt.Errorf("failed to create parquet reader: %v", err)
	}
	defer reader.Close()

	arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{}, memory.DefaultAllocator)
	if err != nil {
		return fmt.Errorf("failed to create arrow reader: %v", err)
	}

	record, err := arrowReader.ReadTable(ctx)
	if err != nil {
		return fmt.Errorf("failed to read record: %v", err)
	}

	// 清除现有的 handlers
	m.Handlers = make([]ArrowHandler, 0)

	// 为每个列创建新的 handler
	for i, field := range record.Schema().Fields() {
		c := record.Column(i)
		var handler ArrowHandler
		switch field.Type.ID() {
		case arrow.BOOL:
			handler = column.NewBoolHandler(field.Name, i, field.Nullable)
		// case arrow.UINT8:
		// 	handler = column.NewUint8Handler(field.Name, i, field.Nullable)
		// case arrow.INT8:
		// 	handler = column.NewInt8Handler(field.Name, i, field.Nullable)
		// case arrow.UINT16:
		// 	handler = column.NewUint16Handler(field.Name, i, field.Nullable)
		// case arrow.INT16:
		// 	handler = column.NewInt16Handler(field.Name, i, field.Nullable)
		// case arrow.UINT32:
		// 	handler = column.NewUint32Handler(field.Name, i, field.Nullable)
		case arrow.INT32:
			handler = column.NewInt32Handler(field.Name, i, field.Nullable)
		// case arrow.UINT64:
		// 	handler = column.NewUint64Handler(field.Name, i, field.Nullable)
		case arrow.INT64:
			handler = column.NewInt64Handler(field.Name, i, field.Nullable)
			handler.SetColumn(c)
		// case arrow.FLOAT32:
		// 	handler = column.NewFloat32Handler(field.Name, i, field.Nullable)
		case arrow.FLOAT64:
			handler = column.NewFloatHandler(field.Name, i, field.Nullable)
		case arrow.STRING:
			handler = column.NewStringHandler(field.Name, i, field.Nullable)
		// case arrow.BINARY:
		// 	handler = column.NewBinaryHandler(field.Name, i, field.Nullable)
		// case arrow.DATE32:
		// 	handler = column.NewDate32Handler(field.Name, i, field.Nullable)
		// case arrow.DATE64:
		// 	handler = column.NewDate64Handler(field.Name, i, field.Nullable)
		case arrow.TIMESTAMP:
			handler = column.NewTimestampHandler(field.Name, i, field.Nullable, arrow.Second)
		// case arrow.TIME32:
		// 	handler = column.NewTime32Handler(field.Name, i, field.Nullable)
		// case arrow.TIME64:
		// 	handler = column.NewTime64Handler(field.Name, i, field.Nullable)
		case arrow.DECIMAL128:
			handler = column.NewDecimalHandler(field.Name, i, field.Nullable, field.Type.(*arrow.Decimal128Type).Precision, field.Type.(*arrow.Decimal128Type).Scale)
		// case arrow.LIST:
		// 	handler = column.NewListHandler(field.Name, i, field.Nullable)
		// case arrow.STRUCT:
		// 	handler = column.NewStructHandler(field.Name, i, field.Nullable)
		// case arrow.MAP:
		// 	handler = column.NewMapHandler(field.Name, i, field.Nullable)
		default:
			return fmt.Errorf("unsupported field type: %v", field.Type)
		}
		handler.SetColumn(c)
		m.AddHandler(handler)
	}

	return nil
}
