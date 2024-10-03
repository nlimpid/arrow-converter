package arrow_conv

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log/slog"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	_ "github.com/lib/pq"
	"github.com/nlimpid/arrow-conv/column"
)

type PGConverter struct {
	db *sql.DB
}

func NewPGConverter(db *sql.DB) *PGConverter {
	return &PGConverter{
		db: db,
	}
}

func (h *PGConverter) HandleRows(ctx context.Context, rows *sql.Rows) ([]ArrowHandler, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("ColumnTypes is %v", columnTypes)
	}
	rowNum := len(columnTypes)
	arrowHandlerMap := make([]ArrowHandler, rowNum)
	emptyScan := make([]interface{}, rowNum)
	for idx, columnType := range columnTypes {
		h, err := GetPGColumnHandler(idx, columnType)
		if err != nil {
			return nil, err
		}
		arrowHandlerMap[idx] = h
		emptyScan[idx] = h.GetScanType()
	}
	slog.Error("emptyScan", "emtpy", emptyScan)

	for rows.Next() {
		b := emptyScan
		if err = rows.Scan(b...); err != nil {
			return nil, fmt.Errorf("error scanning rows: %v", err)
		}
		for idx, v := range b {
			if err := arrowHandlerMap[idx].Add(v); err != nil {
				return nil, err
			}
		}
	}
	return arrowHandlerMap, nil
}

func (h *PGConverter) Query(ctx context.Context, query string, args ...any) ([]ArrowHandler, error) {
	slog.Debug("emptyScan", "emtpy", query)
	rows, err := h.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query err %w", err)
	}
	defer rows.Close()
	return h.HandleRows(ctx, rows)
}

func (h *PGConverter) BuildParquetFile(ctx context.Context, w io.Writer, arrowHandlerMap []ArrowHandler) error {
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

func debugR(r *ipc.Reader) {
	sc := r.Schema()
	fmt.Println(sc.String())
	slog.Info("schema is %s", sc.String(), "")
}

func GetPGColumnHandler(index int, col *sql.ColumnType) (ArrowHandler, error) {
	columnName := col.Name()
	columnNullable, _ := col.Nullable()
	slog.Debug("pg column type", "name", columnName, "type", col.DatabaseTypeName())

	// https://github.com/lib/pq/blob/master/oid/types.go
	switch col.DatabaseTypeName() {
	case "INT8", "INT2":
		return column.NewInt64Handler(columnName, index, columnNullable), nil
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
		return column.NewTimestampHandler(columnName, index, columnNullable, arrow.Second), nil
	}

	return nil, fmt.Errorf("unsupported col type: %s", col.DatabaseTypeName())
}

func (a *PGConverter) WriteFromHandler(table string, handlers []ArrowHandler) error {
	// Implement the logic to write data from handlers to PostgreSQL
	// This is a simplified example and may need to be adjusted based on your specific requirements
	placeholders := make([]string, len(handlers))
	for i := range placeholders {
		placeholders[i] = fmt.Sprintf("$%d", i+1)
	}

	query := fmt.Sprintf("INSERT INTO %s VALUES (%s)", table, strings.Join(placeholders, ", "))
	stmt, err := a.db.Prepare(query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for handlers[0].Next() {
		values := make([]interface{}, len(handlers))
		for i, handler := range handlers {
			values[i] = handler.Value()
		}
		_, err := stmt.Exec(values...)
		if err != nil {
			return err
		}
	}

	return nil
}
