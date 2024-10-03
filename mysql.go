package arrow_conv

import (
	"database/sql"
	"fmt"
	"log/slog"
	"strings"

	"github.com/apache/arrow/go/v17/arrow"

	_ "github.com/go-sql-driver/mysql"
	"github.com/nlimpid/arrow-conv/column"
)

type MySQLConverter struct {
	db *sql.DB
}

func NewMySQLConverter(db *sql.DB) *MySQLConverter {
	return &MySQLConverter{db: db}
}

func (a *MySQLConverter) CreateHandlers(query string) (*HandlerManager, error) {
	// Query the database, but only get meta info
	rows, err := a.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()
	// get column info
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, err
	}
	var handlers []ArrowHandler
	// https://github.com/go-sql-driver/mysql/blob/master/fields.go#L79
	for i, col := range columnTypes {
		var handler ArrowHandler
		name := col.Name()
		nullable, _ := col.Nullable()

		switch col.DatabaseTypeName() {
		case "TINYINT", "BOOL", "BOOLEAN":
			handler = column.NewBoolHandler(name, i, nullable)
		case "INT", "BIGINT", "SMALLINT", "MEDIUMINT":
			handler = column.NewInt64Handler(name, i, nullable)
		case "VARCHAR", "TEXT", "CHAR":
			handler = column.NewStringHandler(name, i, nullable)
		case "FLOAT":
			handler = column.NewFloatHandler(name, i, nullable)
		case "TIMESTAMP":
			handler = column.NewTimestampHandler(name, i, nullable, arrow.Second)
		case "DECIMAL":
			prec, scale, _ := col.DecimalSize()
			handler = column.NewDecimalHandler(name, i, nullable, int32(prec), int32(scale))

		default:
			return nil, fmt.Errorf("unsupported column type: %s", col.DatabaseTypeName())
		}
		handlers = append(handlers, handler)
	}

	return &HandlerManager{
		Handlers: handlers,
	}, nil
}

func (a *MySQLConverter) ReadIntoHandler(query string) (*HandlerManager, error) {
	handler, err := a.CreateHandlers(query)
	if err != nil {
		return nil, err
	}

	rows, err := a.db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	handlers := handler.Handlers

	for rows.Next() {
		values := make([]any, len(handlers))
		for i, handler := range handlers {
			values[i] = handler.GetScanType()
		}
		if err := rows.Scan(values...); err != nil {
			return nil, err
		}
		for i, handler := range handlers {
			if err := handler.Add(values[i]); err != nil {
				return nil, err
			}
		}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return handler, nil
}

func (a *MySQLConverter) WriteFromHandler(table string, handlers []ArrowHandler) error {
	// Implement the logic to write data from handlers to MySQL
	// This is a simplified example and may need to be adjusted based on your specific requirements
	placeholders := make([]string, len(handlers))
	columNames := make([]string, len(handlers))
	for i := range placeholders {
		placeholders[i] = "?"
		columNames[i] = handlers[i].GetArrowField().Name
	}

	query := fmt.Sprintf("INSERT INTO %s(%s) VALUES (%s)", table, strings.Join(columNames, ","), strings.Join(placeholders, ", "))
	stmt, err := a.db.Prepare(query)
	if err != nil {
		return err
	}
	defer func() {
		_ = stmt.Close()
	}()

	for handlers[0].Next() {
		for i := 1; i < len(handlers); i++ {
			handler := handlers[i]
			handler.Next()
		}

		values := make([]interface{}, len(handlers))
		for i, handler := range handlers {
			values[i] = handler.Value()
		}
		slog.Info("values is", "val", values)
		_, err := stmt.Exec(values...)
		if err != nil {
			return fmt.Errorf("could not insert into %s table: %v", table, err)
		}
	}

	return nil
}
