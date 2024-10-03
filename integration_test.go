package arrow_conv

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"testing"
	"time"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
	"github.com/nlimpid/arrow-conv/column"
	"github.com/stretchr/testify/suite"
)

func SetupMySQL() (*sql.DB, error) {
	mysqlDSN := os.Getenv("MYSQL_DSN")
	if mysqlDSN == "" {
		mysqlDSN = "root:password@tcp(localhost:3306)/testdb"
	}
	db, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to MySQL: %v", err)
	}
	createTableSQL := `
	CREATE TABLE IF NOT EXISTS test_data (
		id INT PRIMARY KEY AUTO_INCREMENT,
		string_col VARCHAR(255),
		int_col INT,
		float_col FLOAT,
		bool_col BOOLEAN,
		timestamp_col TIMESTAMP,
		decimal_col DECIMAL(10, 2)
	)`

	_, err = db.Exec(createTableSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to create test table: %v", err)
	}
	// Insert test data
	insertSQL := `INSERT INTO test_data (string_col, int_col, float_col, bool_col, timestamp_col, decimal_col) VALUES (?, ?, ?, ?, ?, ?)`
	_, err = db.Exec(insertSQL, "test string", 42, 3.14, true, "2023-05-01 12:00:00", 123.45)
	if err != nil {
		return nil, fmt.Errorf("failed to insert test data: %v", err)
	}
	return db, nil
}

func SetupPostgres() (*sql.DB, error) {
	postgresDSN := os.Getenv("POSTGRES_DSN")
	if postgresDSN == "" {
		postgresDSN = "postgres://user:password@localhost:5432/testdb?sslmode=disable"
	}
	db, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		return nil, fmt.Errorf("failed to connect to PostgreSQL: %v", err)
	}

	createTableSQL := `
	CREATE TABLE IF NOT EXISTS test_data (
		id SERIAL PRIMARY KEY,
		string_col VARCHAR(255),
		int_col INTEGER,
		float_col REAL,
		bool_col BOOLEAN,
		timestamp_col TIMESTAMP,
		decimal_col DECIMAL(10, 2)
	)`
	_, err = db.Exec(createTableSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to create test table: %v", err)
	}

	insertSQL := `
	INSERT INTO test_data (string_col, int_col, float_col, bool_col, timestamp_col, decimal_col)
	VALUES ($1, $2, $3, $4, $5, $6)`
	_, err = db.Exec(insertSQL)
	if err != nil {
		return nil, fmt.Errorf("failed to create test table: %v", err)
	}

	return db, nil
}

type MySQLConverterTestSuite struct {
	suite.Suite
	DB     *sql.DB
	h      *MySQLConverter
	DBType string
}

func (s *MySQLConverterTestSuite) SetupSuite() {
	var err error

	s.DB, err = SetupMySQL()
	s.h = NewMySQLConverter(s.DB)
	s.Require().NoError(err)
}

func (s *MySQLConverterTestSuite) TearDownSuite() {
	if s.DB != nil {
		s.DB.Close()
	}
}

func TestMysqlSuite(t *testing.T) {
	suite.Run(t, new(MySQLConverterTestSuite))
}

func (s *MySQLConverterTestSuite) TestReadIntoHandler() {
	query := "SELECT * FROM test_data"
	handler, err := s.h.ReadIntoHandler(query)
	handlers := handler.Handlers
	s.Require().NoError(err)

	s.Equal(7, len(handlers), "Expected 7 handlers")

	// Test each handler
	for _, handler := range handlers {
		s.True(handler.Next(), "Expected at least one row")
		value := handler.Value()
		s.NotNil(value, "Expected non-nil value")

		switch handler.(type) {
		case *column.StringHandler:
			s.IsType("", value, "Expected string value")
		case *column.Int64Handler:
			s.IsType(int64(0), value, "Expected int64 value")
		case *column.FloatHandler:
			s.IsType(float64(0), value, "Expected float64 value")
		case *column.BoolHandler:
			s.IsType(bool(false), value, "Expected bool value")
		case *column.TimestampHandler:
			s.IsType(time.Time{}, value, "Expected time.Time value")
		case *column.DecimalHandler:
			s.IsType("", value, "Expected string (decimal) value")
		default:
			s.T().Errorf("Unexpected handler type: %T", handler)
		}
	}
}

func (s *MySQLConverterTestSuite) TestWriteAndReadParquet() {
	query := "SELECT * FROM test_data"
	handler, err := s.h.ReadIntoHandler(query)
	s.Require().NoError(err)

	filename := fmt.Sprintf("test_output_%s.parquet", s.DBType)
	ctx := context.Background()

	w, _ := os.Create(filename)
	err = handler.WriteToParquet(ctx, w)
	handlers := handler.Handlers
	w.Close()

	s.Require().NoError(err)

	readerHandler := NewHandlerManager()
	err = readerHandler.ReadFromParquet(ctx, filename)
	s.Require().NoError(err)
	readHandlers := readerHandler.Handlers

	s.Equal(len(handlers), len(readHandlers), "Number of handlers should match")

	slog.Info("read handler length is ", "len", len(readHandlers))
	for i := range readHandlers {
		//s.Equal(handlers[i].GetArrowField(), readHandlers[i].GetArrowField(), "Arrow fields should match")

		//handlers[i].Next()
		readHandlers[i].Next()

		//originalValue := handlers[i].Value()
		readValue := readHandlers[i].Value()
		slog.Info("read value ", "value", readValue)

		//s.Equal(originalValue, readValue, "Values should match for handler %d", i)
	}

	// Clean up
	//err = os.Remove(filename)
	s.Require().NoError(err)
}

func (s *MySQLConverterTestSuite) TestReadParquet() {
	filename := fmt.Sprintf("test_output_%s.parquet", s.DBType)
	ctx := context.Background()
	readerHandler := NewHandlerManager()
	err := readerHandler.ReadFromParquet(ctx, filename)
	s.Require().NoError(err)
	readHandlers := readerHandler.Handlers
	slog.Info("read handler length is ", "len", len(readHandlers))

	//first := readHandlers[0].Next()
	//slog.Info("read first ", "value", first)
	//for i := range readHandlers {
	//	//s.Equal(handlers[i].GetArrowField(), readHandlers[i].GetArrowField(), "Arrow fields should match")
	//	//handlers[i].Next()
	//	for readHandlers[i].Next() {
	//		readValue := readHandlers[i].Value()
	//		slog.Info("read value ", "value", readValue)
	//	}
	//	//originalValue := handlers[i].Value()
	//
	//	//s.Equal(originalValue, readValue, "Values should match for handler %d", i)
	//}

	rr := make([]ArrowHandler, 0)
	for _, handler := range readHandlers {
		if handler.GetArrowField().Name == "id" {
			continue
		}
		rr = append(rr, handler)
	}

	err = s.h.WriteFromHandler("test_data", rr)
	s.Require().NoError(err)

	// Clean up
	//err = os.Remove(filename)
	s.Require().NoError(err)
}
