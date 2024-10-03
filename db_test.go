package arrow_conv

import (
	"context"
	"database/sql"
	"log/slog"
	"os"
	"testing"

	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/stretchr/testify/suite"
)

type PGSuite struct {
	suite.Suite
	h *PGConverter
}

func (suite *PGSuite) SetupSuite() {
	var err error
	suite.Equal(nil, err)

	connStr := os.Getenv("PG_URL")
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		panic(err)
	}
	slog.SetLogLoggerLevel(slog.LevelDebug)
	h := NewPGConverter(db)
	suite.h = h
}

func (suite *PGSuite) TearDownTest() {}

func TestSuite(t *testing.T) {
	suite.Run(t, new(PGSuite))
}

func (suite *PGSuite) Test_Handle() {
	tmpFile, err := os.CreateTemp("", "arrow-test-*.parquet")
	if err != nil {
		slog.Error("create temp file")
		return
	}
	defer os.Remove(tmpFile.Name()) // Clean up the file afterward
	slog.Info("tmp file", "name", tmpFile)

	ctx := context.Background()
	ah, err := suite.h.Query(ctx, "SELECT g FROM generate_series(1, 2) g")
	suite.NoError(err)
	err = suite.h.BuildParquetFile(ctx, tmpFile, ah)
	suite.NoError(err)

	err = parquetReader(tmpFile.Name())
	suite.NoError(err)
}

func parquetReader(filename string) error {

	r, err := file.OpenParquetFile(filename, false)
	if err != nil {
		return err
	}
	slog.Info("ParquetFile", "num", r.NumRowGroups())
	slog.Info("first group num", "row_num", r.RowGroup(0).NumRows())
	return nil
}
