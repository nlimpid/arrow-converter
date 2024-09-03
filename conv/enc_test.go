package conv

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"testing"
)

type Order struct {
	OrderKey    int32   `arrow:"name=o_orderkey"`
	CustomerKey int32   `arrow:"name=o_custkey"`
	OrderStatus string  `arrow:"name=o_orderstatus"`
	TotalPrice  float64 `arrow:"name=o_totalprice"`
	//OrderDate     time.Time `arrow:"name=o_orderdate"`
	OrderPriority string `arrow:"name=o_orderpriority"`
	Clerk         string `arrow:"name=o_clerk"`
	ShipPriority  int32  `arrow:"name=o_shippriority"`
	Comment       string `arrow:"name=o_comment"`
}

func downloadFile(url string, destPath string) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	out, err := os.Create(destPath)
	if err != nil {
		return err
	}
	defer out.Close()

	_, err = io.Copy(out, resp.Body)
	return err
}

func Test_ParquetToStructsDynamic(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	// 创建临时目录
	tempDir, err := os.MkdirTemp("", "parquet_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// 下载 Parquet 文件
	fileURL := "https://shell.duckdb.org/data/tpch/0_01/parquet/orders.parquet"
	filePath := filepath.Join(tempDir, "orders.parquet")
	err = downloadFile(fileURL, filePath)
	if err != nil {
		t.Fatalf("Failed to download file: %v", err)
	}

	// 执行测试
	ctx := context.Background()
	enc := NewEnc()
	got, err := ParquetToStructsDynamic[Order](ctx, enc, filePath)
	if err != nil {
		t.Fatalf("ParquetToStructsDynamic failed: %v", err)
	}
	// 检查结果
	if len(got) == 0 {
		t.Errorf("Expected non-empty result, got empty slice")
	}

	// 打印一些结果以进行验证
	slog.Info("Number of orders", "count", len(got))
	if len(got) > 0 {
		slog.Info("First order", "order", got[0])
		slog.Info("First order", "order", got[1])
	}
}
