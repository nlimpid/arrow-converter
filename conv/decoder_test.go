package conv

import (
	"bytes"
	"context"
	"github.com/stretchr/testify/assert"
	"io"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"
)

type Order struct {
	OrderKey      int32     `arrow:"name=o_orderkey"`
	CustomerKey   int32     `arrow:"name=o_custkey"`
	OrderStatus   string    `arrow:"name=o_orderstatus"`
	TotalPrice    float64   `arrow:"name=o_totalprice"`
	OrderDate     time.Time `arrow:"name=o_orderdate,arrow_type=date_32"`
	OrderPriority string    `arrow:"name=o_orderpriority"`
	Clerk         string    `arrow:"name=o_clerk"`
	ShipPriority  int32     `arrow:"name=o_shippriority"`
	Comment       string    `arrow:"name=o_comment"`
}

func Test_Decode(t *testing.T) {
	fileURL := "https://shell.duckdb.org/data/tpch/0_01/parquet/orders.parquet"
	b, err := http.Get(fileURL)
	assert.NoError(t, err)
	defer func() {
		b.Body.Close()
	}()
	content, err := io.ReadAll(b.Body)
	assert.NoError(t, err)
	// 执行测试
	enc, err := NewDecoder[Order](bytes.NewReader(content))
	assert.NoError(t, err)
	res, err := enc.Decode(context.Background())
	assert.NoError(t, err)

	for i, v := range res {
		if i > 10 {
			break
		}
		slog.Info("data is", "order", v)
	}
}

func Test_Encode(t *testing.T) {
	fileURL := "https://shell.duckdb.org/data/tpch/0_01/parquet/orders.parquet"
	b, err := http.Get(fileURL)
	assert.NoError(t, err)
	defer func() {
		b.Body.Close()
	}()
	content, err := io.ReadAll(b.Body)
	assert.NoError(t, err)
	// 执行测试
	dec, err := NewDecoder[Order](bytes.NewReader(content))
	assert.NoError(t, err)
	res, err := dec.Decode(context.Background())
	assert.NoError(t, err)

	for _, data := range res[0:2] {
		slog.Info("data is", "order", data)
	}

	enc, _ := NewEncoder[Order]()
	w, _ := os.Create("/Users/nlimpid/Downloads/orders.parquet")
	err = enc.Encode(context.Background(), res[0:2], w)
	assert.NoError(t, err)
}
