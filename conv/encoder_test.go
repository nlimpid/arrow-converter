package conv

import (
	"github.com/stretchr/testify/assert"
	"log/slog"
	"testing"
	"time"
)

func Test_getStructArrowInfo(t *testing.T) {
	type Order struct {
		OrderKey      int32     `arrow:"name=o_orderkey"`
		CustomerKey   int32     `arrow:"name=o_custkey"`
		OrderStatus   string    `arrow:"name=o_orderstatus"`
		TotalPrice    float64   `arrow:"name=o_totalprice"`
		OrderDate     time.Time `arrow:"name=o_orderdate,arrow_type=date_32"`
		OrderPriority string    `arrow:"name=o_orderpriority"`
		Clerk         string    `arrow:"name=o_clerk"`
		ShipPriority  int32     `arrow:"name=o_shippriority"`
		Comment       *string   `arrow:"name=o_comment"`
	}
	slog.SetLogLoggerLevel(slog.LevelDebug)
	structInfo, err := getStructArrowInfo[Order]()
	assert.NoError(t, err)
	structInfoMap := make(map[string]*StructArrowInfo, len(structInfo))
	for _, v := range structInfo {
		structInfoMap[v.ArrowName] = v
	}

	slog.Info("got ", "info", structInfoMap["o_orderdate"].ArrowType)

}
