package conv

import (
	"fmt"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"log/slog"
)

// Reader 定义一个基本的迭代器接口
type Reader interface {
	Value(i int) any
}

// BooleanIterator 用于迭代布尔类型的 Arrow 列
type BooleanIterator struct {
	col   *array.Boolean
	index int
}

func NewBooleanIterator(col *array.Boolean) *BooleanIterator {
	return &BooleanIterator{col: col, index: -1}
}

func (it *BooleanIterator) Next() bool {
	if it.index < it.col.Len()-1 {
		it.index++
		return true
	}
	return false
}

func (it *BooleanIterator) Value(i int) any {
	return it.col.Value(i)
}

func (it *BooleanIterator) Reset() {
	it.index = -1
}

// Int8Iterator 用于迭代 Int8 类型的 Arrow 列
type Int8Iterator struct {
	col   *array.Int8
	index int
}

func NewInt8Iterator(col *array.Int8) *Int8Iterator {
	return &Int8Iterator{col: col, index: -1}
}

func (it *Int8Iterator) Next() bool {
	if it.index < it.col.Len()-1 {
		it.index++
		return true
	}
	return false
}

func (it *Int8Iterator) Value(i int) any {
	return it.col.Value(i)
}

func (it *Int8Iterator) Reset() {
	it.index = -1
}

// Int16Iterator 用于迭代 Int16 类型的 Arrow 列
type Int16Iterator struct {
	col   *array.Int16
	index int
}

func NewInt16Iterator(col *array.Int16) *Int16Iterator {
	return &Int16Iterator{col: col, index: -1}
}

func (it *Int16Iterator) Next() bool {
	if it.index < it.col.Len()-1 {
		it.index++
		return true
	}
	return false
}

func (it *Int16Iterator) Value(i int) any {
	return it.col.Value(i)
}

func (it *Int16Iterator) Reset() {
	it.index = -1
}

// Int32Iterator 用于迭代 Int32 类型的 Arrow 列
type Int32Iterator struct {
	col   *array.Int32
	index int
}

func NewInt32Iterator(col *array.Int32) *Int32Iterator {
	return &Int32Iterator{col: col, index: -1}
}

func (it *Int32Iterator) Next() bool {
	if it.index < it.col.Len()-1 {
		it.index++
		return true
	}
	return false
}

func (it *Int32Iterator) Value(i int) any {
	return it.col.Value(i)
}

func (it *Int32Iterator) Reset() {
	it.index = -1
}

// Int64Iterator 用于迭代 Int64 类型的 Arrow 列
type Int64Iterator struct {
	col   *array.Int64
	index int
}

func NewInt64Iterator(col *array.Int64) *Int64Iterator {
	return &Int64Iterator{col: col, index: -1}
}

func (it *Int64Iterator) Next() bool {
	if it.index < it.col.Len()-1 {
		it.index++
		return true
	}
	return false
}

func (it *Int64Iterator) Value(i int) any {
	return it.col.Value(i)
}

func (it *Int64Iterator) Reset() {
	it.index = -1
}

// Uint8Iterator 用于迭代 Uint8 类型的 Arrow 列
type Uint8Iterator struct {
	col   *array.Uint8
	index int
}

func NewUint8Iterator(col *array.Uint8) *Uint8Iterator {
	return &Uint8Iterator{col: col, index: -1}
}

func (it *Uint8Iterator) Next() bool {
	if it.index < it.col.Len()-1 {
		it.index++
		return true
	}
	return false
}

func (it *Uint8Iterator) Value(i int) any {
	return it.col.Value(i)
}

func (it *Uint8Iterator) Reset() {
	it.index = -1
}

// Uint16Iterator 用于迭代 Uint16 类型的 Arrow 列
type Uint16Iterator struct {
	col   *array.Uint16
	index int
}

func NewUint16Iterator(col *array.Uint16) *Uint16Iterator {
	return &Uint16Iterator{col: col, index: -1}
}

func (it *Uint16Iterator) Next() bool {
	if it.index < it.col.Len()-1 {
		it.index++
		return true
	}
	return false
}

func (it *Uint16Iterator) Value(i int) any {
	return it.col.Value(i)
}

func (it *Uint16Iterator) Reset() {
	it.index = -1
}

// Uint32Iterator 用于迭代 Uint32 类型的 Arrow 列
type Uint32Iterator struct {
	col   *array.Uint32
	index int
}

func NewUint32Iterator(col *array.Uint32) *Uint32Iterator {
	return &Uint32Iterator{col: col, index: -1}
}

func (it *Uint32Iterator) Next() bool {
	if it.index < it.col.Len()-1 {
		it.index++
		return true
	}
	return false
}

func (it *Uint32Iterator) Value(i int) any {
	return it.col.Value(i)
}

func (it *Uint32Iterator) Reset() {
	it.index = -1
}

// Uint64Iterator 用于迭代 Uint64 类型的 Arrow 列
type Uint64Iterator struct {
	col   *array.Uint64
	index int
}

func NewUint64Iterator(col *array.Uint64) *Uint64Iterator {
	return &Uint64Iterator{col: col, index: -1}
}

func (it *Uint64Iterator) Next() bool {
	if it.index < it.col.Len()-1 {
		it.index++
		return true
	}
	return false
}

func (it *Uint64Iterator) Value(i int) any {
	return it.col.Value(i)
}

func (it *Uint64Iterator) Reset() {
	it.index = -1
}

// Float32Iterator 用于迭代 Float32 类型的 Arrow 列
type Float32Iterator struct {
	col   *array.Float32
	index int
}

func NewFloat32Iterator(col *array.Float32) *Float32Iterator {
	return &Float32Iterator{col: col, index: -1}
}

func (it *Float32Iterator) Next() bool {
	if it.index < it.col.Len()-1 {
		it.index++
		return true
	}
	return false
}

func (it *Float32Iterator) Value(i int) any {
	return it.col.Value(i)
}

func (it *Float32Iterator) Reset() {
	it.index = -1
}

// Float64Iterator 用于迭代 Float64 类型的 Arrow 列
type Float64Iterator struct {
	col   *array.Float64
	index int
}

func NewFloat64Iterator(col *array.Float64) *Float64Iterator {
	return &Float64Iterator{col: col, index: -1}
}

func (it *Float64Iterator) Next() bool {
	if it.index < it.col.Len()-1 {
		it.index++
		return true
	}
	return false
}

func (it *Float64Iterator) Value(i int) any {
	return it.col.Value(i)
}

func (it *Float64Iterator) Reset() {
	it.index = -1
}

// StringIterator 用于迭代 String 类型的 Arrow 列
type StringIterator struct {
	col   *array.String
	index int
}

func NewStringIterator(col *array.String) *StringIterator {
	return &StringIterator{col: col, index: -1}
}

func (it *StringIterator) Next() bool {
	if it.index < it.col.Len()-1 {
		it.index++
		return true
	}
	return false
}

func (it *StringIterator) Value(i int) any {
	return it.col.Value(i)
}

func (it *StringIterator) Reset() {
	it.index = -1
}

// TimestampIterator 用于迭代 Timestamp 类型的 Arrow 列
type TimestampIterator struct {
	col   *array.Timestamp
	index int
}

func NewTimestampIterator(col *array.Timestamp) *TimestampIterator {
	return &TimestampIterator{col: col, index: -1}
}

func (it *TimestampIterator) Next() bool {
	if it.index < it.col.Len()-1 {
		it.index++
		return true
	}
	return false
}

func (it *TimestampIterator) Value(i int) any {
	return it.col.Value(i)
}

func (it *TimestampIterator) Reset() {
	it.index = -1
}

// Date32Iterator 用于迭代 Date32 类型的 Arrow 列
type Date32Iterator struct {
	col   *array.Date32
	index int
}

func NewDate32Iterator(col *array.Date32) *Date32Iterator {
	return &Date32Iterator{col: col, index: -1}
}

func (it *Date32Iterator) Next() bool {
	if it.index < it.col.Len()-1 {
		it.index++
		return true
	}
	return false
}

func (it *Date32Iterator) Value(i int) any {
	return it.col.Value(i)
}

func (it *Date32Iterator) Reset() {
	it.index = -1
}

// Date64Iterator 用于迭代 Date64 类型的 Arrow 列
type Date64Iterator struct {
	col   *array.Date64
	index int
}

func NewDate64Iterator(col *array.Date64) *Date64Iterator {
	return &Date64Iterator{col: col, index: -1}
}

func (it *Date64Iterator) Next() bool {
	if it.index < it.col.Len()-1 {
		it.index++
		return true
	}
	return false
}

func (it *Date64Iterator) Value(i int) any {
	return it.col.Value(i)
}

func (it *Date64Iterator) Reset() {
	it.index = -1
}

// Decimal128Iterator 用于迭代 Decimal128 类型的 Arrow 列
type Decimal128Iterator struct {
	col   *array.Decimal128
	index int
}

func NewDecimal128Iterator(col *array.Decimal128) *Decimal128Iterator {
	return &Decimal128Iterator{col: col, index: -1}
}

func (it *Decimal128Iterator) Next() bool {
	if it.index < it.col.Len()-1 {
		it.index++
		return true
	}
	return false
}

func (it *Decimal128Iterator) Value(i int) any {
	return it.col.Value(i)
}

func (it *Decimal128Iterator) Reset() {
	it.index = -1
}

func buildReader(col arrow.Array) (Reader, error) {
	slog.Debug("column type", "col", col.DataType())
	switch col := col.(type) {
	case *array.Boolean:
		return NewBooleanIterator(col), nil
	case *array.Int8:
		return NewInt8Iterator(col), nil
	case *array.Int16:
		return NewInt16Iterator(col), nil
	case *array.Int32:
		return NewInt32Iterator(col), nil
	case *array.Int64:
		return NewInt64Iterator(col), nil
	case *array.Uint8:
		return NewUint8Iterator(col), nil
	case *array.Uint16:
		return NewUint16Iterator(col), nil
	case *array.Uint32:
		return NewUint32Iterator(col), nil
	case *array.Uint64:
		return NewUint64Iterator(col), nil
	case *array.Float32:
		return NewFloat32Iterator(col), nil
	case *array.Float64:
		return NewFloat64Iterator(col), nil
	case *array.String:
		return NewStringIterator(col), nil
	case *array.Timestamp:
		return NewTimestampIterator(col), nil
	case *array.Date32:
		return NewDate32Iterator(col), nil
	case *array.Date64:
		return NewDate64Iterator(col), nil
	case *array.Decimal128:
		return NewDecimal128Iterator(col), nil
	default:
		return nil, fmt.Errorf("unsupported field type: %s", col.DataType())
	}
}

func ParseTable(table arrow.Table) (map[string]Reader, error) {
	res := make(map[string]Reader)
	for i := 0; i < int(table.NumCols()); i++ {
		col := table.Column(i)
		chunk := col.Data().Chunks()[0]
		r, err := buildReader(chunk)
		if err != nil {
			return nil, err
		}
		res[col.Name()] = r
	}
	slog.Debug("table info", "res", res)

	return res, nil
}
