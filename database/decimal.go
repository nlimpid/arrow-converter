package database

func scalePGDecimal128(precision, scale int64) (int32, int32) {
	// too much
	if precision > 38 {
		precision = 38
		scale = 8
	}
	return int32(precision), int32(scale)

}
