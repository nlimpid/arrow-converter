package conv

//
//func StructsToParquet[T any](data []T, filename string) error {
//	// Get field mappings
//	fieldMappings, err := getFieldMappings[T]()
//	if err != nil {
//		return err
//	}
//
//	// Create Arrow schema
//	fields := make([]arrow.Field, len(fieldMappings))
//	for i, mapping := range fieldMappings {
//		fields[i] = arrow.Field{Name: mapping.StructField, Type: mapping.FieldType}
//	}
//	schema := arrow.NewSchema(fields, nil)
//
//	// Create Arrow arrays
//	arrays, err := createArrowArrays(data, fieldMappings)
//	if err != nil {
//		return err
//	}
//	defer func() {
//		for _, arr := range arrays {
//			arr.Release()
//		}
//	}()
//
//	// Create Arrow table
//	table := array.NewTable(schema, arrays, int64(len(data)))
//	defer table.Release()
//
//	// Write to Parquet file
//	return writeTableToParquet(table, filename)
//}
//
//func createArrowArrays[T any](data []T, fieldMappings []FieldMapping) ([]arrow.Array, error) {
//	arrays := make([]arrow.Array, len(fieldMappings))
//	for i, mapping := range fieldMappings {
//		builder := array.NewBuilder(memory.DefaultAllocator, mapping.FieldType)
//		defer builder.Release()
//
//		for _, item := range data {
//			value := reflect.ValueOf(item).FieldByName(mapping.StructField)
//			if err := appendToBuilder(builder, value); err != nil {
//				return nil, err
//			}
//		}
//
//		arrays[i] = builder.NewArray()
//	}
//	return arrays, nil
//}
//
//func appendToBuilder(builder array.Builder, value reflect.Value) error {
//	switch builder := builder.(type) {
//	case *array.Int32Builder:
//		builder.Append(int32(value.Int()))
//	case *array.Int64Builder:
//		builder.Append(value.Int())
//	case *array.Float64Builder:
//		builder.Append(value.Float())
//	case *array.StringBuilder:
//		builder.Append(value.String())
//	case *array.BooleanBuilder:
//		builder.Append(value.Bool())
//	// Add more cases for other types as needed
//	default:
//		return fmt.Errorf("unsupported builder type: %T", builder)
//	}
//	return nil
//}
//
//func writeTableToParquet(table arrow.Table, filename string) error {
//
//	// Open Parquet file for writing
//	file, err := parquet.NewFileDecryptionProperties(filename)
//	if err != nil {
//		return err
//	}
//	defer file.Close()
//
//	// Create Arrow to Parquet writer
//	writerProps := pqarrow.NewArrowWriterProperties(pqarrow.WithStoreSchema())
//	arrowWriter, err := pqarrow.NewFileWriter(table.Schema(), file, writerProps, nil)
//	if err != nil {
//		return err
//	}
//
//	// Write the table
//	if err := arrowWriter.Write(context.Background(), table); err != nil {
//		return err
//	}
//
//	return nil
//}
