package conv

import (
	"fmt"
	"github.com/apache/arrow/go/v18/arrow"
	"reflect"
	"strings"
)

type StructArrowInfo struct {
	ArrowName       string
	StructFieldName string
	ArrowType       arrow.DataType
	ArrowTypeName   string
}

func parseArrowTag(tag string) (map[string]string, error) {
	parts := strings.Split(tag, ",")
	tagMap := make(map[string]string)
	for _, part := range parts {
		kv := strings.SplitN(part, "=", 2)
		if len(kv) != 2 {
			return nil, fmt.Errorf("invalid tag format: %s", part)
		}
		key := strings.TrimSpace(kv[0])
		value := strings.TrimSpace(kv[1])
		tagMap[key] = value
	}
	return tagMap, nil
}

func getStructArrowInfo[T any]() ([]*StructArrowInfo, error) {
	var fieldMappings []*StructArrowInfo
	instance := new(T)
	t := reflect.TypeOf(instance)

	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	if t.Kind() != reflect.Struct {
		return nil, fmt.Errorf("instance must be a struct or pointer to struct")
	}

	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		tag := field.Tag.Get("arrow")

		if tag == "" {
			return nil, fmt.Errorf("missing `arrow` tag in field: %s", field.Name)
		}

		tagMap, err := parseArrowTag(tag)
		if err != nil {
			return nil, fmt.Errorf("field %s: %v", field.Name, err)
		}

		arrowName, ok := tagMap["name"]
		if !ok {
			indexStr, ok := tagMap["index"]
			if !ok {
				return nil, fmt.Errorf("missing `name` or `index` in `arrow` tag for field: %s", field.Name)
			}
			arrowName = indexStr // 或者根据 index 进行其他处理
		}

		arrowTypeName := tagMap["arrow_type"]
		arrowType, err := getArrowDataType(field.Type, arrowTypeName)
		if err != nil {
			return nil, fmt.Errorf("field %s: %v", field.Name, err)
		}

		fieldMappings = append(fieldMappings, &StructArrowInfo{
			ArrowName:       arrowName,
			StructFieldName: field.Name,
			ArrowType:       arrowType,
			ArrowTypeName:   arrowTypeName,
		})
	}

	return fieldMappings, nil
}
