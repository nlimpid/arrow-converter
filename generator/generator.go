// generator.go
package main

import (
	"os"
	"text/template"
)

type TypeInfo struct {
	TypeName    string
	Type        string
	ArrowType   string
	BuilderType string
}

func main() {
	types := []TypeInfo{
		{"Int64", "int64", "Int64", "Int64Builder"},
		{"Int32", "int32", "Int32", "Int32Builder"},
		{"Uint64", "int64", "Int64", "Int64Builder"},
		{"Float64", "float64", "Float64", "Float64Builder"},
	}

	tmpl, err := template.ParseFiles("handler.tmpl")
	if err != nil {
		panic(err)
	}

	for _, t := range types {
		file, err := os.Create(t.TypeName + "Handler.go")
		if err != nil {
			panic(err)
		}
		defer file.Close()

		err = tmpl.Execute(file, t)
		if err != nil {
			panic(err)
		}
	}
}
