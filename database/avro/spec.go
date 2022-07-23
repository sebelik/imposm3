package avro

import (
	"reflect"
	"regexp"

	"github.com/omniscale/imposm3/mapping"
	"github.com/omniscale/imposm3/mapping/config"
	"github.com/pkg/errors"
)

type FieldSpec struct {
	Name        string
	MappingType mapping.ColumnType
	Type        FieldType
}
type TableSpec struct {
	Name         string
	Fields       []FieldSpec
	GeometryType string
	Srid         int
}

type AvroSchema struct {
	Type   AvroType    `json:"type"`
	Name   string      `json:"name"`
	Fields []AvroField `json:"fields,omitempty"`
}

type AvroField struct {
	Type      interface{} `json:"type"`
	Name      string      `json:"name,omitempty"`
	Namespace string      `json:"namespace,omitempty"`
	Default   interface{} `json:"default,omitempty"`
	Fields    []AvroField `json:"fields,omitempty"` // Schema of fields in a Record
	Symbols   []string    `json:"symbols,omitempty"`
	Items     *AvroField  `json:"items,omitempty"` // Schema of items in an Array
}

type AvroType string

const (
	AvroTypeNull   AvroType = "null"
	AvroTypeBool   AvroType = "boolean"
	AvroTypeInt    AvroType = "int"
	AvroTypeLong   AvroType = "long"
	AvroTypeFloat  AvroType = "float"
	AvroTypeDouble AvroType = "double"
	AvroTypeBytes  AvroType = "bytes"
	AvroTypeString AvroType = "string"
	AvroTypeRecord AvroType = "record"
	AvroTypeEnum   AvroType = "enum"
	AvroTypeArray  AvroType = "array"
	AvroTypeMap    AvroType = "map"
	AvroTypeFixed  AvroType = "fixed"
)

func (f *FieldSpec) AvroName() string {
	name := f.Name
	return regexp.MustCompile("[^a-zA-Z0-9_]").ReplaceAllString(name, "_")
}

func (f *FieldSpec) AsAvroFieldSchema() AvroField {

	// Thisis a special key/value nested record type
	if _, ok := f.Type.(*TagsType); ok {
		return AvroField{
			Name: f.AvroName(),
			Type: AvroField{
				Type:    AvroTypeArray,
				Default: []interface{}{},
				Items: &AvroField{
					Type:      AvroTypeRecord,
					Name:      "Tags",
					Namespace: "root",
					Fields: []AvroField{
						{Name: "key", Type: AvroTypeString},
						{Name: "value", Type: AvroTypeString},
					},
				},
			},
		}
	}

	// Primitive data types
	return AvroField{
		Name: f.AvroName(),
		Type: []AvroType{AvroTypeNull, f.Type.AvroType()},
	}

}

func (f *AvroField) PrimaryType() AvroType {

	if val, ok := f.Type.(AvroType); ok {
		return val
	}

	if val, ok := f.Type.([]AvroType); ok {
		for _, t := range val {
			if t != AvroTypeNull {
				return t
			}
		}
	}

	return AvroTypeNull

}

func (t AvroType) ValueAsType(in interface{}) interface{} {
	switch t {
	case AvroTypeString, AvroTypeEnum:
		return reflect.ValueOf(in).String()
	case AvroTypeBool:
		return reflect.ValueOf(in).Bool()
	case AvroTypeBytes:
		return reflect.ValueOf(in).Bytes()
	case AvroTypeDouble, AvroTypeFixed, AvroTypeFloat:
		return reflect.ValueOf(in).Float()
	case AvroTypeInt, AvroTypeLong:
		return reflect.ValueOf(in).Int()
	}
	return in
}

func (spec *TableSpec) AsAvroSchema() AvroSchema {

	schema := AvroSchema{
		Name: spec.Name,
		Type: AvroTypeRecord,
	}

	for _, field := range spec.Fields {
		schema.Fields = append(schema.Fields, field.AsAvroFieldSchema())
	}

	return schema

}

func NewTableSpec(t *config.Table, srid int) (*TableSpec, error) {

	var geomType string

	if mapping.TableType(t.Type) == mapping.RelationMemberTable {
		geomType = "geometry"
	} else {
		geomType = string(t.Type)
	}

	spec := TableSpec{
		Name:         t.Name,
		GeometryType: geomType,
		Srid:         srid,
	}

	for _, column := range t.Columns {

		columnType, err := mapping.MakeColumnType(column)
		if err != nil {
			return nil, err
		}

		bqType, ok := avroTypes[columnType.GoType]
		if !ok {
			return nil, errors.Errorf("unhandled column type %v, using string type", columnType)
		}

		col := FieldSpec{column.Name, *columnType, bqType}
		spec.Fields = append(spec.Fields, col)

	}

	return &spec, nil
}
