package gcs

import (
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
	Bucket       string
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
	Type    []AvroType  `json:"type"`
	Name    string      `json:"name"`
	Default interface{} `json:"default,omitempty"`
	Fields  []AvroField `json:"fields,omitempty"` // Schema of fields in a Record
	Symbols []string    `json:"symbols,omitempty"`
	Items   *AvroField  `json:"items,omitempty"` // Schema of items in an Array
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

	schema := AvroField{
		Name: f.AvroName(),
		Type: []AvroType{AvroTypeNull, f.Type.AvroType()},
	}

	// Append nested fields
	if f.Type.AvroType() == AvroTypeRecord {
		schema.Type = []AvroType{AvroTypeNull, AvroTypeArray}
		schema.Items = &AvroField{
			Type: []AvroType{AvroTypeRecord},
			Name: f.AvroName(),
			Fields: []AvroField{
				AvroField{Name: "key", Type: []AvroType{AvroTypeString}},
				AvroField{Name: "value", Type: []AvroType{AvroTypeNull, AvroTypeString}},
			},
		}
	}

	return schema

}

func (f *AvroField) PrimaryType() AvroType {
	for _, t := range f.Type {
		if t != AvroTypeNull {
			return t
		}
	}
	return AvroTypeNull
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

func NewTableSpec(gcs *GCS, t *config.Table) (*TableSpec, error) {

	var geomType string

	if mapping.TableType(t.Type) == mapping.RelationMemberTable {
		geomType = "geometry"
	} else {
		geomType = string(t.Type)
	}

	spec := TableSpec{
		Name:         t.Name,
		Bucket:       gcs.Bucket,
		GeometryType: geomType,
		Srid:         gcs.Config.Srid,
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
