package bigquery

import (
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/omniscale/imposm3/log"
)

type FieldType interface {
	Type() bigquery.FieldType
	AvroType() AvroType
	Repeated() bool
	GeneralizeSQL(colSpec *FieldSpec, spec *GeneralizedTableSpec) string
}

type simpleFieldType struct {
	fieldType bigquery.FieldType
	repeated  bool
}

func (t *simpleFieldType) Type() bigquery.FieldType {
	return t.fieldType
}

func (t *simpleFieldType) AvroType() AvroType {

	switch t.fieldType {
	case bigquery.IntegerFieldType:
		return AvroTypeLong
	case bigquery.BigNumericFieldType:
		return AvroTypeLong
	case bigquery.FloatFieldType:
		return AvroTypeFloat
	case bigquery.NumericFieldType:
		return AvroTypeDouble

	case bigquery.BytesFieldType:
		return AvroTypeBytes

	case bigquery.BooleanFieldType:
		return AvroTypeBool

	case bigquery.StringFieldType:
		return AvroTypeString

	case bigquery.GeographyFieldType:
		return AvroTypeString

	case bigquery.RecordFieldType:
		return AvroTypeRecord
	}

	return AvroTypeNull
}

func (t *simpleFieldType) Repeated() bool {
	return t.repeated
}

func (t *simpleFieldType) GeneralizeSQL(colSpec *FieldSpec, spec *GeneralizedTableSpec) string {
	return colSpec.BigQueryName()
}

type geometryType struct {
	fieldType bigquery.FieldType
}

func (t *geometryType) Type() bigquery.FieldType {
	return t.fieldType
}

func (t *geometryType) AvroType() AvroType {
	return AvroTypeString
}

func (t *geometryType) Repeated() bool {
	return false
}

func (t *geometryType) GeneralizeSQL(colSpec *FieldSpec, spec *GeneralizedTableSpec) string {
	return fmt.Sprintf("ST_SIMPLIFY(%s, %f) as %s", colSpec.BigQueryName(), spec.Tolerance, colSpec.BigQueryName())
}

type validatedGeometryType struct {
	geometryType
}

func (t *validatedGeometryType) GeneralizeSQL(colSpec *FieldSpec, spec *GeneralizedTableSpec) string {
	if spec.Source.GeometryType != "polygon" {
		// TODO return warning earlier
		log.Printf("[warn] validated_geometry column returns polygon geometries for %s", spec.Name)
	}
	return fmt.Sprintf("ST_BUFFER(ST_SIMPLIFY(%s, %f), 0) as %s", colSpec.BigQueryName(), spec.Tolerance, colSpec.BigQueryName())
}

var bqTypes map[string]FieldType

func init() {
	bqTypes = map[string]FieldType{
		"string":             &simpleFieldType{bigquery.StringFieldType, false},
		"bool":               &simpleFieldType{bigquery.BooleanFieldType, false},
		"int8":               &simpleFieldType{bigquery.IntegerFieldType, false},
		"int32":              &simpleFieldType{bigquery.IntegerFieldType, false},
		"int64":              &simpleFieldType{bigquery.IntegerFieldType, false},
		"float32":            &simpleFieldType{bigquery.FloatFieldType, false},
		"hstore_string":      &simpleFieldType{bigquery.RecordFieldType, true},
		"geometry":           &geometryType{bigquery.GeographyFieldType},
		"validated_geometry": &validatedGeometryType{geometryType{bigquery.GeographyFieldType}},
	}
}
