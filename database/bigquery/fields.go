package bigquery

import (
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/omniscale/imposm3/log"
)

type FieldType interface {
	Type() bigquery.FieldType
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

func (t *simpleFieldType) Repeated() bool {
	return t.repeated
}

func (t *simpleFieldType) GeneralizeSQL(colSpec *FieldSpec, spec *GeneralizedTableSpec) string {
	return "\"" + colSpec.Name + "\""
}

type geometryType struct {
	fieldType bigquery.FieldType
}

func (t *geometryType) Type() bigquery.FieldType {
	return t.fieldType
}

func (t *geometryType) Repeated() bool {
	return false
}

func (t *geometryType) GeneralizeSQL(colSpec *FieldSpec, spec *GeneralizedTableSpec) string {
	return fmt.Sprintf(`ST_SIMPLIFY("%s", %f) as "%s"`,
		colSpec.Name, spec.Tolerance, colSpec.Name,
	)
}

type validatedGeometryType struct {
	geometryType
}

func (t *validatedGeometryType) GeneralizeSQL(colSpec *FieldSpec, spec *GeneralizedTableSpec) string {
	if spec.Source.GeometryType != "polygon" {
		// TODO return warning earlier
		log.Printf("[warn] validated_geometry column returns polygon geometries for %s", spec.Name)
	}
	return fmt.Sprintf(`ST_BUFFER(ST_SIMPLIFY("%s", %f), 0) as "%s"`,
		colSpec.Name, spec.Tolerance, colSpec.Name,
	)
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
