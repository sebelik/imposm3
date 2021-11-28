package bigquery

import (
	"fmt"

	"cloud.google.com/go/bigquery"
	"github.com/omniscale/imposm3/log"
)

type FieldType interface {
	BigQueryType() bigquery.FieldType
	GeneralizeSQL(colSpec *FieldSpec, spec *GeneralizedTableSpec) string
}

type simpleFieldType struct {
	fieldType bigquery.FieldType
}

func (t *simpleFieldType) BigQueryType() bigquery.FieldType {
	return t.fieldType
}

func (t *simpleFieldType) GeneralizeSQL(colSpec *FieldSpec, spec *GeneralizedTableSpec) string {
	return colSpec.BigQueryName()
}

type geometryType struct {
	fieldType bigquery.FieldType
}

func (t *geometryType) BigQueryType() bigquery.FieldType {
	return t.fieldType
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

type tagsType struct{}

func (t *tagsType) BigQueryType() bigquery.FieldType {
	return bigquery.RecordFieldType
}

func (t *tagsType) GeneralizeSQL(colSpec *FieldSpec, spec *GeneralizedTableSpec) string {
	return colSpec.BigQueryName()
}

var bqTypes map[string]FieldType

func init() {
	bqTypes = map[string]FieldType{
		"string":             &simpleFieldType{bigquery.StringFieldType},
		"bool":               &simpleFieldType{bigquery.BooleanFieldType},
		"int8":               &simpleFieldType{bigquery.IntegerFieldType},
		"int32":              &simpleFieldType{bigquery.IntegerFieldType},
		"int64":              &simpleFieldType{bigquery.IntegerFieldType},
		"float32":            &simpleFieldType{bigquery.FloatFieldType},
		"hstore_string":      &tagsType{},
		"geometry":           &geometryType{bigquery.GeographyFieldType},
		"validated_geometry": &validatedGeometryType{geometryType{bigquery.GeographyFieldType}},
	}
}
