package bigquery

import (
	"fmt"
	"regexp"
	"strings"

	"cloud.google.com/go/bigquery"
	"github.com/omniscale/imposm3/mapping"
	"github.com/omniscale/imposm3/mapping/config"
	"github.com/pkg/errors"
)

type FieldSpec struct {
	Name        string
	MappingType mapping.ColumnType
	Type        FieldType
	Fields      []FieldSpec // Nested fields
}
type TableSpec struct {
	Name            string
	Dataset         string
	Fields          []FieldSpec
	GeometryType    string
	Srid            int
	Generalizations []*GeneralizedTableSpec
}

type GeneralizedTableSpec struct {
	Name              string
	Dataset           string
	SourceName        string
	Source            *TableSpec
	SourceGeneralized *GeneralizedTableSpec
	Tolerance         float64
	Where             string
	created           bool
	Generalizations   []*GeneralizedTableSpec
}

func (f *FieldSpec) BigQueryName() string {
	// "BigQuery fields must contain only letters, numbers, and underscores,
	// start with a letter or underscore, and be at most 300 characters long."
	name := f.Name
	return regexp.MustCompile("[^a-zA-Z0-9_]").ReplaceAllString(name, "_")
}

func (f *FieldSpec) AsBigQueryFieldSchema() *bigquery.FieldSchema {

	schema := &bigquery.FieldSchema{
		Name:     f.BigQueryName(),
		Type:     f.Type.Type(),
		Repeated: f.Type.Repeated(),
	}

	// Append nested fields
	if len(f.Fields) > 0 {
		for _, nestedField := range f.Fields {
			schema.Schema = append(schema.Schema, nestedField.AsBigQueryFieldSchema())
		}
	}

	return schema

}

func (spec *TableSpec) BigQuerySchema() bigquery.Schema {

	schema := bigquery.Schema{}

	for _, field := range spec.Fields {
		schema = append(schema, field.AsBigQueryFieldSchema())
	}

	return schema

}

func (spec *TableSpec) BigQueryTable() bigquery.Table {
	return bigquery.Table{DatasetID: spec.Dataset, TableID: spec.Name}
}

func (spec *TableSpec) DeleteSQL() string {
	var idColumnName string
	for _, col := range spec.Fields {
		if col.Name == "id" {
			idColumnName = col.Name
			break
		}
	}

	if idColumnName == "" {
		panic("missing id column")
	}

	return fmt.Sprintf(`DELETE FROM "%s"."%s" WHERE "%s" = $1`, spec.Dataset, spec.Name, idColumnName)
}

func NewTableSpec(bq *BigQuery, t *config.Table) (*TableSpec, error) {
	var geomType string
	if mapping.TableType(t.Type) == mapping.RelationMemberTable {
		geomType = "geometry"
	} else {
		geomType = string(t.Type)
	}

	spec := TableSpec{
		Name:         t.Name,
		Dataset:      bq.Config.ImportSchema,
		GeometryType: geomType,
		Srid:         bq.Config.Srid,
	}

	for _, column := range t.Columns {

		columnType, err := mapping.MakeColumnType(column)
		if err != nil {
			return nil, err
		}

		bqType, ok := bqTypes[columnType.GoType]
		if !ok {
			return nil, errors.Errorf("unhandled column type %v, using string type", columnType)
		}

		col := FieldSpec{column.Name, *columnType, bqType, nil}
		spec.Fields = append(spec.Fields, col)

	}

	return &spec, nil
}

func NewGeneralizedTableSpec(bq *BigQuery, t *config.GeneralizedTable) *GeneralizedTableSpec {
	spec := GeneralizedTableSpec{
		Name:       t.Name,
		Dataset:    bq.Config.ImportSchema,
		Tolerance:  t.Tolerance,
		Where:      t.SQLFilter,
		SourceName: t.SourceTableName,
	}
	return &spec
}

func (spec *GeneralizedTableSpec) DeleteSQL() string {
	var idColumnName string
	for _, col := range spec.Source.Fields {
		if col.Name == "id" {
			idColumnName = col.Name
			break
		}
	}

	if idColumnName == "" {
		panic("missing id column")
	}

	return fmt.Sprintf(`DELETE FROM "%s"."%s" WHERE "%s" = $1`, spec.Dataset, spec.Name, idColumnName)
}

func (spec *GeneralizedTableSpec) InsertSQL() string {
	var idColumnName string
	for _, col := range spec.Source.Fields {
		if col.Name == "id" {
			idColumnName = col.Name
			break
		}
	}

	if idColumnName == "" {
		panic("missing id column")
	}

	var cols []string
	for _, col := range spec.Source.Fields {
		cols = append(cols, col.Type.GeneralizeSQL(&col, spec))
	}

	where := fmt.Sprintf(` WHERE "%s" = $1`, idColumnName)
	if spec.Where != "" {
		where += " AND (" + spec.Where + ")"
	}

	columnSQL := strings.Join(cols, ",\n")
	sql := fmt.Sprintf(`INSERT INTO "%s"."%s" (SELECT %s FROM "%s"."%s"%s)`, spec.Dataset, spec.Name, columnSQL, spec.Source.Dataset, spec.Source.Name, where)
	return sql

}
