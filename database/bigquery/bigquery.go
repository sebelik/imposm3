package bigquery

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/database"
	"github.com/omniscale/imposm3/geom"
	"github.com/omniscale/imposm3/log"
	"github.com/omniscale/imposm3/mapping"
	"github.com/omniscale/imposm3/mapping/config"
	"github.com/pkg/errors"
)

type BigQueryError struct {
	query         string
	originalError error
}

func (e *BigQueryError) Error() string {
	return fmt.Sprintf("BigQuery Error: %s in query %s", e.originalError.Error(), e.query)
}

func (bq *BigQuery) createTable(spec TableSpec) error {

	table := bq.Client.Dataset(spec.Dataset).Table(spec.Name)

	// Drop the table if exists
	if err := bq.deleteTable(table); err != nil {
		return errors.Wrapf(err, "dropping BigQuery table %s", table.FullyQualifiedName())
	}

	metadata := &bigquery.TableMetadata{
		Name:     spec.Name,
		Location: bq.Location,
		Schema:   spec.AsBigQueryTableSchema(),
		Clustering: &bigquery.Clustering{
			Fields: []string{},
		},
	}

	for _, field := range spec.Fields {
		if field.Type.Type() == bigquery.GeographyFieldType {
			metadata.Clustering.Fields = append(metadata.Clustering.Fields, field.Name)
		}
	}

	return errors.Wrapf(table.Create(context.Background(), metadata), "creating table %s", table.FullyQualifiedName())

}

func (bq *BigQuery) createDatasetIfNotExists(ds *bigquery.Dataset) error {

	ok, err := bq.datasetExists(ds)
	if err != nil {
		return errors.Wrapf(err, "creating BigQuery dataset %s", ds.DatasetID)
	}
	if ok == true {
		return nil
	}

	opts := &bigquery.DatasetMetadata{}

	if bq.Location != "" {
		opts.Location = bq.Location
	}

	err = ds.Create(context.Background(), opts)
	return errors.Wrapf(err, "creating BigQuery dataset %s", ds.DatasetID)

}

func (bq *BigQuery) datasetExists(ds *bigquery.Dataset) (bool, error) {

	// Check if the table exists
	_, err := ds.Metadata(context.Background())
	if err != nil && !strings.Contains(err.Error(), "Not found") {
		return false, errors.Wrapf(err, "checking if dataset %s exists", ds.DatasetID)
	}

	return true, nil

}

func (bq *BigQuery) tableExists(table *bigquery.Table) (bool, error) {

	// Check if the table exists
	_, err := table.Metadata(context.Background())
	if err != nil && !strings.Contains(err.Error(), "Not found") {
		return false, errors.Wrapf(err, "checking if table %s exists", table.FullyQualifiedName())
	}

	return true, nil

}

func (bq *BigQuery) deleteTable(table *bigquery.Table) error {

	// Check if the table exists
	err := table.Delete(context.Background())
	if err != nil && !strings.Contains(err.Error(), "Not found") {
		return errors.Wrapf(err, "deleting table %s", table.FullyQualifiedName())
	}

	return nil

}

// Init creates schema and tables, drops existing data.
func (bq *BigQuery) Init() error {

	ds := bq.Client.Dataset(bq.Config.ImportSchema)

	if exists, err := bq.datasetExists(ds); err != nil || !exists {
		return err
	}

	for _, spec := range bq.Tables {
		if err := bq.createTable(*spec); err != nil {
			return err
		}
	}

	return nil

}

// Finish creates spatial indices on all tables.
func (bq *BigQuery) Finish() error {
	// Not needed, indexes do not exist in BigQuery
	return nil
}

func (bq *BigQuery) GeneralizeUpdates() error {
	defer log.Step("Updating generalized tables")()
	for _, table := range bq.sortedGeneralizedTables() {
		if ids, ok := bq.updatedIDs[table]; ok {
			for _, id := range ids {
				bq.txRouter.Insert(table, []interface{}{id})
			}
		}
	}
	return nil
}

func (bq *BigQuery) Generalize() error {
	defer log.Step("Creating generalized tables")()

	// generalized tables can depend on other generalized tables
	// create tables with non-generalized sources first
	for _, table := range bq.GeneralizedTables {
		if table.SourceGeneralized == nil {
			if err := bq.generalizeTable(table); err != nil {
				return err
			}
			table.created = true
		}
	}

	// next create tables with created generalized sources until
	// no new source is created
	for _, table := range bq.GeneralizedTables {
		if !table.created && table.SourceGeneralized.created {
			if err := bq.generalizeTable(table); err != nil {
				return err
			}
			table.created = true
		}
	}

	return nil

}

func (bq *BigQuery) generalizeTable(table *GeneralizedTableSpec) error {

	defer log.Step(fmt.Sprintf("Generalizing %s into %s", table.Source.Name, table.Name))()

	var where string
	if table.Where != "" {
		where = " WHERE " + table.Where
	}
	var cols []string

	for _, col := range table.Source.Fields {
		cols = append(cols, col.Type.GeneralizeSQL(&col, table))
	}

	columnSQL := strings.Join(cols, ", ")

	var sourceTable string
	if table.SourceGeneralized != nil {
		sourceTable = table.SourceGeneralized.Name
	} else {
		sourceTable = table.Source.Name
	}

	sql := fmt.Sprintf("CREATE OR REPLACE TABLE `%s.%s` AS (SELECT %s FROM `%s.%s` %s)", bq.Config.ImportSchema, table.Name, columnSQL, bq.Config.ImportSchema, sourceTable, where)
	query := bq.Client.Query(sql)

	job, err := query.Run(context.Background())
	if err != nil {
		return &BigQueryError{sql, err}
	}

	result, err := job.Wait(context.Background())
	if err != nil {
		return &BigQueryError{sql, err}
	}

	if err := result.Err(); err != nil {
		return &BigQueryError{sql, err}
	}

	return nil

}

type BigQuery struct {
	Client                  *bigquery.Client
	GCSClient               *storage.Client
	ProjectId               string
	TempGCSBucket           string
	TempGCSPrefix           string
	Location                string
	Config                  database.Config
	Tables                  map[string]*TableSpec
	GeneralizedTables       map[string]*GeneralizedTableSpec
	txRouter                *TxRouter
	updateGeneralizedTables bool

	updateIDsMu sync.Mutex
	updatedIDs  map[string][]int64
}

func (bq *BigQuery) Open() error {
	var err error

	if bq.ProjectId == "" {
		bq.ProjectId = bigquery.DetectProjectID
	}

	bq.Client, err = bigquery.NewClient(context.Background(), bq.ProjectId)
	if err != nil {
		return errors.Wrap(err, "creating BigQuery client")
	}

	bq.GCSClient, err = storage.NewClient(context.Background())
	if err != nil {
		return errors.Wrap(err, "creating GCS client")
	}

	return nil

}

func (bq *BigQuery) InsertPoint(elem osm.Element, geom geom.Geometry, matches []mapping.Match) error {
	for _, match := range matches {
		row := match.Row(&elem, &geom)
		if err := bq.txRouter.Insert(match.Table.Name, row); err != nil {
			return err
		}
	}
	return nil
}

func (bq *BigQuery) InsertLineString(elem osm.Element, geom geom.Geometry, matches []mapping.Match) error {
	for _, match := range matches {
		row := match.Row(&elem, &geom)
		if err := bq.txRouter.Insert(match.Table.Name, row); err != nil {
			return err
		}
	}
	if bq.updateGeneralizedTables {
		genMatches := bq.generalizedFromMatches(matches)
		if len(genMatches) > 0 {
			bq.updateIDsMu.Lock()
			for _, generalizedTable := range genMatches {
				bq.updatedIDs[generalizedTable.Name] = append(bq.updatedIDs[generalizedTable.Name], elem.ID)

			}
			bq.updateIDsMu.Unlock()
		}
	}
	return nil
}

func (bq *BigQuery) InsertPolygon(elem osm.Element, geom geom.Geometry, matches []mapping.Match) error {
	for _, match := range matches {
		row := match.Row(&elem, &geom)
		if err := bq.txRouter.Insert(match.Table.Name, row); err != nil {
			return err
		}
	}
	if bq.updateGeneralizedTables {
		genMatches := bq.generalizedFromMatches(matches)
		if len(genMatches) > 0 {
			bq.updateIDsMu.Lock()
			for _, generalizedTable := range genMatches {
				bq.updatedIDs[generalizedTable.Name] = append(bq.updatedIDs[generalizedTable.Name], elem.ID)

			}
			bq.updateIDsMu.Unlock()
		}
	}
	return nil
}

func (bq *BigQuery) InsertRelationMember(rel osm.Relation, m osm.Member, geom geom.Geometry, matches []mapping.Match) error {
	for _, match := range matches {
		row := match.MemberRow(&rel, &m, &geom)
		if err := bq.txRouter.Insert(match.Table.Name, row); err != nil {
			return err
		}
	}
	return nil
}

func (bq *BigQuery) Delete(id int64, matches []mapping.Match) error {
	for _, match := range matches {
		if err := bq.txRouter.Delete(match.Table.Name, id); err != nil {
			return errors.Wrapf(err, "deleting %d from %q", id, match.Table.Name)
		}
	}
	if bq.updateGeneralizedTables {
		for _, generalizedTable := range bq.generalizedFromMatches(matches) {
			if err := bq.txRouter.Delete(generalizedTable.Name, id); err != nil {
				return errors.Wrapf(err, "deleting %d from %q", id, generalizedTable.Name)
			}
		}
	}
	return nil
}

func (bq *BigQuery) generalizedFromMatches(matches []mapping.Match) []*GeneralizedTableSpec {
	generalizedTables := []*GeneralizedTableSpec{}
	for _, match := range matches {
		tbl := bq.Tables[match.Table.Name]
		generalizedTables = append(generalizedTables, tbl.Generalizations...)
	}
	return generalizedTables
}

func (bq *BigQuery) sortedGeneralizedTables() []string {
	added := map[string]bool{}
	sorted := []string{}

	for len(bq.GeneralizedTables) > len(sorted) {
		for _, tbl := range bq.GeneralizedTables {
			if _, ok := added[tbl.Name]; !ok {
				if tbl.Source != nil || added[tbl.SourceGeneralized.Name] {
					added[tbl.Name] = true
					sorted = append(sorted, tbl.Name)
				}
			}
		}
	}
	return sorted
}

func (bq *BigQuery) EnableGeneralizeUpdates() {
	bq.updateGeneralizedTables = true
	bq.updatedIDs = make(map[string][]int64)
}

func (bq *BigQuery) Begin() error {
	var err error
	bq.txRouter, err = newTxRouter(bq, false)
	return err
}

func (bq *BigQuery) BeginBulk() error {
	var err error
	bq.txRouter, err = newTxRouter(bq, true)
	return err
}

func (bq *BigQuery) Abort() error {
	return bq.txRouter.Abort()
}

func (bq *BigQuery) End() error {
	return bq.txRouter.End()
}

func (bq *BigQuery) Close() error {
	return bq.Client.Close()
}

func New(conf database.Config, m *config.Mapping) (database.DB, error) {
	db := &BigQuery{}

	db.Tables = make(map[string]*TableSpec)
	db.GeneralizedTables = make(map[string]*GeneralizedTableSpec)

	var err error
	db.Config = conf

	// ConnectionParams is a list of semicolon-separated parameters
	// bigquery://ProjectId={PROJECT ID};Location={LOCATION}
	db.ProjectId, db.TempGCSBucket, db.TempGCSPrefix, db.Location = parseConnectionString(db.Config.ConnectionParams)

	for name, table := range m.Tables {
		db.Tables[name], err = NewTableSpec(db, table)
		if err != nil {
			return nil, errors.Wrapf(err, "creating table spec for %q", name)
		}
	}
	for name, table := range m.GeneralizedTables {
		db.GeneralizedTables[name] = NewGeneralizedTableSpec(db, table)
	}
	if err := db.prepareGeneralizedTableSources(); err != nil {
		return nil, errors.Wrap(err, "preparing generalized table sources")
	}
	db.prepareGeneralizations()

	err = db.Open()
	if err != nil {
		return nil, errors.Wrap(err, "opening db")
	}
	return db, nil
}

// prepareGeneralizedTableSources checks if all generalized table have an
// existing source and sets .Source to the original source (works even
// when source is allready generalized).
func (bq *BigQuery) prepareGeneralizedTableSources() error {
	for name, table := range bq.GeneralizedTables {
		if source, ok := bq.Tables[table.SourceName]; ok {
			table.Source = source
		} else if source, ok := bq.GeneralizedTables[table.SourceName]; ok {
			table.SourceGeneralized = source
		} else {
			return errors.Errorf("missing source %q for generalized table %q",
				table.SourceName, name)
		}
	}

	// set source table until all generalized tables have a source
	for filled := true; filled; {
		filled = false
		for _, table := range bq.GeneralizedTables {
			if table.Source == nil {
				if source, ok := bq.GeneralizedTables[table.SourceName]; ok && source.Source != nil {
					table.Source = source.Source
				}
				filled = true
			}
		}
	}
	return nil
}

func (bq *BigQuery) prepareGeneralizations() {
	for _, table := range bq.GeneralizedTables {
		table.Source.Generalizations = append(table.Source.Generalizations, table)
		if source, ok := bq.GeneralizedTables[table.SourceName]; ok {
			source.Generalizations = append(source.Generalizations, table)
		}
	}
}

func init() {
	database.Register("bigquery", New)
}
