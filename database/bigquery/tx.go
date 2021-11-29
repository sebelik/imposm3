package bigquery

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/omniscale/imposm3/database/gcs"
	"github.com/omniscale/imposm3/log"
	"github.com/pkg/errors"
)

type TableImport interface {
	Begin() error
	Insert(row []interface{}) error
	Delete(id int64) error
	End() error
}

type tableImport struct {
	Bq         *BigQuery
	Table      *bigquery.Table
	avroImport gcs.AvroImport
	Spec       *TableSpec
}

func NewTableImport(bq *BigQuery, spec *TableSpec) TableImport {

	// Create GCS Avro table spec
	gcsSpec, err := gcs.NewTableSpec(bq.GCS, spec.Table)
	if err != nil {
		log.DefaultLogger.Panicf("[import] creating Avro import table spec: %+v", err)
	}

	tt := &tableImport{
		Bq:         bq,
		Table:      bq.Client.Dataset(spec.Dataset).Table(spec.Name),
		avroImport: gcs.NewAvroImport(bq.GCS, gcsSpec),
		Spec:       spec,
	}

	return tt

}

func (tt *tableImport) Begin() error {
	return tt.avroImport.Begin()
}

func (tt *tableImport) Insert(row []interface{}) error {
	return tt.avroImport.Insert(row)
}

func (tt *tableImport) Delete(id int64) error {
	panic("unable to delete in bulkImport mode")
}

func (tt *tableImport) End() error {

	gcsUri := tt.avroImport.Location()

	// Wait for all rows to be written
	tt.avroImport.End()

	// Create a temporary table for loading from GCS
	// Note this extra step is necessary due to the limitations of BigQuery
	// imports in relation to the GEOGRAPHY data type.
	tempTable := tt.Bq.Client.Dataset(tt.Table.DatasetID).Table(tt.Table.TableID + "_tmp")

	// Create a GCS object reference with proper spec
	gcsRef := bigquery.NewGCSReference(gcsUri)
	gcsRef.SourceFormat = bigquery.Avro
	gcsRef.Compression = bigquery.Deflate

	// Initiate BQ load job from the Avro temp file into the BigQuery temp table
	loader := tempTable.LoaderFrom(gcsRef)
	loader.CreateDisposition = bigquery.CreateIfNeeded
	loader.WriteDisposition = bigquery.WriteTruncate
	loader.Location = tt.Bq.Location

	job, err := loader.Run(context.Background())
	if err != nil {
		return errors.Wrapf(err, "starting BigQuery import from %s", gcsUri)
	}

	// Wait for the job to finish
	status, err := job.Wait(context.Background())
	if err != nil {
		return errors.Wrapf(err, "waiting for BigQuery import from %s to finish", gcsUri)
	}

	if status.Err() != nil {
		return errors.Wrap(err, "BigQuery import failed")
	}

	// Set expiration on the temp table to avoid dataset pollution
	_, err = tempTable.Update(context.Background(), bigquery.TableMetadataToUpdate{
		ExpirationTime: time.Now().Add(24 * time.Hour),
		Description:    fmt.Sprintf(`imposm: Temporary table for loading data into %q`, tt.Table.FullyQualifiedName()),
	}, "")
	if err != nil {
		return errors.Wrapf(err, "creating temporary BigQuery table %s", tempTable.FullyQualifiedName())
	}

	// For some schema updates (e.g. changing data type of a column), BigQuery currently
	// returns internal errors, so it's safer to drop the table before importing.
	err = tt.Bq.deleteTable(tt.Table)
	if err != nil {
		return errors.Wrapf(err, "deleting BigQuery table %s", tt.Table.FullyQualifiedName())
	}

	// Copy the data from the temporary table to the target table
	copyQuery := fmt.Sprintf("CREATE OR REPLACE TABLE `%s.%s` AS ( SELECT * FROM `%s.%s` );", tt.Table.DatasetID, tt.Table.TableID, tempTable.DatasetID, tempTable.TableID)

	// If the table contains a geometry column, makle sure to parse WKB and cluster
	// by geography
	for _, field := range tt.Spec.Fields {
		if field.Type.BigQueryType() == bigquery.GeographyFieldType {
			copyQuery = fmt.Sprintf("CREATE OR REPLACE TABLE `%s.%s` CLUSTER BY geometry AS ( SELECT * EXCEPT(geometry), ST_GEOGFROMWKB(geometry) AS geometry FROM `%s.%s` );", tt.Table.DatasetID, tt.Table.TableID, tempTable.DatasetID, tempTable.TableID)
			break
		}
	}

	query := tt.Bq.Client.Query(copyQuery)

	job, err = query.Run(context.Background())
	if err != nil {
		return errors.Wrapf(err, "copying data from %s to target BigQuery table %s", tempTable.FullyQualifiedName(), tt.Table.FullyQualifiedName())
	}

	// Wait for the copy query to finish
	status, err = job.Wait(context.Background())
	if err != nil {
		return &BigQueryError{copyQuery, err}
	}

	if status.Err() != nil {
		return &BigQueryError{copyQuery, err}
	}

	// Delete the temporary table
	err = tt.Bq.deleteTable(tempTable)
	if err != nil {
		return errors.Wrapf(err, "deleting BigQuery table %s", tempTable.FullyQualifiedName())
	}

	return nil

}
