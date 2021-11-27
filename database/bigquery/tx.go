package bigquery

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/bigquery"
	"cloud.google.com/go/storage"
	goavro "github.com/linkedin/goavro/v2"
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
	Bq               *BigQuery
	Table            *bigquery.Table
	GCSTempObject    *storage.ObjectHandle
	avroWriter       *goavro.OCFWriter
	tempObjectWriter *storage.Writer
	Spec             *TableSpec
	wg               *sync.WaitGroup
	rows             chan []interface{}
}

func NewTableImport(bq *BigQuery, spec *TableSpec) TableImport {

	table := bq.Client.Dataset(spec.Dataset).Table(spec.Name)
	gcsKey := bq.TempGCSPrefix + spec.Name + ".avro"
	gcsObject := bq.GCSClient.Bucket(bq.TempGCSBucket).Object(gcsKey)

	tt := &tableImport{
		Bq:            bq,
		Table:         table,
		GCSTempObject: gcsObject,
		Spec:          spec,
		wg:            &sync.WaitGroup{},
		rows:          make(chan []interface{}, 64),
	}

	tt.wg.Add(1)
	go tt.loop()

	return tt

}

func (tt *tableImport) Begin() error {

	// Create GCS object writer
	tt.tempObjectWriter = tt.GCSTempObject.NewWriter(context.Background())

	// Marshal schema to JSON to parse to goavro codec
	schema, err := json.Marshal(tt.Spec.AsAvroSchema())
	if err != nil {
		return errors.Wrap(err, "marshaling Avro schema")
	}

	// Create an Avro writer instance
	tt.avroWriter, err = goavro.NewOCFWriter(goavro.OCFConfig{
		W:               tt.tempObjectWriter,
		Schema:          string(schema),
		CompressionName: goavro.CompressionDeflateLabel,
	})

	return errors.Wrap(err, "creating Avro writer")

}

func (tt *tableImport) Insert(row []interface{}) error {
	tt.rows <- row
	return nil
}

func (tt *tableImport) loop() {

	fields := tt.Spec.Fields

	for row := range tt.rows {

		// Convert each row, which is a slice of values, into a map
		encodedValue := make(map[string]interface{})
		for i, val := range row {

			field := fields[i]

			// Field must be passed as key-value map where key is the Avro type
			// and value is the field value
			var key AvroType = AvroTypeNull
			if val != nil {
				key = field.Type.AvroType()
			}

			encodedValue[field.BigQueryName()] = map[string]interface{}{string(key): val}
		}

		values := []map[string]interface{}{encodedValue}

		if err := tt.avroWriter.Append(values); err != nil {
			// InsertStmt uses COPY so the error may not be related to this row.
			// Abort the import as the whole transaction is lost anyway.
			log.Fatalf("[fatal] insert into %q: %+v", tt.Table.FullyQualifiedName(), err)
		}
	}

	tt.wg.Done()

}

func (tt *tableImport) Delete(id int64) error {
	panic("unable to delete in bulkImport mode")
}

func (tt *tableImport) End() error {

	// Wait for all rows to be written
	close(tt.rows)
	tt.wg.Wait()

	gcsUri := fmt.Sprintf(`gs://%s/%s`, tt.GCSTempObject.BucketName(), tt.GCSTempObject.ObjectName())

	// Close the GCS temp file
	if err := tt.tempObjectWriter.Close(); err != nil {
		return errors.Wrapf(err, "closing GCS object writer: %s", gcsUri)
	}

	// Create a temporary table for loading from GCS
	//
	// Note this extra step is necessary due to the limitations of BigQuery
	// imports in relation to the GEOGRAPHY data type.
	tempTable := tt.Bq.Client.Dataset(tt.Table.DatasetID).Table(tt.Table.TableID + "_tmp")
	// err := tempTable.Create(context.Background(), &bigquery.TableMetadata{
	// 	ExpirationTime: time.Now().Add(24 * time.Hour),
	// 	Location:       tt.Bq.Client.Location,
	// 	Description:    fmt.Sprintf(`imposm: Temporary table for loading data into %q`, tt.Table.FullyQualifiedName()),
	// 	Schema:         tt.Spec.AsBigQueryTableSchema(),
	// })
	// if err != nil {
	// 	return errors.Wrapf(err, "creating temporary BigQuery table %s", tempTable.FullyQualifiedName())
	// }

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
	copyQuery := fmt.Sprintf("CREATE OR REPLACE TABLE `%s.%s` AS ( SELECT * EXCEPT(geometry), ST_GEOGFROMWKB(geometry) AS geometry FROM `%s.%s` );", tt.Table.DatasetID, tt.Table.TableID, tempTable.DatasetID, tempTable.TableID)
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
