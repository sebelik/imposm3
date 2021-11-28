package gcs

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"cloud.google.com/go/storage"
	goavro "github.com/linkedin/goavro/v2"
	"github.com/omniscale/imposm3/log"
	"github.com/pkg/errors"
)

type AvroImport interface {
	Begin() error
	Insert(row []interface{}) error
	Delete(id int64) error
	End() error
	Location() string
}

type GCSAvroImport struct {
	Gcs          *GCS
	GCSObject    *storage.ObjectHandle
	AvroWriter   *goavro.OCFWriter
	ObjectWriter *storage.Writer
	Spec         *TableSpec
	wg           *sync.WaitGroup
	rows         chan []interface{}
}

func NewAvroImport(gcs *GCS, spec *TableSpec) AvroImport {

	gcsKey := gcs.Prefix + spec.Name + ".avro"
	gcsObject := gcs.Client.Bucket(gcs.Bucket).Object(gcsKey)

	tt := &GCSAvroImport{
		Gcs:       gcs,
		GCSObject: gcsObject,
		Spec:      spec,
		wg:        &sync.WaitGroup{},
		rows:      make(chan []interface{}, 64),
	}

	tt.wg.Add(1)
	go tt.loop()

	return tt

}

func (tt *GCSAvroImport) Begin() error {

	// Create GCS object writer
	tt.ObjectWriter = tt.GCSObject.NewWriter(context.Background())

	// Marshal schema to JSON to parse to goavro codec
	schema, err := json.Marshal(tt.Spec.AsAvroSchema())
	if err != nil {
		return errors.Wrap(err, "marshaling Avro schema")
	}

	// Create an Avro writer instance
	tt.AvroWriter, err = goavro.NewOCFWriter(goavro.OCFConfig{
		W:               tt.ObjectWriter,
		Schema:          string(schema),
		CompressionName: goavro.CompressionDeflateLabel,
	})

	return errors.Wrap(err, "creating Avro writer")

}

func (tt *GCSAvroImport) Insert(row []interface{}) error {
	tt.rows <- row
	return nil
}

func (tt *GCSAvroImport) loop() {

	fields := tt.Spec.Fields

	for row := range tt.rows {

		// Convert each row, which is a slice of values, into a map
		encodedValue := make(map[string]interface{})
		for i, cell := range row {
			field := fields[i]
			encodedValue[field.AvroName()] = encodeFieldAvro(field.AsAvroFieldSchema(), cell)
		}

		values := []map[string]interface{}{encodedValue}

		if err := tt.AvroWriter.Append(values); err != nil {
			log.Fatalf("[fatal] write into %s: %+v", tt.GCSObject.ObjectName(), err)
		}
	}

	tt.wg.Done()

}

func encodeFieldAvro(field AvroField, value interface{}) map[string]interface{} {

	avroType := field.PrimaryType()
	encodedValue := value

	// Field must be passed as key-value map where key is the Avro type
	// and value is the field value
	var key = string(AvroTypeNull)

	if encodedValue != nil {

		key = string(avroType)

		// Records must be re-encoded
		if avroType == AvroTypeRecord {

			valueMap := value.(map[string]interface{})

			for _, nestedField := range field.Fields {
				encodedValue = encodeFieldAvro(nestedField, valueMap[nestedField.Name])
			}

		}

	}

	return map[string]interface{}{key: encodedValue}

}

func (tt *GCSAvroImport) Delete(id int64) error {
	panic("unable to delete in bulkImport mode")
}

func (tt *GCSAvroImport) End() error {

	// Wait for all rows to be written
	close(tt.rows)
	tt.wg.Wait()

	gcsUri := tt.Location()

	// Close the GCS temp file
	if err := tt.ObjectWriter.Close(); err != nil {
		return errors.Wrapf(err, "closing GCS object writer: %s", gcsUri)
	}

	return nil

}

func (tt *GCSAvroImport) Location() string {
	return fmt.Sprintf(`gs://%s/%s`, tt.GCSObject.BucketName(), tt.GCSObject.ObjectName())
}
