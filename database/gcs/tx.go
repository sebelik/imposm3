package gcs

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"cloud.google.com/go/storage"
	"github.com/lib/pq/hstore"
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
			encodedValue[field.AvroName()] = encodeFieldAvro(field, cell)
		}

		values := []map[string]interface{}{encodedValue}

		if err := tt.AvroWriter.Append(values); err != nil {
			log.Fatalf("[fatal] write into %s: %+v", tt.GCSObject.ObjectName(), err)
		}
	}

	tt.wg.Done()

}

func encodeFieldAvro(field FieldSpec, value interface{}) interface{} {

	avroSchema := field.AsAvroFieldSchema()
	avroType := avroSchema.PrimaryType()

	// Tags must be returned as key/value pair array, but since they
	// are pre-formatted as Potgres hstore type, they need to be parsed
	// first
	if _, ok := field.Type.(*tagsType); ok {

		hst := hstore.Hstore{}
		encodedValue := make([]map[string]interface{}, 0)

		if err := hst.Scan([]byte(value.(string))); err == nil {
			for k, v := range hst.Map {
				encodedValue = append(encodedValue, map[string]interface{}{
					"key":   k,
					"value": v.String,
				})
			}
		}

		return encodedValue

	}

	// Primitive types must be returned as type:value map
	if value == nil {
		return map[string]interface{}{string(AvroTypeNull): nil}
	}

	return map[string]interface{}{string(avroType): avroType.ValueAsType(value)}

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
