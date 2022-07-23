package gcs

import (
	"context"
	"fmt"

	"cloud.google.com/go/storage"
	"github.com/omniscale/imposm3/database/avro"
)

type GCSWriter struct {
	*storage.Writer
}

func (w *GCSWriter) Location() string {
	return fmt.Sprintf("gs://%s/%s", w.Writer.Bucket, w.Writer.Name)
}

func NewTx(gcs *GCS, spec *avro.TableSpec) avro.AvroTx {

	// Create GCS object writer
	gcsKey := gcs.Prefix + spec.Name + ".avro"
	gcsObject := gcs.Client.Bucket(gcs.Bucket).Object(gcsKey)
	writer := gcsObject.NewWriter(context.Background())

	tt := avro.NewTx(spec, &GCSWriter{writer})
	return tt

}
