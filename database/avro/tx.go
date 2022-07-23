package avro

import (
	"encoding/json"
	"io"
	"sync"

	"github.com/lib/pq/hstore"
	goavro "github.com/linkedin/goavro/v2"
	"github.com/omniscale/imposm3/log"
	"github.com/pkg/errors"
)

type AvroTx interface {
	Begin() error
	Insert(row []interface{}) error
	Delete(id int64) error
	End() error
	Location() string
}

type AvroWriter interface {
	io.Writer
	io.Closer
	Location() string
}

// provides write methods for a single table
type AvroTable struct {
	AvroDBWriter *goavro.OCFWriter
	Spec         *TableSpec
	Writer       AvroWriter
	wg           *sync.WaitGroup
	rows         chan []interface{}
}

func NewTx(spec *TableSpec, writer AvroWriter) AvroTx {

	tt := &AvroTable{
		Spec:   spec,
		Writer: writer,
		wg:     &sync.WaitGroup{},
		rows:   make(chan []interface{}, 64),
	}

	tt.wg.Add(1)
	go tt.loop()

	return tt

}

func (tt *AvroTable) Begin() (err error) {

	// Marshal schema to JSON to parse to goavro codec
	schema, err := json.Marshal(tt.Spec.AsAvroSchema())
	if err != nil {
		return errors.Wrap(err, "marshaling AvroDB schema")
	}

	// Create an AvroDB writer instance
	tt.AvroDBWriter, err = goavro.NewOCFWriter(goavro.OCFConfig{
		W:               tt.Writer,
		Schema:          string(schema),
		CompressionName: goavro.CompressionDeflateLabel,
	})

	return errors.Wrap(err, "creating AvroDB writer")

}

func (tt *AvroTable) Insert(row []interface{}) error {
	tt.rows <- row
	return nil
}

func (tt *AvroTable) loop() {

	fields := tt.Spec.Fields

	for row := range tt.rows {

		// Convert each row, which is a slice of values, into a map
		encodedValue := make(map[string]interface{})
		for i, cell := range row {
			field := fields[i]
			encodedValue[field.AvroName()] = encodeFieldAvroDB(field, cell)
		}

		values := []map[string]interface{}{encodedValue}

		if err := tt.AvroDBWriter.Append(values); err != nil {
			log.Fatalf("[fatal] write into %+v", tt.Spec.Name, err)
		}
	}

	tt.wg.Done()

}

func encodeFieldAvroDB(field FieldSpec, value interface{}) interface{} {

	avroSchema := field.AsAvroFieldSchema()
	avroType := avroSchema.PrimaryType()

	// Tags must be returned as key/value pair array, but since they
	// are pre-formatted as Potgres hstore type, they need to be parsed
	// first
	if _, ok := field.Type.(*TagsType); ok {

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

func (tt *AvroTable) Delete(id int64) error {
	panic("unable to delete in bulkImport mode")
}

func (tt *AvroTable) End() error {

	// Wait for all rows to be written
	close(tt.rows)
	tt.wg.Wait()

	// Close the write stream
	if err := tt.Writer.Close(); err != nil {
		return errors.Wrap(err, "closing object writer")
	}

	return nil

}

func (tt *AvroTable) Location() string {
	return tt.Writer.Location()
}
