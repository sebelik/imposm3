package bigquery

import (
	"context"
	"sync"

	"cloud.google.com/go/bigquery"
	"github.com/omniscale/imposm3/log"
)

type TableImport interface {
	Begin() error
	Insert(row []interface{}) error
	Delete(id int64) error
	End()
}

type tableImport struct {
	Bq       *BigQuery
	Table    *bigquery.Table
	Inserter *bigquery.Inserter
	Spec     *TableSpec
	wg       *sync.WaitGroup
	rows     chan []interface{}
}

func NewTableImport(bq *BigQuery, spec *TableSpec) TableImport {

	table := bq.Client.Dataset(spec.Dataset).Table(spec.Name)

	tt := &tableImport{
		Bq:       bq,
		Table:    table,
		Inserter: table.Inserter(),
		Spec:     spec,
		wg:       &sync.WaitGroup{},
		rows:     make(chan []interface{}, 64),
	}

	tt.wg.Add(1)
	go tt.loop()

	return tt

}

func (tt *tableImport) Begin() error {
	// TODO: maybe recreate the table here?
	return nil
}

func (tt *tableImport) Insert(row []interface{}) error {
	tt.rows <- row
	return nil
}

type RowSaver struct {
	row    []interface{}
	fields *[]FieldSpec
}

func (s *RowSaver) Save() (row map[string]bigquery.Value, insertID string, err error) {

	// The row gets converted into a map
	for i, val := range s.row {
		row[(*s.fields)[i].BigQueryName()] = val
	}

	log.Printf("Saving row: %+v", row)

	return row, bigquery.NoDedupeID, nil

}

func (tt *tableImport) loop() {

	values := []RowSaver{}

	for row := range tt.rows {
		values = append(values, RowSaver{row, &tt.Spec.Fields})
	}

	if err := tt.Inserter.Put(context.Background(), values); err != nil {
		// InsertStmt uses COPY so the error may not be related to this row.
		// Abort the import as the whole transaction is lost anyway.
		log.Fatalf("[fatal] bulk insert into %q: %+v", tt.Table.FullyQualifiedName(), err)
	}

	tt.wg.Done()

}

func (tt *tableImport) Delete(id int64) error {
	panic("unable to delete in bulkImport mode")
}

func (tt *tableImport) End() {
	close(tt.rows)
	tt.wg.Wait()
}
