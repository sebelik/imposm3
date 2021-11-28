package gcs

import (
	"github.com/pkg/errors"
)

// TxRouter routes inserts/deletes to TableTx
type TxRouter struct {
	Tables map[string]AvroImport
	gcs    *GCS
}

func newTxRouter(gcs *GCS, bulkImport bool) (*TxRouter, error) {
	txr := TxRouter{
		Tables: make(map[string]AvroImport),
	}

	for tableName, table := range gcs.Tables {
		tt := NewAvroImport(gcs, table)
		err := tt.Begin()
		if err != nil {
			return nil, err
		}
		txr.Tables[tableName] = tt
	}

	return &txr, nil
}

func (txr *TxRouter) End() error {
	var outErr error
	for _, tt := range txr.Tables {
		if err := tt.End(); err != nil {
			outErr = err
		}
	}
	return outErr
}

func (txr *TxRouter) Abort() error {
	for _, tt := range txr.Tables {
		tt.End()
	}
	return nil
}

func (txr *TxRouter) Insert(table string, row []interface{}) error {
	tt, ok := txr.Tables[table]
	if !ok {
		return errors.New("Insert into unknown table " + table)
	}
	return tt.Insert(row)
}

func (txr *TxRouter) Delete(table string, id int64) error {
	tt, ok := txr.Tables[table]
	if !ok {
		return errors.New("Delete from unknown table " + table)
	}
	return tt.Delete(id)
}
