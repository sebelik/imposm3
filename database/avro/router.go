package avro

import (
	"os"
	"path"

	"github.com/pkg/errors"
)

// TxRouter routes inserts/deletes to TableTx
type TxRouter struct {
	Tables map[string]AvroTx
	db     *AvroDB
}

type FileWriter struct {
	*os.File
	Name string
}

func (w *FileWriter) Location() string {
	return w.Name
}

func newTxRouter(db *AvroDB, bulkImport bool) (*TxRouter, error) {
	txr := TxRouter{
		Tables: make(map[string]AvroTx),
	}

	for tableName, table := range db.Tables {

		var fpath = path.Join(db.Path, tableName+".avro")
		file, err := os.OpenFile(fpath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0777)
		if err != nil {
			return nil, errors.Wrapf(err, "opening file %q", fpath)
		}

		tt := NewTx(table, &FileWriter{file, fpath})
		err = tt.Begin()
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
