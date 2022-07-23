package avro

import (
	"net/url"
	"os"
	"path"
	"strings"

	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/database"
	"github.com/omniscale/imposm3/geom"
	"github.com/omniscale/imposm3/log"
	"github.com/omniscale/imposm3/mapping"
	"github.com/omniscale/imposm3/mapping/config"
	"github.com/pkg/errors"
)

// Init creates schema and tables, drops existing data.
func (db *AvroDB) Init() error {
	return nil
}

// Finish creates spatial indices on all tables.
func (db *AvroDB) Finish() error {
	// Not needed, indexes do not exist in AvroDB
	return nil
}

func (db *AvroDB) GeneralizeUpdates() error {
	return nil
}

func (db *AvroDB) Generalize() error {
	return nil
}

type AvroDB struct {
	Path                    string
	Config                  database.Config
	Tables                  map[string]*TableSpec
	txRouter                *TxRouter
	updateGeneralizedTables bool
}

func (db *AvroDB) Open() error {
	err := os.MkdirAll(db.Path, 0777)
	return errors.Wrapf(err, "creating directory %s", db.Path)
}

func (db *AvroDB) InsertPoint(elem osm.Element, geom geom.Geometry, matches []mapping.Match) error {
	for _, match := range matches {
		row := match.Row(&elem, &geom)
		if err := db.txRouter.Insert(match.Table.Name, row); err != nil {
			return err
		}
	}
	return nil
}

func (db *AvroDB) InsertLineString(elem osm.Element, geom geom.Geometry, matches []mapping.Match) error {
	for _, match := range matches {
		row := match.Row(&elem, &geom)
		if err := db.txRouter.Insert(match.Table.Name, row); err != nil {
			return err
		}
	}
	return nil
}

func (db *AvroDB) InsertPolygon(elem osm.Element, geom geom.Geometry, matches []mapping.Match) error {
	for _, match := range matches {
		row := match.Row(&elem, &geom)
		if err := db.txRouter.Insert(match.Table.Name, row); err != nil {
			return err
		}
	}
	return nil
}

func (db *AvroDB) InsertRelationMember(rel osm.Relation, m osm.Member, geom geom.Geometry, matches []mapping.Match) error {
	for _, match := range matches {
		row := match.MemberRow(&rel, &m, &geom)
		if err := db.txRouter.Insert(match.Table.Name, row); err != nil {
			return err
		}
	}
	return nil
}

func (db *AvroDB) Delete(id int64, matches []mapping.Match) error {
	return nil
}

func (db *AvroDB) Begin() error {
	var err error
	db.txRouter, err = newTxRouter(db, false)
	return err
}

func (db *AvroDB) BeginBulk() error {
	var err error
	db.txRouter, err = newTxRouter(db, true)
	return err
}

func (db *AvroDB) Abort() error {
	return db.txRouter.Abort()
}

func (db *AvroDB) End() error {
	return db.txRouter.End()
}

func (db *AvroDB) Close() error {
	return nil
}

func New(conf database.Config, m *config.Mapping) (database.DB, error) {

	db := &AvroDB{
		Tables: make(map[string]*TableSpec),
		Config: conf,
	}

	var err error

	// ConnectionParams is a AvroDB URI like "gs://my-bucket/my-prefix/"
	db.Path = parseAVROPath(db.Config.ConnectionParams)

	for name, table := range m.Tables {
		db.Tables[name], err = NewTableSpec(table, db.Config.Srid)
		if err != nil {
			return nil, errors.Wrapf(err, "creating table spec for %q", name)
		}
	}

	err = db.Open()
	if err != nil {
		return nil, errors.Wrap(err, "opening db")
	}

	return db, nil

}

// parseAVROPath accepts a connection string and returns a parsed
// path on local filesystem where AvroDB files will be stored
//
// The connection string should be in the format:
// 		avro:///path/on/filesystem
func parseAVROPath(connStr string) string {

	if connStr == "" {
		log.Fatal("connection string not specified")
	}

	u, err := url.Parse(connStr)
	if err != nil {
		log.Fatalf("Failed to parse file path %q: %s", connStr, err)
	}

	if u.Scheme != "avro" {
		log.Fatalf("%q is not a file path in the format \"avro://...\"", connStr)
	}

	return path.Clean(strings.Replace(connStr, "avro://", "", 1)) + "/"

}

func init() {
	database.Register("avro", New)
}
