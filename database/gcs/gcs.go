package gcs

import (
	"context"
	"net/url"
	"path"
	"strings"

	"cloud.google.com/go/storage"
	osm "github.com/omniscale/go-osm"
	"github.com/omniscale/imposm3/database"
	"github.com/omniscale/imposm3/database/avro"
	"github.com/omniscale/imposm3/geom"
	"github.com/omniscale/imposm3/log"
	"github.com/omniscale/imposm3/mapping"
	"github.com/omniscale/imposm3/mapping/config"
	"github.com/pkg/errors"
)

// Init creates schema and tables, drops existing data.
func (gcs *GCS) Init() error {
	return nil
}

// Finish creates spatial indices on all tables.
func (gcs *GCS) Finish() error {
	// Not needed, indexes do not exist in GCS
	return nil
}

func (gcs *GCS) GeneralizeUpdates() error {
	return nil
}

func (gcs *GCS) Generalize() error {
	return nil
}

type GCS struct {
	Client                  *storage.Client
	Bucket                  string
	Prefix                  string
	Config                  database.Config
	Tables                  map[string]*avro.TableSpec
	txRouter                *TxRouter
	updateGeneralizedTables bool
}

func (gcs *GCS) Open() error {
	var err error

	gcs.Client, err = storage.NewClient(context.Background())
	if err != nil {
		return errors.Wrap(err, "creating GCS client")
	}

	return nil

}

func (gcs *GCS) InsertPoint(elem osm.Element, geom geom.Geometry, matches []mapping.Match) error {
	for _, match := range matches {
		row := match.Row(&elem, &geom)
		if err := gcs.txRouter.Insert(match.Table.Name, row); err != nil {
			return err
		}
	}
	return nil
}

func (gcs *GCS) InsertLineString(elem osm.Element, geom geom.Geometry, matches []mapping.Match) error {
	for _, match := range matches {
		row := match.Row(&elem, &geom)
		if err := gcs.txRouter.Insert(match.Table.Name, row); err != nil {
			return err
		}
	}
	return nil
}

func (gcs *GCS) InsertPolygon(elem osm.Element, geom geom.Geometry, matches []mapping.Match) error {
	for _, match := range matches {
		row := match.Row(&elem, &geom)
		if err := gcs.txRouter.Insert(match.Table.Name, row); err != nil {
			return err
		}
	}
	return nil
}

func (gcs *GCS) InsertRelationMember(rel osm.Relation, m osm.Member, geom geom.Geometry, matches []mapping.Match) error {
	for _, match := range matches {
		row := match.MemberRow(&rel, &m, &geom)
		if err := gcs.txRouter.Insert(match.Table.Name, row); err != nil {
			return err
		}
	}
	return nil
}

func (gcs *GCS) Delete(id int64, matches []mapping.Match) error {
	return nil
}

func (gcs *GCS) Begin() error {
	var err error
	gcs.txRouter, err = newTxRouter(gcs, false)
	return err
}

func (gcs *GCS) BeginBulk() error {
	var err error
	gcs.txRouter, err = newTxRouter(gcs, true)
	return err
}

func (gcs *GCS) Abort() error {
	return gcs.txRouter.Abort()
}

func (gcs *GCS) End() error {
	return gcs.txRouter.End()
}

func (gcs *GCS) Close() error {
	return gcs.Client.Close()
}

func New(conf database.Config, m *config.Mapping) (database.DB, error) {

	db := &GCS{
		Tables: make(map[string]*avro.TableSpec),
		Config: conf,
	}

	var err error

	// ConnectionParams is a GCS URI like "gs://my-bucket/my-prefix/"
	db.Bucket, db.Prefix = parseGCSURI(db.Config.ConnectionParams)

	for name, table := range m.Tables {
		db.Tables[name], err = avro.NewTableSpec(table, conf.Srid)
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

// parseGCSURI accepts a connection string and returns a parsed
// bucket name and object prefix
//
// The connection string should be in the format:
// 		gs://{BUCKET NAME}[{OBJECTPREFIX}]
func parseGCSURI(connStr string) (gcsBucket string, gcsPrefix string) {

	if connStr == "" {
		log.Fatal("connection string not specified")
	}

	u, err := url.Parse(connStr)
	if err != nil {
		log.Fatalf("Failed to parse GCS URI %q: %s", connStr, err)
	}

	if u.Scheme != "gs" {
		log.Fatalf("%q is not a GCS URI in the format \"gs://...\"", connStr)
	}

	gcsBucket = u.Hostname()
	gcsPrefix = strings.TrimLeft(path.Clean(u.EscapedPath()), "/") + "/"

	return

}

func init() {
	database.Register("gs", New)
}
