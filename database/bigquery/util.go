package bigquery

import (
	"net/url"
	"path"
	"strings"

	"github.com/omniscale/imposm3/log"
)

// parseConnectionString accepts a connection string and returns a parsed
// GCP project ID, BigQuery dataset name and (optionally) processing location.
//
// The connection string should be in the format:
// 		bigquery://ProjectId={...};TempGCSURI=gs://...[;Location={...}]
//
// The "TempGCSURI" parameter must specify a location in GCS to store temporary
// AVRO files beforeloading them to BigQuery.
func parseConnectionString(connStr string) (projectID string, gcsBucket string, gcsPrefix string, location string) {

	connStr = strings.ToLower(connStr)
	connStr = strings.TrimPrefix(connStr, "bigquery:")
	connStr = strings.TrimSpace(strings.TrimPrefix(connStr, "//"))

	var gcsURI string
	params := strings.Split(connStr, ";")

	for _, param := range params {

		property := strings.SplitN(param, "=", 2)

		if len(property) < 2 {
			continue
		}

		switch property[0] {
		case "projectid":
			projectID = property[1]
		case "location":
			location = property[1]
		case "tempgcsuri":
			gcsURI = property[1]
		}

	}

	if gcsURI == "" {
		log.Fatal("TempGCSURI parameter not specified in the connection string")
	}

	u, err := url.Parse(gcsURI)
	if err != nil {
		log.Fatalf("Failed to parse TempGCSURI %q: %s", gcsURI, err)
	}

	if u.Scheme != "gs" {
		log.Fatalf("TempGCSURI %q is not a GCS URI in the format \"gs://...\"", gcsURI)
	}

	gcsBucket = u.Hostname()
	gcsPrefix = strings.TrimLeft(path.Clean(u.EscapedPath()), "/") + "/"

	return

}
