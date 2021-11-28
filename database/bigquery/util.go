package bigquery

import (
	"strings"
)

// parseConnectionString accepts a connection string and returns a parsed
// GCP project ID, BigQuery dataset name and (optionally) processing location.
//
// The connection string should be in the format:
// 		bigquery://ProjectId={...};TempGCSURI=gs://...[;Location={...}]
//
// The "TempGCSURI" parameter must specify a location in GCS to store temporary
// AVRO files beforeloading them to BigQuery.
func parseConnectionString(connStr string) (projectID string, tempGCSUri string, location string) {

	connStr = strings.ToLower(connStr)
	connStr = strings.TrimPrefix(connStr, "bigquery:")
	connStr = strings.TrimSpace(strings.TrimPrefix(connStr, "//"))

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
			tempGCSUri = property[1]
		}

	}

	return

}
