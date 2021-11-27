package bigquery

import (
	"context"

	"cloud.google.com/go/bigquery"
	"github.com/omniscale/imposm3/log"
)

func (bq *BigQuery) rotate(source, dest, backup string) error {

	defer log.Step("Rotating tables")()

	sourceDS := bq.Client.Dataset(source)
	destDS := bq.Client.Dataset(dest)
	backupDS := bq.Client.Dataset(backup)

	if err := bq.createDatasetIfNotExists(destDS); err != nil {
		return err
	}

	if err := bq.createDatasetIfNotExists(backupDS); err != nil {
		return err
	}

	for _, tableName := range bq.tableNames() {

		sourceTable := sourceDS.Table(tableName)
		destTable := destDS.Table(tableName)
		backupTable := backupDS.Table(tableName)

		log.Printf("[info] Rotating %s from %s -> %s -> %s", tableName, sourceTable.FullyQualifiedName(), destTable.FullyQualifiedName(), backupTable.FullyQualifiedName())

		sourceExists, err := bq.tableExists(sourceTable)
		if err != nil {
			return err
		}
		destExists, err := bq.tableExists(destTable)
		if err != nil {
			return err
		}

		if !sourceExists {
			log.Printf("[warn] skipping rotate of %s, table does not exists in %s", tableName, source)
			continue
		}

		// Take a snapshot of the destination table before overwriting it
		if destExists {

			log.Printf("[info] backup of %s, to %s", tableName, backup)

			copier := backupTable.CopierFrom(destTable)
			copier.CreateDisposition = bigquery.CreateIfNeeded
			copier.WriteDisposition = bigquery.WriteTruncate
			copier.OperationType = bigquery.SnapshotOperation

			job, err := copier.Run(context.Background())
			if err != nil {
				return err
			}

			if _, err := job.Wait(context.Background()); err != nil {
				return err
			}

		}

		// Copy data from the source to destination table, truncating it if needed
		copier := destTable.CopierFrom(sourceTable)
		copier.CreateDisposition = bigquery.CreateIfNeeded
		copier.WriteDisposition = bigquery.WriteTruncate
		copier.OperationType = bigquery.CopyOperation

		job, err := copier.Run(context.Background())
		if err != nil {
			return err
		}

		if _, err := job.Wait(context.Background()); err != nil {
			return err
		}

	}

	return nil
}

func (bq *BigQuery) Deploy() error {
	return bq.rotate(bq.Config.ImportSchema, bq.Config.ProductionSchema, bq.Config.BackupSchema)
}

func (bq *BigQuery) RevertDeploy() error {
	return bq.rotate(bq.Config.BackupSchema, bq.Config.ProductionSchema, bq.Config.ImportSchema)
}

func (bq *BigQuery) RemoveBackup() error {

	backup := bq.Config.BackupSchema

	for _, tableName := range bq.tableNames() {

		table := bq.Client.Dataset(backup).Table(tableName)
		log.Printf("[info] removing backup of %s from %s", tableName, backup)

		if err := bq.deleteTable(table); err != nil {
			return err
		}

	}

	return nil

}

// tableNames returns a list of all tables (without prefix).
func (bq *BigQuery) tableNames() []string {
	var names []string
	for name := range bq.Tables {
		names = append(names, name)
	}
	for name := range bq.GeneralizedTables {
		names = append(names, name)
	}
	return names
}
