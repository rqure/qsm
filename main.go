package main

import (
	"os"
	"time"

	qdb "github.com/rqure/qdb/src"
)

func getDatabaseAddress() string {
	addr := os.Getenv("QDB_ADDR")
	if addr == "" {
		addr = "redis:6379"
	}

	return addr
}

func main() {
	db := qdb.NewRedisDatabase(qdb.RedisDatabaseConfig{
		Address: getDatabaseAddress(),
	})

	dbWorker := qdb.NewDatabaseWorker(db)
	leaderElectionWorker := qdb.NewLeaderElectionWorker(db)
	clockWorker := NewClockWorker(db, 1*time.Second)
	schemaValidator := qdb.NewSchemaValidator(db)

	schemaValidator.AddEntity("Root", "SchemaUpdateTrigger")
	schemaValidator.AddEntity("SystemClock", "CurrentTime")

	dbWorker.Signals.SchemaUpdated.Connect(qdb.Slot(schemaValidator.ValidationRequired))
	dbWorker.Signals.Connected.Connect(qdb.Slot(schemaValidator.ValidationRequired))
	leaderElectionWorker.AddAvailabilityCriteria(func() bool {
		return schemaValidator.IsValid()
	})

	dbWorker.Signals.Connected.Connect(qdb.Slot(leaderElectionWorker.OnDatabaseConnected))
	dbWorker.Signals.Disconnected.Connect(qdb.Slot(leaderElectionWorker.OnDatabaseDisconnected))

	leaderElectionWorker.Signals.BecameLeader.Connect(qdb.Slot(clockWorker.OnBecameLeader))
	leaderElectionWorker.Signals.LosingLeadership.Connect(qdb.Slot(clockWorker.OnLostLeadership))

	// Create a new application configuration
	config := qdb.ApplicationConfig{
		Name: "clock",
		Workers: []qdb.IWorker{
			dbWorker,
			leaderElectionWorker,
			clockWorker,
		},
	}

	// Create a new application
	app := qdb.NewApplication(config)

	// Execute the application
	app.Execute()
}
