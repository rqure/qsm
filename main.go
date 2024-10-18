package main

import (
	"os"

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
	serviceManager := NewServiceManager(db)
	containerManager := NewContainerManager(db)
	schemaValidator := qdb.NewSchemaValidator(db)
	schemaValidator.AddEntity("Container", "ContainerName", "ContainerId", "ContainerImage", "IsLeader", "CreateTime", "StartTime", "ContainerState", "ContainerStatus", "MemoryUsage", "CPUUsage", "ResetTrigger", "RestartContainers", "MACAddress", "IPAddress")

	dbWorker.Signals.SchemaUpdated.Connect(qdb.Slot(schemaValidator.ValidationRequired))
	dbWorker.Signals.Connected.Connect(qdb.Slot(schemaValidator.ValidationRequired))
	leaderElectionWorker.AddAvailabilityCriteria(func() bool {
		return dbWorker.IsConnected() && schemaValidator.IsValid()
	})

	dbWorker.Signals.Connected.Connect(qdb.Slot(leaderElectionWorker.OnDatabaseConnected))
	dbWorker.Signals.Disconnected.Connect(qdb.Slot(leaderElectionWorker.OnDatabaseDisconnected))

	leaderElectionWorker.Signals.BecameLeader.Connect(qdb.Slot(serviceManager.OnBecameLeader))
	leaderElectionWorker.Signals.LosingLeadership.Connect(qdb.Slot(serviceManager.OnLostLeadership))

	leaderElectionWorker.Signals.BecameLeader.Connect(qdb.Slot(containerManager.OnBecameLeader))
	leaderElectionWorker.Signals.LosingLeadership.Connect(qdb.Slot(containerManager.OnLostLeadership))

	// Create a new application configuration
	config := qdb.ApplicationConfig{
		Name: "qsm",
		Workers: []qdb.IWorker{
			dbWorker,
			leaderElectionWorker,
			serviceManager,
			containerManager,
		},
	}

	// Create a new application
	app := qdb.NewApplication(config)

	// Execute the application
	app.Execute()
}
