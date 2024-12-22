package main

import (
	"os"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/app/workers"
	"github.com/rqure/qlib/pkg/data/store"
)

func getStoreAddress() string {
	addr := os.Getenv("Q_ADDR")
	if addr == "" {
		addr = "ws://webgateway:20000/ws"
	}

	return addr
}

func main() {
	s := store.NewWeb(store.WebConfig{
		Address: getStoreAddress(),
	})

	storeWorker := workers.NewStore(s)
	leadershipWorker := workers.NewLeadership(s)
	serviceManager := NewServiceManager(s)
	containerManager := NewContainerManager(s)

	schemaValidator := leadershipWorker.GetEntityFieldValidator()
	schemaValidator.RegisterEntityFields("Container", "ContainerName", "ContainerId", "ContainerImage", "IsLeader", "CreateTime", "StartTime", "ContainerState", "ContainerStatus", "MemoryUsage", "CPUUsage", "ResetTrigger", "MACAddress", "IPAddress")

	storeWorker.Connected.Connect(leadershipWorker.OnStoreConnected)
	storeWorker.Disconnected.Connect(leadershipWorker.OnStoreDisconnected)

	leadershipWorker.BecameLeader().Connect(serviceManager.OnBecameLeader)
	leadershipWorker.LosingLeadership().Connect(serviceManager.OnLostLeadership)

	leadershipWorker.BecameLeader().Connect(containerManager.OnBecameLeader)
	leadershipWorker.LosingLeadership().Connect(containerManager.OnLostLeadership)

	a := app.NewApplication("qsm")
	a.AddWorker(storeWorker)
	a.AddWorker(leadershipWorker)
	a.AddWorker(serviceManager)
	a.AddWorker(containerManager)
	a.Execute()
}
