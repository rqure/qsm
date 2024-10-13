package main

import (
	"time"

	qdb "github.com/rqure/qdb/src"
)

type HeartbeatManager struct {
	db       qdb.IDatabase
	isLeader bool
}

func NewServiceManager(db qdb.IDatabase) *HeartbeatManager {
	return &HeartbeatManager{
		db: db,
	}
}

func (w *HeartbeatManager) OnBecameLeader() {
	w.isLeader = true
}

func (w *HeartbeatManager) OnLostLeadership() {
	w.isLeader = false
}

func (w *HeartbeatManager) Init() {

}

func (w *HeartbeatManager) Deinit() {

}

func (w *HeartbeatManager) ManageHeartbeats() {
	services := qdb.NewEntityFinder(w.db).Find(qdb.SearchCriteria{
		EntityType: "Service",
	})

	for _, service := range services {
		heartbeatTrigger := &qdb.DatabaseRequest{
			Id:    service.GetId(),
			Field: "HeartbeatTrigger",
		}

		w.db.Read([]*qdb.DatabaseRequest{heartbeatTrigger})

		if !heartbeatTrigger.Success {
			continue
		}

		heartbeatTime := heartbeatTrigger.WriteTime.GetRaw().AsTime()
		if heartbeatTime.Add(qdb.LeaderLeaseTimeout).Before(time.Now()) {
			leader := service.GetField("Leader")
			candidates := service.GetField("Candidates")

			if leader.PullString() != "" {
				leader.PushString("")
			}

			if candidates.PullString() != "" {
				candidates.PushString("")
			}
		}
	}
}

func (w *HeartbeatManager) DoWork() {
	if !w.isLeader {
		return
	}

	w.ManageHeartbeats()
}
