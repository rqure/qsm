package main

import (
	"time"

	qdb "github.com/rqure/qdb/src"
)

type HeartbeatManager struct {
	db           qdb.IDatabase
	isLeader     bool
	ticker       *time.Ticker
	loopInterval time.Duration
}

func NewServiceManager(db qdb.IDatabase) *HeartbeatManager {
	return &HeartbeatManager{
		db:           db,
		loopInterval: 5 * time.Second,
	}
}

func (w *HeartbeatManager) SetLoopInterval(d time.Duration) {
	w.loopInterval = d
}

func (w *HeartbeatManager) OnBecameLeader() {
	w.isLeader = true
}

func (w *HeartbeatManager) OnLostLeadership() {
	w.isLeader = false
}

func (w *HeartbeatManager) Init() {
	w.ticker = time.NewTicker(w.loopInterval)
}

func (w *HeartbeatManager) Deinit() {
	w.ticker.Stop()
}

func (w *HeartbeatManager) ManageHeartbeats() {
	services := qdb.NewEntityFinder(w.db).Find(qdb.SearchCriteria{
		EntityType: "Service",
	})

	for _, service := range services {
		if service.GetField("HeartbeatTrigger").PullWriteTime().Add(qdb.LeaderLeaseTimeout).Before(time.Now()) {
			service.GetField("Leader").PushString("", qdb.PushIfNotEqual)
			service.GetField("Candidates").PushString("", qdb.PushIfNotEqual)
		}
	}
}

func (w *HeartbeatManager) DoWork() {
	if !w.isLeader {
		return
	}

	select {
	case <-w.ticker.C:
		w.ManageHeartbeats()
	default:
		// Do nothing
	}
}
