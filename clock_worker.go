package main

import (
	"time"

	qdb "github.com/rqure/qdb/src"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ClockWorker struct {
	db       qdb.IDatabase
	isLeader bool

	lastUpdateTime  time.Time
	updateFrequency time.Duration
}

func NewClockWorker(db qdb.IDatabase, updateFrequency time.Duration) *ClockWorker {
	return &ClockWorker{
		db:              db,
		updateFrequency: updateFrequency,
	}
}

func (w *ClockWorker) OnBecameLeader() {
	w.isLeader = true
}

func (w *ClockWorker) OnLostLeadership() {
	w.isLeader = false
}

func (w *ClockWorker) Init() {

}

func (w *ClockWorker) Deinit() {

}

func (w *ClockWorker) DoWork() {
	if !w.isLeader {
		return
	}

	if time.Since(w.lastUpdateTime) < w.updateFrequency {
		return
	}

	w.lastUpdateTime = time.Now()
	clocks := qdb.NewEntityFinder(w.db).Find(qdb.SearchCriteria{
		EntityType: "SystemClock",
	})

	for _, clock := range clocks {
		clock.GetField("CurrentTime").PushValue(&qdb.Timestamp{Raw: timestamppb.Now()})
	}
}
