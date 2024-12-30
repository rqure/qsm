package main

import (
	"context"
	"time"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/binding"
	"github.com/rqure/qlib/pkg/data/query"
	"github.com/rqure/qlib/pkg/leadership/candidate"
)

type HeartbeatManager struct {
	store        data.Store
	isLeader     bool
	ticker       *time.Ticker
	loopInterval time.Duration
}

func NewServiceManager(store data.Store) *HeartbeatManager {
	return &HeartbeatManager{
		store:        store,
		loopInterval: 5 * time.Second,
	}
}

func (w *HeartbeatManager) SetLoopInterval(d time.Duration) {
	w.loopInterval = d
}

func (w *HeartbeatManager) OnBecameLeader(context.Context) {
	w.isLeader = true
}

func (w *HeartbeatManager) OnLostLeadership(context.Context) {
	w.isLeader = false
}

func (w *HeartbeatManager) Init(context.Context, app.Handle) {
	w.ticker = time.NewTicker(w.loopInterval)
}

func (w *HeartbeatManager) Deinit(context.Context) {
	w.ticker.Stop()
}

func (w *HeartbeatManager) ManageHeartbeats(ctx context.Context) {
	multi := binding.NewMulti(w.store)
	services := EntityBindingArray(
		query.New(multi).
			Select("HeartbeatTrigger").
			From("Service").
			Execute(ctx),
	).AsMap()

	for _, service := range services {
		if service.GetField("HeartbeatTrigger").GetWriteTime().Add(candidate.LeaseTimeout).Before(time.Now()) {
			service.GetField("Leader").WriteString(ctx, "", data.WriteChanges)
			service.GetField("Candidates").WriteString(ctx, "", data.WriteChanges)
		}
	}

	multi.Commit(ctx)
}

func (w *HeartbeatManager) DoWork(ctx context.Context) {
	if !w.isLeader {
		return
	}

	select {
	case <-w.ticker.C:
		w.ManageHeartbeats(ctx)
	default:
		// Do nothing
	}
}
