package main

import (
	"context"
	"encoding/json"
	"io"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"

	qdb "github.com/rqure/qdb/src"
)

type ContainerManager struct {
	db           qdb.IDatabase
	isLeader     bool
	ticker       *time.Ticker
	loopInterval time.Duration
}

func NewContainerManager(db qdb.IDatabase) *ContainerManager {
	return &ContainerManager{
		db:           db,
		loopInterval: 5 * time.Second,
	}
}

func (w *ContainerManager) SetLoopInterval(d time.Duration) {
	w.loopInterval = d
}

func (w *ContainerManager) OnBecameLeader() {
	w.isLeader = true
}

func (w *ContainerManager) OnLostLeadership() {
	w.isLeader = false
}

func (w *ContainerManager) Init() {
	w.ticker = time.NewTicker(w.loopInterval)
}

func (w *ContainerManager) Deinit() {
	w.ticker.Stop()
}

func (w *ContainerManager) ProcessContainerStats() {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		qdb.Error("[ContainerManager::ProcessContainerStats] Failed to create docker client: %v", err)
		return
	}

	containers, err := cli.ContainerList(context.Background(), container.ListOptions{})
	if err != nil {
		qdb.Error("[ContainerManager::ProcessContainerStats] Failed to list containers: %v", err)
		return
	}

	for _, c := range containers {
		for _, n := range c.Names {
			entities := qdb.NewEntityFinder(w.db).Find(qdb.SearchCriteria{
				EntityType: "Container",
				Conditions: []qdb.FieldConditionEval{
					qdb.NewStringCondition().Where("Identifier").IsEqualTo(&qdb.String{Raw: n}),
				},
			})

			for _, entity := range entities {
				entity.GetField("Image").PushString(c.Image, qdb.PushIfNotEqual)
				entity.GetField("State").PushString(c.State, qdb.PushIfNotEqual)
				entity.GetField("Status").PushString(c.Status, qdb.PushIfNotEqual)
				entity.GetField("CreateTime").PushTimestamp(time.Unix(c.Created, 0), qdb.PushIfNotEqual)

				func() {
					stats, err := cli.ContainerStats(context.Background(), c.ID, false)
					if err != nil {
						qdb.Error("[ContainerManager::ProcessContainerStats] Failed to get container stats: %v", err)
						return
					}
					defer stats.Body.Close()

					var stat container.StatsResponse
					decoder := json.NewDecoder(stats.Body)
					for {
						if err := decoder.Decode(&stat); err != nil {
							if err == io.EOF {
								break
							}

							qdb.Error("[ContainerManager::ProcessContainerStats] Failed to decode container stats: %v", err)
							return
						}

						cpuUsage := ((stat.CPUStats.CPUUsage.TotalUsage / stat.CPUStats.SystemUsage) * uint64(stat.CPUStats.OnlineCPUs)) * 100
						entity.GetField("CPUUsage").PushInt(int64(cpuUsage), qdb.PushIfNotEqual)

						// fmt.Printf("Memory Usage: %v / %v\n", stat.MemoryStats.Usage, stat.MemoryStats.Limit)
						memoryUsage := stat.MemoryStats.Usage / (1024 * 1024)
						entity.GetField("MemoryUsage").PushInt(int64(memoryUsage), qdb.PushIfNotEqual)
					}
				}()
			}
		}
	}
}

func (w *ContainerManager) DoWork() {
	if !w.isLeader {
		return
	}

	select {
	case <-w.ticker.C:
		w.ProcessContainerStats()
	default:
		// Do nothing
	}
}
