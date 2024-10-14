package main

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"

	qdb "github.com/rqure/qdb/src"
)

type ContainerManager struct {
	db                 qdb.IDatabase
	isLeader           bool
	ticker             *time.Ticker
	loopInterval       time.Duration
	notificationTokens []qdb.INotificationToken
}

func NewContainerManager(db qdb.IDatabase) *ContainerManager {
	return &ContainerManager{
		db:                 db,
		loopInterval:       5 * time.Second,
		notificationTokens: []qdb.INotificationToken{},
	}
}

func (w *ContainerManager) SetLoopInterval(d time.Duration) {
	w.loopInterval = d
}

func (w *ContainerManager) OnBecameLeader() {
	w.isLeader = true

	w.notificationTokens = append(w.notificationTokens, w.db.Notify(&qdb.DatabaseNotificationConfig{
		Type:  "Container",
		Field: "ResetTrigger",
		ContextFields: []string{
			"ContainerName",
			"ContainerId",
		},
	}, qdb.NewNotificationCallback(w.ProcessNotification)))
}

func (w *ContainerManager) OnLostLeadership() {
	w.isLeader = false

	for _, token := range w.notificationTokens {
		token.Unbind()
	}

	w.notificationTokens = []qdb.INotificationToken{}
}

func (w *ContainerManager) ProcessNotification(notification *qdb.DatabaseNotification) {
	if !w.isLeader {
		return
	}

	qdb.Debug("[ContainerManager::ProcessNotification] Received notification: %v", notification)

	switch notification.Current.Name {
	case "ResetTrigger":
		w.onResetTrigger(notification)
	}
}

func (w *ContainerManager) onResetTrigger(notification *qdb.DatabaseNotification) {
	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		qdb.Error("[ContainerManager::ProcessNotification] Failed to create docker client: %v", err)
		return
	}
	defer cli.Close()

	containerName := qdb.ValueCast[*qdb.String](notification.Context[0].Value).Raw
	containerId := qdb.ValueCast[*qdb.String](notification.Context[1].Value).Raw

	go func() {
		err = cli.ContainerRestart(context.Background(), containerId, container.StopOptions{})
		if err != nil {
			qdb.Error("[ContainerManager::ProcessNotification] Failed to restart container %s: %v", containerName, err)
		} else {
			qdb.Info("[ContainerManager::ProcessNotification] Container restarted: %v", containerName)
		}
	}()
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
					qdb.NewStringCondition().Where("ContainerName").IsEqualTo(&qdb.String{Raw: n}),
				},
			})

			for _, entity := range entities {
				entity.GetField("ContainerId").PushString(c.ID, qdb.PushIfNotEqual)
				entity.GetField("ContainerImage").PushString(c.Image, qdb.PushIfNotEqual)
				if entity.GetField("ContainerState").PushString(c.State, qdb.PushIfNotEqual) && c.State == "running" {
					entity.GetField("StartTime").PushTimestamp(time.Now(), qdb.PushIfNotEqual)

					for _, restartableContainerId := range strings.Split(entity.GetField("RestartContainers").PullString(), ",") {
						if restartableContainerId == "" || w.db.GetEntity(restartableContainerId) == nil {
							continue
						}

						restartableContainerEntity := qdb.NewEntity(w.db, restartableContainerId)
						restartableContainerEntity.GetField("ResetTrigger").PushInt()
					}
				}
				entity.GetField("ContainerStatus").PushString(c.Status, qdb.PushIfNotEqual)
				entity.GetField("CreateTime").PushTimestamp(c.Created, qdb.PushIfNotEqual)

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

						memoryUsage := stat.MemoryStats.Usage / (1024 * 1024)
						entity.GetField("MemoryUsage").PushInt(int64(memoryUsage), qdb.PushIfNotEqual)
					}
				}()
			}
		}
	}

	entities := qdb.NewEntityFinder(w.db).Find(qdb.SearchCriteria{
		EntityType: "Container",
	})

	for _, entity := range entities {
		isLeader := entity.GetField("ServiceReference->Leader").PullString() == entity.GetField("ContainerName").PullString()
		isAvailable := strings.Contains(entity.GetField("ServiceReference->Candidates").PullString(), entity.GetField("ContainerName").GetString())
		entity.GetField("IsLeader").PushBool(isLeader, qdb.PushIfNotEqual)
		entity.GetField("IsAvailable").PushBool(isAvailable, qdb.PushIfNotEqual)
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
