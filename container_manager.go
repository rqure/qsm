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
	containerStatsCh   chan map[string]map[string]interface{}
}

func NewContainerManager(db qdb.IDatabase) *ContainerManager {
	return &ContainerManager{
		db:                 db,
		loopInterval:       15 * time.Second,
		notificationTokens: []qdb.INotificationToken{},
		containerStatsCh:   make(chan map[string]map[string]interface{}, 10),
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
	containerName := qdb.ValueCast[*qdb.String](notification.Context[0].Value).Raw
	containerId := qdb.ValueCast[*qdb.String](notification.Context[1].Value).Raw

	go func() {
		cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
		if err != nil {
			qdb.Error("[ContainerManager::ProcessNotification] Failed to create docker client: %v", err)
			return
		}
		defer cli.Close()

		<-time.After(1 * time.Minute)

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

func (w *ContainerManager) UpdateContainerStats(statsByContainerName map[string]map[string]interface{}) {
	for containerName, stat := range statsByContainerName {
		entities := qdb.NewEntityFinder(w.db).Find(qdb.SearchCriteria{
			EntityType: "Container",
			Conditions: []qdb.FieldConditionEval{
				qdb.NewStringCondition().Where("ContainerName").IsEqualTo(&qdb.String{Raw: containerName}),
			},
		})

		if len(entities) == 0 {
			qdb.Warn("[ContainerManager::ProcessContainerStats] Container '%s' not found in database", containerName)
		}

		for _, entity := range entities {
			entity.GetField("ContainerId").PushString(stat["ContainerId"], qdb.PushIfNotEqual)
			entity.GetField("ContainerImage").PushString(stat["ContainerImage"], qdb.PushIfNotEqual)
			entity.GetField("ContainerState").PushString(stat["ContainerState"], qdb.PushIfNotEqual)
			if entity.GetField("StartTime").PushTimestamp(stat["StartTime"], qdb.PushIfNotEqual) {
				for _, restartableContainerId := range strings.Split(entity.GetField("RestartContainers").PullString(), ",") {
					if restartableContainerId == "" || w.db.GetEntity(restartableContainerId) == nil {
						continue
					}

					restartableContainerEntity := qdb.NewEntity(w.db, restartableContainerId)
					restartableContainerEntity.GetField("ResetTrigger").PushInt()
				}
			}
			entity.GetField("ContainerStatus").PushString(stat["ContainerStatus"], qdb.PushIfNotEqual)
			entity.GetField("CreateTime").PushTimestamp(stat["CreateTime"], qdb.PushIfNotEqual)
			entity.GetField("CPUUsage").PushFloat(stat["CPUUsage"], qdb.PushIfNotEqual)
			entity.GetField("MemoryUsage").PushFloat(stat["MemoryUsage"], qdb.PushIfNotEqual)
			entity.GetField("MACAddress").PushString(stat["MACAddress"], qdb.PushIfNotEqual)
			entity.GetField("IPAddress").PushString(stat["IPAddress"], qdb.PushIfNotEqual)
		}
	}
}

func (w *ContainerManager) FindContainerStats() {
	statsByContainerName := make(map[string]map[string]interface{})

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		qdb.Error("[ContainerManager::ProcessContainerStats] Failed to create docker client: %v", err)
		return
	}
	defer cli.Close()

	containers, err := cli.ContainerList(context.Background(), container.ListOptions{})
	if err != nil {
		qdb.Error("[ContainerManager::ProcessContainerStats] Failed to list containers: %v", err)
		return
	}

	for _, c := range containers {
		inspect, err := cli.ContainerInspect(context.Background(), c.ID)
		if err != nil {
			qdb.Error("[ContainerManager::ProcessContainerStats] Failed to inspect container %s: %v", c.ID, err)
			continue
		}

		statsByContainerName[inspect.Name] = map[string]interface{}{
			"ContainerId":     c.ID,
			"ContainerImage":  c.Image,
			"ContainerState":  inspect.State.Status,
			"StartTime":       inspect.State.StartedAt,
			"ContainerStatus": c.Status,
			"CreateTime":      c.Created,
			"IPAddress":       inspect.NetworkSettings.IPAddress,
			"MACAddress":      inspect.NetworkSettings.MacAddress,
		}

		networkName := c.HostConfig.NetworkMode
		if network, ok := c.NetworkSettings.Networks[networkName]; ok {
			statsByContainerName[inspect.Name]["IPAddress"] = network.IPAddress
			statsByContainerName[inspect.Name]["MACAddress"] = network.MacAddress
		}

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

				statsByContainerName[inspect.Name]["CPUUsage"] = ((float64(stat.CPUStats.CPUUsage.TotalUsage) / float64(stat.CPUStats.SystemUsage)) * float64(stat.CPUStats.OnlineCPUs)) * 100.0
				statsByContainerName[inspect.Name]["MemoryUsage"] = float64(stat.MemoryStats.Usage) / float64(1024*1024)
			}
		}()
	}

	w.containerStatsCh <- statsByContainerName
}

func (w *ContainerManager) UpdateContainerAvailability() {
	entities := qdb.NewEntityFinder(w.db).Find(qdb.SearchCriteria{
		EntityType: "Container",
	})

	ipAddresses := make(map[string]string)
	macAddresses := make(map[string]string)

	for _, entity := range entities {
		containerNameField := entity.GetField("ContainerName")

		ipAddress := entity.GetField("IPAddress").PullString()
		macAddress := entity.GetField("MACAddress").PullString()

		if _, ok := ipAddresses[ipAddress]; !ok {
			ipAddresses[ipAddress] = containerNameField.PullString()
		} else {
			qdb.Warn("[ContainerManager::UpdateContainerAvailability] Duplicate IP address '%s' found for containers '%s' and '%s'", ipAddress, ipAddresses[ipAddress], containerNameField.GetString())
		}

		if _, ok := macAddresses[macAddress]; !ok {
			macAddresses[macAddress] = containerNameField.GetString()
		} else {
			qdb.Warn("[ContainerManager::UpdateContainerAvailability] Duplicate MAC address '%s' found for containers '%s' and '%s'", macAddress, macAddresses[macAddress], containerNameField.GetString())
		}

		isAvailable := false
		isLeader := false

		containerIdField := entity.GetField("ContainerId")
		if entity.GetField("ServiceReference").PullString() != "" {
			isLeader = strings.Contains(containerIdField.PullString(), entity.GetField("ServiceReference->Leader").PullString())
			for _, candidate := range strings.Split(entity.GetField("ServiceReference->Candidates").PullString(), ",") {
				if candidate == "" {
					continue
				}

				if strings.Contains(containerIdField.GetString(), candidate) {
					isAvailable = true
					break
				}
			}
		}

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
		go w.FindContainerStats()

		w.UpdateContainerAvailability()
	case statByContainerName := <-w.containerStatsCh:
		w.UpdateContainerStats(statByContainerName)
	default:
		// Do nothing
	}
}
