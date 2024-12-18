package main

import (
	"context"
	"encoding/json"
	"io"
	"strings"
	"time"

	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/client"

	"github.com/rqure/qlib/pkg/app"
	"github.com/rqure/qlib/pkg/data"
	"github.com/rqure/qlib/pkg/data/binding"
	"github.com/rqure/qlib/pkg/data/notification"
	"github.com/rqure/qlib/pkg/data/query"
	"github.com/rqure/qlib/pkg/log"
)

type EntityBindingArray []data.EntityBinding

func (a EntityBindingArray) AsMap() map[string]data.EntityBinding {
	m := make(map[string]data.EntityBinding)
	for _, b := range a {
		m[b.GetId()] = b
	}
	return m
}

type ContainerManager struct {
	store              data.Store
	isLeader           bool
	ticker             *time.Ticker
	loopInterval       time.Duration
	notificationTokens []data.NotificationToken
	containerStatsCh   chan map[string]map[string]interface{}
}

func NewContainerManager(store data.Store) *ContainerManager {
	return &ContainerManager{
		store:              store,
		loopInterval:       15 * time.Second,
		notificationTokens: []data.NotificationToken{},
		containerStatsCh:   make(chan map[string]map[string]interface{}, 10),
	}
}

func (w *ContainerManager) SetLoopInterval(d time.Duration) {
	w.loopInterval = d
}

func (w *ContainerManager) OnBecameLeader(ctx context.Context) {
	w.isLeader = true

	w.notificationTokens = append(w.notificationTokens, w.store.Notify(
		ctx,
		notification.NewConfig().
			SetEntityType("Container").
			SetFieldName("ResetTrigger").
			SetContextFields([]string{"ContainerName", "ContainerId"}),
		notification.NewCallback(w.ProcessNotification)))
}

func (w *ContainerManager) OnLostLeadership(ctx context.Context) {
	w.isLeader = false

	for _, token := range w.notificationTokens {
		token.Unbind(ctx)
	}

	w.notificationTokens = []data.NotificationToken{}
}

func (w *ContainerManager) ProcessNotification(ctx context.Context, n data.Notification) {
	if !w.isLeader {
		return
	}

	log.Debug("Received notification: %v", n)

	switch n.GetCurrent().GetFieldName() {
	case "ResetTrigger":
		w.onResetTrigger(ctx, n)
	}
}

func (w *ContainerManager) onResetTrigger(ctx context.Context, n data.Notification) {
	containerName := n.GetContext(0).GetValue().GetString()
	containerId := n.GetContext(1).GetValue().GetString()

	go func() {
		cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
		if err != nil {
			log.Error("Failed to create docker client: %v", err)
			return
		}
		defer cli.Close()

		<-time.After(1 * time.Minute)

		err = cli.ContainerRestart(context.Background(), containerId, container.StopOptions{})
		if err != nil {
			log.Error("Failed to restart container %s: %v", containerName, err)
		} else {
			log.Info("Container restarted: %v", containerName)
		}
	}()
}

func (w *ContainerManager) Init(context.Context, app.Handle) {
	w.ticker = time.NewTicker(w.loopInterval)
}

func (w *ContainerManager) Deinit(context.Context) {
	w.ticker.Stop()
}

func (w *ContainerManager) UpdateContainerStats(ctx context.Context, statsByContainerName map[string]map[string]interface{}) {
	multi := binding.NewMulti(w.store)

	entities := EntityBindingArray(query.New(multi).
		ForType("Container").
		Execute(ctx)).AsMap()

	for containerName, stat := range statsByContainerName {
		if entities[containerName] == nil {
			continue
		}

		for _, entity := range entities {
			entity.GetField("ContainerId").WriteString(ctx, stat["ContainerId"], data.WriteChanges)
			entity.GetField("ContainerImage").WriteString(ctx, stat["ContainerImage"], data.WriteChanges)
			entity.GetField("ContainerState").WriteString(ctx, stat["ContainerState"], data.WriteChanges)
			entity.GetField("StartTime").WriteTimestamp(ctx, stat["StartTime"], data.WriteChanges)
			entity.GetField("ContainerStatus").WriteString(ctx, stat["ContainerStatus"], data.WriteChanges)
			entity.GetField("CreateTime").WriteTimestamp(ctx, stat["CreateTime"], data.WriteChanges)
			entity.GetField("CPUUsage").WriteFloat(ctx, stat["CPUUsage"], data.WriteChanges)
			entity.GetField("MemoryUsage").WriteFloat(ctx, stat["MemoryUsage"], data.WriteChanges)
			entity.GetField("MACAddress").WriteString(ctx, stat["MACAddress"], data.WriteChanges)
			entity.GetField("IPAddress").WriteString(ctx, stat["IPAddress"], data.WriteChanges)
		}
	}

	multi.Commit(ctx)
}

func (w *ContainerManager) FindContainerStats() {
	statsByContainerName := make(map[string]map[string]interface{})

	cli, err := client.NewClientWithOpts(client.FromEnv, client.WithAPIVersionNegotiation())
	if err != nil {
		log.Error("Failed to create docker client: %v", err)
		return
	}
	defer cli.Close()

	containers, err := cli.ContainerList(context.Background(), container.ListOptions{})
	if err != nil {
		log.Error("Failed to list containers: %v", err)
		return
	}

	for _, c := range containers {
		inspect, err := cli.ContainerInspect(context.Background(), c.ID)
		if err != nil {
			log.Error("Failed to inspect container %s: %v", c.ID, err)
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
				log.Error("Failed to get container stats: %v", err)
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

					log.Error("Failed to decode container stats: %v", err)
					return
				}

				statsByContainerName[inspect.Name]["CPUUsage"] = ((float64(stat.CPUStats.CPUUsage.TotalUsage) / float64(stat.CPUStats.SystemUsage)) * float64(stat.CPUStats.OnlineCPUs)) * 100.0
				statsByContainerName[inspect.Name]["MemoryUsage"] = float64(stat.MemoryStats.Usage) / float64(1024*1024)
			}
		}()
	}

	w.containerStatsCh <- statsByContainerName
}

func (w *ContainerManager) UpdateContainerAvailability(ctx context.Context) {
	entities := query.New(w.store).ForType("Container").Execute(ctx)

	ipAddresses := make(map[string]string)
	macAddresses := make(map[string]string)

	for _, entity := range entities {
		containerNameField := entity.GetField("ContainerName")
		ipAddress := entity.GetField("IPAddress").ReadString(ctx)
		macAddress := entity.GetField("MACAddress").ReadString(ctx)

		if _, ok := ipAddresses[ipAddress]; !ok {
			ipAddresses[ipAddress] = containerNameField.ReadString(ctx)
		} else {
			log.Warn("Duplicate IP address '%s' found for containers '%s' and '%s'", ipAddress, ipAddresses[ipAddress], containerNameField.GetString())
		}

		if _, ok := macAddresses[macAddress]; !ok {
			macAddresses[macAddress] = containerNameField.GetString()
		} else {
			log.Warn("Duplicate MAC address '%s' found for containers '%s' and '%s'", macAddress, macAddresses[macAddress], containerNameField.GetString())
		}

		isAvailable := false
		isLeader := false

		containerIdField := entity.GetField("ContainerId")
		if entity.GetField("ServiceReference").ReadString(ctx) != "" {
			isLeader = strings.Contains(containerIdField.ReadString(ctx), entity.GetField("ServiceReference->Leader").ReadString(ctx))
			for _, candidate := range strings.Split(entity.GetField("ServiceReference->Candidates").ReadString(ctx), ",") {
				if candidate == "" {
					continue
				}

				if strings.Contains(containerIdField.GetString(), candidate) {
					isAvailable = true
					break
				}
			}
		}

		entity.GetField("IsLeader").WriteBool(ctx, isLeader, data.WriteChanges)
		entity.GetField("IsAvailable").WriteBool(ctx, isAvailable, data.WriteChanges)
	}
}

func (w *ContainerManager) DoWork(ctx context.Context) {
	if !w.isLeader {
		return
	}

	select {
	case <-w.ticker.C:
		go w.FindContainerStats()

		w.UpdateContainerAvailability(ctx)
	case statByContainerName := <-w.containerStatsCh:
		w.UpdateContainerStats(ctx, statByContainerName)
	default:
		// Do nothing
	}
}
