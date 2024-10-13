package main

import (
	qdb "github.com/rqure/qdb/src"
)

type ServiceManager struct {
	db       qdb.IDatabase
	isLeader bool
}

func NewServiceManager(db qdb.IDatabase) *ServiceManager {
	return &ServiceManager{
		db: db,
	}
}

func (w *ServiceManager) OnBecameLeader() {
	w.isLeader = true
}

func (w *ServiceManager) OnLostLeadership() {
	w.isLeader = false
}

func (w *ServiceManager) Init() {

}

func (w *ServiceManager) Deinit() {

}

func (w *ServiceManager) DoWork() {

}
