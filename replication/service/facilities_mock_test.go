// Code generated by MockGen. DO NOT EDIT.
// Source: facilities.go

// Package service is a generated GoMock package.
package service

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	cluster "github.com/maxpoletaev/kv/cluster"
	membership "github.com/maxpoletaev/kv/membership"
)

// MockCluster is a mock of Cluster interface.
type MockCluster struct {
	ctrl     *gomock.Controller
	recorder *MockClusterMockRecorder
}

// MockClusterMockRecorder is the mock recorder for MockCluster.
type MockClusterMockRecorder struct {
	mock *MockCluster
}

// NewMockCluster creates a new mock instance.
func NewMockCluster(ctrl *gomock.Controller) *MockCluster {
	mock := &MockCluster{ctrl: ctrl}
	mock.recorder = &MockClusterMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockCluster) EXPECT() *MockClusterMockRecorder {
	return m.recorder
}

// Conn mocks base method.
func (m *MockCluster) Conn(arg0 membership.NodeID) (cluster.Client, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Conn", arg0)
	ret0, _ := ret[0].(cluster.Client)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Conn indicates an expected call of Conn.
func (mr *MockClusterMockRecorder) Conn(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Conn", reflect.TypeOf((*MockCluster)(nil).Conn), arg0)
}

// Members mocks base method.
func (m *MockCluster) Members() []membership.Member {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Members")
	ret0, _ := ret[0].([]membership.Member)
	return ret0
}

// Members indicates an expected call of Members.
func (mr *MockClusterMockRecorder) Members() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Members", reflect.TypeOf((*MockCluster)(nil).Members))
}

// Self mocks base method.
func (m *MockCluster) Self() membership.Member {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Self")
	ret0, _ := ret[0].(membership.Member)
	return ret0
}

// Self indicates an expected call of Self.
func (mr *MockClusterMockRecorder) Self() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Self", reflect.TypeOf((*MockCluster)(nil).Self))
}

// SelfConn mocks base method.
func (m *MockCluster) SelfConn() cluster.Client {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SelfConn")
	ret0, _ := ret[0].(cluster.Client)
	return ret0
}

// SelfConn indicates an expected call of SelfConn.
func (mr *MockClusterMockRecorder) SelfConn() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SelfConn", reflect.TypeOf((*MockCluster)(nil).SelfConn))
}
