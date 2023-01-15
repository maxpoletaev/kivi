// Code generated by MockGen. DO NOT EDIT.
// Source: facilities.go

// Package replication is a generated GoMock package.
package replication

import (
	reflect "reflect"

	gomock "github.com/golang/mock/gomock"
	membership "github.com/maxpoletaev/kiwi/membership"
	nodeclient "github.com/maxpoletaev/kiwi/nodeclient"
)

// MockConnRegistry is a mock of ConnRegistry interface.
type MockConnRegistry struct {
	ctrl     *gomock.Controller
	recorder *MockConnRegistryMockRecorder
}

// MockConnRegistryMockRecorder is the mock recorder for MockConnRegistry.
type MockConnRegistryMockRecorder struct {
	mock *MockConnRegistry
}

// NewMockConnRegistry creates a new mock instance.
func NewMockConnRegistry(ctrl *gomock.Controller) *MockConnRegistry {
	mock := &MockConnRegistry{ctrl: ctrl}
	mock.recorder = &MockConnRegistryMockRecorder{mock}
	return mock
}

// EXPECT returns an object that allows the caller to indicate expected use.
func (m *MockConnRegistry) EXPECT() *MockConnRegistryMockRecorder {
	return m.recorder
}

// Get mocks base method.
func (m *MockConnRegistry) Get(nodeID membership.NodeID) (nodeclient.Conn, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Get", nodeID)
	ret0, _ := ret[0].(nodeclient.Conn)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Get indicates an expected call of Get.
func (mr *MockConnRegistryMockRecorder) Get(nodeID interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Get", reflect.TypeOf((*MockConnRegistry)(nil).Get), nodeID)
}

// Local mocks base method.
func (m *MockConnRegistry) Local() nodeclient.Conn {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Local")
	ret0, _ := ret[0].(nodeclient.Conn)
	return ret0
}

// Local indicates an expected call of Local.
func (mr *MockConnRegistryMockRecorder) Local() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Local", reflect.TypeOf((*MockConnRegistry)(nil).Local))
}