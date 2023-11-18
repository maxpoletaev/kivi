// Code generated by MockGen. DO NOT EDIT.
// Source: github.com/maxpoletaev/kivi/membership (interfaces: Cluster)

// Package mock is a generated GoMock package.
package mock

import (
	context "context"
	reflect "reflect"

	membership "github.com/maxpoletaev/kivi/membership"
	nodeapi "github.com/maxpoletaev/kivi/noderpc"
	gomock "go.uber.org/mock/gomock"
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

// ApplyState mocks base method.
func (m *MockCluster) ApplyState(arg0 []membership.Node, arg1 membership.NodeID) []membership.Node {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ApplyState", arg0, arg1)
	ret0, _ := ret[0].([]membership.Node)
	return ret0
}

// ApplyState indicates an expected call of ApplyState.
func (mr *MockClusterMockRecorder) ApplyState(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ApplyState", reflect.TypeOf((*MockCluster)(nil).ApplyState), arg0, arg1)
}

// Conn mocks base method.
func (m *MockCluster) Conn(arg0 membership.NodeID) (nodeapi.Client, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Conn", arg0)
	ret0, _ := ret[0].(nodeapi.Client)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// Conn indicates an expected call of Conn.
func (mr *MockClusterMockRecorder) Conn(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Conn", reflect.TypeOf((*MockCluster)(nil).Conn), arg0)
}

// ConnContext mocks base method.
func (m *MockCluster) ConnContext(arg0 context.Context, arg1 membership.NodeID) (nodeapi.Client, error) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "ConnContext", arg0, arg1)
	ret0, _ := ret[0].(nodeapi.Client)
	ret1, _ := ret[1].(error)
	return ret0, ret1
}

// ConnContext indicates an expected call of ConnContext.
func (mr *MockClusterMockRecorder) ConnContext(arg0, arg1 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "ConnContext", reflect.TypeOf((*MockCluster)(nil).ConnContext), arg0, arg1)
}

// LocalConn mocks base method.
func (m *MockCluster) LocalConn() nodeapi.Client {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "LocalConn")
	ret0, _ := ret[0].(nodeapi.Client)
	return ret0
}

// LocalConn indicates an expected call of LocalConn.
func (mr *MockClusterMockRecorder) LocalConn() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "LocalConn", reflect.TypeOf((*MockCluster)(nil).LocalConn))
}

// Node mocks base method.
func (m *MockCluster) Node(arg0 membership.NodeID) (membership.Node, bool) {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Node", arg0)
	ret0, _ := ret[0].(membership.Node)
	ret1, _ := ret[1].(bool)
	return ret0, ret1
}

// Node indicates an expected call of Node.
func (mr *MockClusterMockRecorder) Node(arg0 interface{}) *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Node", reflect.TypeOf((*MockCluster)(nil).Node), arg0)
}

// Nodes mocks base method.
func (m *MockCluster) Nodes() []membership.Node {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Nodes")
	ret0, _ := ret[0].([]membership.Node)
	return ret0
}

// Nodes indicates an expected call of Nodes.
func (mr *MockClusterMockRecorder) Nodes() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Nodes", reflect.TypeOf((*MockCluster)(nil).Nodes))
}

// Self mocks base method.
func (m *MockCluster) Self() membership.Node {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "Self")
	ret0, _ := ret[0].(membership.Node)
	return ret0
}

// Self indicates an expected call of Self.
func (mr *MockClusterMockRecorder) Self() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "Self", reflect.TypeOf((*MockCluster)(nil).Self))
}

// SelfID mocks base method.
func (m *MockCluster) SelfID() membership.NodeID {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "SelfID")
	ret0, _ := ret[0].(membership.NodeID)
	return ret0
}

// SelfID indicates an expected call of SelfID.
func (mr *MockClusterMockRecorder) SelfID() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "SelfID", reflect.TypeOf((*MockCluster)(nil).SelfID))
}

// StateHash mocks base method.
func (m *MockCluster) StateHash() uint64 {
	m.ctrl.T.Helper()
	ret := m.ctrl.Call(m, "StateHash")
	ret0, _ := ret[0].(uint64)
	return ret0
}

// StateHash indicates an expected call of StateHash.
func (mr *MockClusterMockRecorder) StateHash() *gomock.Call {
	mr.mock.ctrl.T.Helper()
	return mr.mock.ctrl.RecordCallWithMethodType(mr.mock, "StateHash", reflect.TypeOf((*MockCluster)(nil).StateHash))
}
