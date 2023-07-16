package membership

import (
	"time"

	"github.com/go-kit/log/level"
)

func (cl *SWIMCluster) startGC() {
	cl.wg.Add(1)

	go func() {
		defer cl.wg.Done()

		ticker := time.NewTicker(cl.gcInterval)
		defer ticker.Stop()

		for {
			select {
			case <-ticker.C:
				cl.collectConnections()
			case <-cl.stop:
				return
			}
		}
	}()
}

func (cl *SWIMCluster) collectConnections() {
	cl.mut.Lock()
	defer cl.mut.Unlock()

	for id, conn := range cl.connections {
		node, ok := cl.nodes[id]

		if !ok || node.Status != StatusLeft {
			if err := conn.Close(); err != nil {
				level.Warn(cl.logger).Log("msg", "failed to close connection", "node", id, "err", err)
			}
		}

		// Remove all closed connections. They may have been closed manually.
		if conn.IsClosed() {
			delete(cl.connections, id)
		}
	}
}
