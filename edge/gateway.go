package edge

import (
	"sync"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/controlplane"
	"github.com/xingjianluo/edge-fleet-orchestrator-go/fleet"
)

// Gateway 模拟边缘网关：断网时本地缓冲，重连后按位点续传。
type Gateway struct {
	mu        sync.Mutex
	tenantID  string
	clusterID string // 网关所属集群；为空表示未显式绑定（单集群回退）
	source    string // 通道/网关标识，随上报进入冲突报告以便解释来源
	cp        *controlplane.ControlPlane
	store     *Store
	online    bool
}

// NewGateway 创建绑定到控制面与本地存储的网关（不绑定具体集群）。
func NewGateway(tenantID string, cp *controlplane.ControlPlane, store *Store) *Gateway {
	return &Gateway{tenantID: tenantID, cp: cp, store: store, online: true}
}

// NewClusterGateway 创建绑定到指定租户/集群/上行通道的网关。
// 多路网关可各自创建实例；source 仅用于冲突可解释，不参与收敛判定。
func NewClusterGateway(tenantID, clusterID, source string, cp *controlplane.ControlPlane, store *Store) *Gateway {
	if source == "" {
		source = "gw:" + clusterID
	}
	return &Gateway{tenantID: tenantID, clusterID: clusterID, source: source, cp: cp, store: store, online: true}
}

// SetOnline 切换网关在线状态（模拟断网/重连）。
func (g *Gateway) SetOnline(online bool) {
	g.mu.Lock()
	g.online = online
	g.mu.Unlock()
}

// stamp 补齐集群与通道信息（收敛只看内容，来源仅用于冲突回报）。
func (g *Gateway) stamp(t fleet.Telemetry) fleet.Telemetry {
	g.mu.Lock()
	if t.ClusterID == "" {
		t.ClusterID = g.clusterID
	}
	if t.Source == "" {
		t.Source = g.source
	}
	g.mu.Unlock()
	return t
}

// Report 接收设备遥测：始终先写入本地缓冲（保证断网不丢）；
// 在线时立即尝试续传。
func (g *Gateway) Report(t fleet.Telemetry) error {
	t = g.stamp(t)
	if err := g.store.Append(t); err != nil {
		return err
	}
	g.mu.Lock()
	online := g.online
	g.mu.Unlock()
	if !online {
		return nil // 断网：已缓冲，等待重连续传
	}
	_, err := g.Sync(t.DeviceID)
	return err
}

// Sync 重连后按位点续传缓冲数据，返回最新确认位点。
// 断网时为空操作，返回当前已确认位点。
func (g *Gateway) Sync(deviceID string) (uint64, error) {
	g.mu.Lock()
	online := g.online
	g.mu.Unlock()
	acked := g.store.Checkpoint(deviceID)
	if !online {
		return acked, nil
	}
	batch := g.store.Pending(deviceID, acked)
	if len(batch) == 0 {
		return acked, nil
	}
	cp, err := g.cp.Ingest(g.tenantID, batch)
	if err != nil {
		return acked, err
	}
	if cp.AckedSeq > acked {
		if err := g.store.Ack(deviceID, cp.AckedSeq); err != nil {
			return acked, err
		}
		acked = cp.AckedSeq
	}
	return acked, nil
}
