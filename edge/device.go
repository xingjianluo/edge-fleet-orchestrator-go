package edge

import (
	"sync"
	"time"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/fleet"
)

// Device 模拟设备端代理：校验并应用部署、产生单调递增的遥测位点。
type Device struct {
	mu       sync.Mutex
	id       string
	caps     fleet.Capabilities
	deployed []string // 已应用版本（应用顺序）
	seq      uint64   // 遥测位点
}

// NewDevice 创建设备端代理。
func NewDevice(id string, caps fleet.Capabilities) *Device {
	return &Device{id: id, caps: caps}
}

// Apply 应用一次部署：校验版本与能力兼容性；幂等（同版本重复应用不重复记录）。
func (d *Device) Apply(spec fleet.DeploymentSpec) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	a := spec.Artifact
	if a.Arch != d.caps.Arch || a.OS != d.caps.OS {
		return fleet.NewIncompatible("device %q (%s/%s) cannot run artifact %s/%s",
			d.id, d.caps.OS, d.caps.Arch, a.OS, a.Arch)
	}
	if d.caps.MaxArtifact > 0 && a.SizeBytes > d.caps.MaxArtifact {
		return fleet.NewIncompatible("device %q artifact size %d exceeds limit %d", d.id, a.SizeBytes, d.caps.MaxArtifact)
	}
	for _, f := range a.RequireFeature {
		if !d.caps.Supports(f) {
			return fleet.NewIncompatible("device %q lacks required feature %q", d.id, f)
		}
	}
	if n := len(d.deployed); n > 0 && d.deployed[n-1] == a.Version {
		return nil // 幂等：当前已是该版本
	}
	d.deployed = append(d.deployed, a.Version)
	return nil
}

// Deployed 返回设备当前版本。
func (d *Device) Deployed() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	if len(d.deployed) == 0 {
		return ""
	}
	return d.deployed[len(d.deployed)-1]
}

// Emit 产生一条遥测（位点单调递增）。
func (d *Device) Emit(key, value string) fleet.Telemetry {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.seq++
	return fleet.Telemetry{
		DeviceID: d.id,
		Seq:      d.seq,
		Key:      key,
		Value:    value,
		At:       time.Now().UTC(),
	}
}
