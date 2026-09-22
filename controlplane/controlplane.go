// Package controlplane 模拟云端控制面：多租户/多集群的部署下发、
// 幂等去重、配额与权限校验、回滚以及遥测汇聚。
package controlplane

import (
	"regexp"
	"sync"
	"time"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/fleet"
)

// Quota 是租户级配额。
type Quota struct {
	MaxDevices     int
	MaxDeployments int
}

// stateEntry 是某状态键的当前值及其来源位点（用于确定性冲突收敛）。
type stateEntry struct {
	value string
	seq   uint64
}

type deviceState struct {
	caps           fleet.Capabilities
	clusterID      string
	history        []fleet.DeploymentRecord
	currentVersion string
	idempotency    map[string]*fleet.DeploymentRecord
	receivedSeq    map[uint64]bool // 已接收的遥测位点（去重）
	ackedSeq       uint64          // 已确认的最大连续位点
	state          map[string]stateEntry
}

type tenantState struct {
	quota       Quota
	deployCount int
	devices     map[string]*deviceState // deviceID -> state
}

// ControlPlane 是云端控制面的内存实现，并发安全。
type ControlPlane struct {
	mu        sync.Mutex
	tenants   map[string]*tenantState
	deviceIdx map[string]string // deviceID -> tenantID（全局索引，用于区分跨租户与不存在）
	nextSeq   int64
}

// New 创建空控制面。
func New() *ControlPlane {
	return &ControlPlane{
		tenants:   make(map[string]*tenantState),
		deviceIdx: make(map[string]string),
	}
}

// SetQuota 设置租户配额（首次调用隐式创建租户）。
func (c *ControlPlane) SetQuota(tenantID string, q Quota) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.tenants[tenantID]
	if t == nil {
		t = &tenantState{devices: make(map[string]*deviceState)}
		c.tenants[tenantID] = t
	}
	t.quota = q
}

// RegisterDevice 注册设备到租户与集群。
func (c *ControlPlane) RegisterDevice(tenantID, clusterID, deviceID string, caps fleet.Capabilities) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.tenants[tenantID]
	if t == nil {
		t = &tenantState{devices: make(map[string]*deviceState)}
		c.tenants[tenantID] = t
	}
	if owner, ok := c.deviceIdx[deviceID]; ok && owner != tenantID {
		return fleet.NewForbidden("device %q belongs to tenant %q, not %q", deviceID, owner, tenantID)
	}
	if _, ok := t.devices[deviceID]; ok {
		return nil // 重复注册幂等
	}
	if t.quota.MaxDevices > 0 && len(t.devices) >= t.quota.MaxDevices {
		return fleet.NewQuotaExceeded("tenant %q device quota %d exceeded", tenantID, t.quota.MaxDevices)
	}
	t.devices[deviceID] = &deviceState{
		caps:        caps,
		clusterID:   clusterID,
		idempotency: make(map[string]*fleet.DeploymentRecord),
		receivedSeq: make(map[uint64]bool),
		state:       make(map[string]stateEntry),
	}
	c.deviceIdx[deviceID] = tenantID
	return nil
}

var versionRe = regexp.MustCompile(`^v\d+\.\d+\.\d+$`)

// lookup 在持锁状态下定位设备，并区分“跨租户”与“不存在”。
func (c *ControlPlane) lookup(tenantID, deviceID string) (*tenantState, *deviceState, error) {
	t := c.tenants[tenantID]
	if t == nil {
		if owner, ok := c.deviceIdx[deviceID]; ok && owner != tenantID {
			return nil, nil, fleet.NewForbidden("device %q belongs to tenant %q", deviceID, owner)
		}
		return nil, nil, fleet.NewNotFound("tenant %q not found", tenantID)
	}
	d := t.devices[deviceID]
	if d == nil {
		if owner, ok := c.deviceIdx[deviceID]; ok && owner != tenantID {
			return nil, nil, fleet.NewForbidden("device %q belongs to tenant %q", deviceID, owner)
		}
		return nil, nil, fleet.NewNotFound("device %q not found in tenant %q", deviceID, tenantID)
	}
	return t, d, nil
}

// Deploy 版本化部署下发；幂等、校验版本与能力、配额控制。
func (c *ControlPlane) Deploy(spec fleet.DeploymentSpec) (*fleet.DeploymentRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, d, err := c.lookup(spec.TenantID, spec.DeviceID)
	if err != nil {
		return nil, err
	}
	// 幂等：同幂等键直接返回首次结果，不产生新记录。
	if spec.IdempotencyKey != "" {
		if rec, ok := d.idempotency[spec.IdempotencyKey]; ok {
			cp := *rec
			return &cp, nil
		}
	}
	// 版本格式校验。
	if !versionRe.MatchString(spec.Artifact.Version) {
		return nil, fleet.NewInvalidVersion("version %q must match vMAJOR.MINOR.PATCH", spec.Artifact.Version)
	}
	// 能力兼容性校验。
	if err := checkCompatible(d.caps, spec.Artifact); err != nil {
		return nil, err
	}
	// 配额校验。
	if t.quota.MaxDeployments > 0 && t.deployCount >= t.quota.MaxDeployments {
		return nil, fleet.NewQuotaExceeded("tenant %q deployment quota %d exceeded", spec.TenantID, t.quota.MaxDeployments)
	}
	rec := c.appendRecord(t, d, spec, "")
	if spec.IdempotencyKey != "" {
		d.idempotency[spec.IdempotencyKey] = rec
	}
	cp := *rec
	return &cp, nil
}

func checkCompatible(caps fleet.Capabilities, a fleet.Artifact) error {
	if a.Arch != caps.Arch || a.OS != caps.OS {
		return fleet.NewIncompatible("artifact %s/%s incompatible with device %s/%s", a.OS, a.Arch, caps.OS, caps.Arch)
	}
	if caps.MaxArtifact > 0 && a.SizeBytes > caps.MaxArtifact {
		return fleet.NewIncompatible("artifact size %d exceeds device limit %d", a.SizeBytes, caps.MaxArtifact)
	}
	for _, f := range a.RequireFeature {
		if !caps.Supports(f) {
			return fleet.NewIncompatible("device lacks required feature %q", f)
		}
	}
	return nil
}

func (c *ControlPlane) appendRecord(t *tenantState, d *deviceState, spec fleet.DeploymentSpec, rollbackOf string) *fleet.DeploymentRecord {
	c.nextSeq++
	rec := fleet.DeploymentRecord{
		Seq:        c.nextSeq,
		Spec:       spec,
		RollbackOf: rollbackOf,
		AppliedAt:  time.Now().UTC(),
	}
	d.history = append(d.history, rec)
	d.currentVersion = spec.Artifact.Version
	t.deployCount++
	return &d.history[len(d.history)-1]
}

// Rollback 将设备可复现地恢复到指定历史版本：复用历史中记录的完整 Spec。
func (c *ControlPlane) Rollback(tenantID, deviceID, targetVersion string) (*fleet.DeploymentRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, d, err := c.lookup(tenantID, deviceID)
	if err != nil {
		return nil, err
	}
	var target *fleet.DeploymentRecord
	for i := range d.history {
		if d.history[i].Spec.Artifact.Version == targetVersion {
			target = &d.history[i]
		}
	}
	if target == nil {
		return nil, fleet.NewNotFound("device %q has no historical version %q", deviceID, targetVersion)
	}
	if t.quota.MaxDeployments > 0 && t.deployCount >= t.quota.MaxDeployments {
		return nil, fleet.NewQuotaExceeded("tenant %q deployment quota %d exceeded", tenantID, t.quota.MaxDeployments)
	}
	// 可复现：使用历史记录中的原始 Spec，仅替换幂等键避免与首次下发混淆。
	spec := target.Spec
	spec.IdempotencyKey = ""
	rec := c.appendRecord(t, d, spec, targetVersion)
	cp := *rec
	return &cp, nil
}

// Ingest 接收网关重连后续传的遥测；按位点去重、乱序容忍、冲突确定性收敛。
// 收敛规则：同一 (device,key) 位点大者胜；位点相同保留先到的值（不覆盖）。
func (c *ControlPlane) Ingest(tenantID string, batch []fleet.Telemetry) (fleet.Checkpoint, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var cp fleet.Checkpoint
	for _, rec := range batch {
		_, d, err := c.lookup(tenantID, rec.DeviceID)
		if err != nil {
			return cp, err
		}
		if !d.receivedSeq[rec.Seq] {
			d.receivedSeq[rec.Seq] = true
			cur, ok := d.state[rec.Key]
			if !ok || rec.Seq > cur.seq {
				d.state[rec.Key] = stateEntry{value: rec.Value, seq: rec.Seq}
			}
			// 位点较小或相等：按规则保留现有值（确定性收敛，非静默覆盖——可通过 State 验证）。
			// 推进连续确认位点。
			for d.receivedSeq[d.ackedSeq+1] {
				d.ackedSeq++
			}
		}
		// 无论是否去重，都回报该设备当前确认位点（幂等查询语义）。
		cp = fleet.Checkpoint{DeviceID: rec.DeviceID, AckedSeq: d.ackedSeq}
	}
	return cp, nil
}

// State 读取设备某状态键的确定值。
func (c *ControlPlane) State(tenantID, deviceID, key string) (string, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, d, err := c.lookup(tenantID, deviceID)
	if err != nil {
		return "", false, err
	}
	e, ok := d.state[key]
	return e.value, ok, nil
}

// History 返回设备的部署历史（按 Seq 升序）。
func (c *ControlPlane) History(tenantID, deviceID string) ([]fleet.DeploymentRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, d, err := c.lookup(tenantID, deviceID)
	if err != nil {
		return nil, err
	}
	out := make([]fleet.DeploymentRecord, len(d.history))
	copy(out, d.history)
	return out, nil
}

// CurrentVersion 返回设备当前部署版本。
func (c *ControlPlane) CurrentVersion(tenantID, deviceID string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, d, err := c.lookup(tenantID, deviceID)
	if err != nil {
		return "", err
	}
	return d.currentVersion, nil
}
