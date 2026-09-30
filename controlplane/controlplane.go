// Package controlplane 模拟云端控制面：多租户/多集群的部署下发、
// 幂等去重、配额与权限校验、回滚以及遥测汇聚。
package controlplane

import (
	"regexp"
	"sort"
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

// conflictKey 标识“同一个状态位置”的同位点竞争：设备 + 键 + 位点。
type conflictKey struct {
	key string
	seq uint64
}

// conflictAgg 聚合同一位置同位点从多路上报收到的不同值。
type conflictAgg struct {
	values map[string]bool // 所有候选值（集合）
	report fleet.ConflictReport
}

type deviceState struct {
	caps               fleet.Capabilities
	clusterID          string
	history            []fleet.DeploymentRecord
	currentVersion     string
	idempotency        map[string]*fleet.DeploymentRecord // 幂等键 -> 首次生效记录
	contentFirst       map[string]*fleet.DeploymentRecord // 内容指纹 -> 该内容第一次生效的记录（同租户同集群内）
	currentFingerprint string                             // 当前生效内容的指纹
	currentRecord      *fleet.DeploymentRecord            // 当前生效内容对应的记录（重复回滚时原样返回）
	receivedSeq        map[uint64]bool                    // 已接收的遥测位点（去重）
	ackedSeq           uint64                             // 已确认的最大连续位点
	state              map[string]stateEntry
	firstValue         map[conflictKey]string       // 尚未冲突位置的首个上报值（用于检测同位点分歧）
	conflicts          map[conflictKey]*conflictAgg // 同位点多值冲突的候选值聚合
}

type tenantState struct {
	quota       Quota
	deployCount int
	devices     map[string]*deviceState // (clusterID,deviceID) 复合键 -> state
}

// deviceKey 是同一租户下设备的身份键：集群 + 设备。
// 不同集群下的同名设备各自独立，互不顶掉。
func deviceKey(clusterID, deviceID string) string {
	return clusterID + "\x00" + deviceID
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

// DeploymentCount 返回租户已消耗的部署额度（仅真正生效的新内容/回滚计数）。
func (c *ControlPlane) DeploymentCount(tenantID string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	if t := c.tenants[tenantID]; t != nil {
		return t.deployCount
	}
	return 0
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
	if _, ok := t.devices[deviceKey(clusterID, deviceID)]; ok {
		return nil // 重复注册幂等
	}
	if t.quota.MaxDevices > 0 && len(t.devices) >= t.quota.MaxDevices {
		return fleet.NewQuotaExceeded("tenant %q device quota %d exceeded", tenantID, t.quota.MaxDevices)
	}
	t.devices[deviceKey(clusterID, deviceID)] = &deviceState{
		caps:         caps,
		clusterID:    clusterID,
		idempotency:  make(map[string]*fleet.DeploymentRecord),
		contentFirst: make(map[string]*fleet.DeploymentRecord),
		receivedSeq:  make(map[uint64]bool),
		state:        make(map[string]stateEntry),
		firstValue:   make(map[conflictKey]string),
		conflicts:    make(map[conflictKey]*conflictAgg),
	}
	c.deviceIdx[deviceID] = tenantID
	return nil
}

var versionRe = regexp.MustCompile(`^v\d+\.\d+\.\d+$`)

// lookup 在持锁状态下按 (租户, 集群, 设备) 三元组定位设备，
// 并区分“跨租户”（FORBIDDEN）与“不存在”（NOT_FOUND）。
// 设备存在但属于同租户的别的集群时，视为本集群内资源不存在。
func (c *ControlPlane) lookup(tenantID, clusterID, deviceID string) (*tenantState, *deviceState, error) {
	t := c.tenants[tenantID]
	if t == nil {
		if owner, ok := c.deviceIdx[deviceID]; ok && owner != tenantID {
			return nil, nil, fleet.NewForbidden("device %q belongs to tenant %q", deviceID, owner)
		}
		return nil, nil, fleet.NewNotFound("tenant %q not found", tenantID)
	}
	d := t.devices[deviceKey(clusterID, deviceID)]
	if d == nil {
		if owner, ok := c.deviceIdx[deviceID]; ok && owner != tenantID {
			return nil, nil, fleet.NewForbidden("device %q belongs to tenant %q", deviceID, owner)
		}
		return nil, nil, fleet.NewNotFound("device %q not found in tenant %q cluster %q", deviceID, tenantID, clusterID)
	}
	return t, d, nil
}

// Deploy 版本化部署下发。
// 两层幂等（作用域均为同一租户+同一集群下的同一台设备）：
//  1. 请求幂等键：同键重放（含重试链路换键前的原始请求）直接返回首次结果；
//  2. 内容级幂等：不带键或键被换掉也没关系——只要版本与制品属性（OS/架构/大小/依赖特性）
//     与该设备历史上某次生效的内容完全一致，就返回第一次生效的那条记录，
//     不新增历史、不推进版本、不消耗部署额度。
//
// 只有该设备从未生效过的内容才算一次新部署。
// 非法版本、不兼容、跨租户、超配额的请求在任何写入前被拒绝，不留任何副作用。
func (c *ControlPlane) Deploy(spec fleet.DeploymentSpec) (*fleet.DeploymentRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, d, err := c.lookup(spec.TenantID, spec.ClusterID, spec.DeviceID)
	if err != nil {
		return nil, err
	}
	// 请求级幂等：同幂等键直接返回首次结果，不产生新记录。
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
	// 内容级幂等：校验通过后、写入与配额扣减前判定。
	fp := contentFingerprint(spec.Artifact)
	if first, ok := d.contentFirst[fp]; ok {
		// 内容此前已生效过：无论当前版本是否已被别的内容顶替，都只返回首次记录。
		d.currentFingerprint = fp
		d.currentVersion = spec.Artifact.Version
		d.currentRecord = first
		if spec.IdempotencyKey != "" {
			d.idempotency[spec.IdempotencyKey] = first
		}
		cp := *first
		return &cp, nil
	}
	// 配额校验（只有真正的新内容才扣额度）。
	if t.quota.MaxDeployments > 0 && t.deployCount >= t.quota.MaxDeployments {
		return nil, fleet.NewQuotaExceeded("tenant %q deployment quota %d exceeded", spec.TenantID, t.quota.MaxDeployments)
	}
	rec := c.appendRecord(t, d, spec, "")
	d.contentFirst[fp] = rec
	d.currentFingerprint = fp
	d.currentRecord = rec
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
	fp := contentFingerprint(spec.Artifact)
	d.currentVersion = spec.Artifact.Version
	d.currentFingerprint = fp
	d.currentRecord = &d.history[len(d.history)-1]
	t.deployCount++
	return &d.history[len(d.history)-1]
}

// Rollback 将设备可复现地恢复到指定历史版本：复用历史中记录的完整 Spec。
// 幂等规则（同租户同集群同设备）：
//   - 目标版本不在历史中：返回 NOT_FOUND，不改任何状态；
//   - 当前生效内容已与目标历史版本一致（含对同一目标的重复回滚）：返回当前生效记录，
//     不新增历史、不重复占额度；
//   - 否则产生一条 RollbackOf=目标版本 的新记录（复用原始 Spec），消耗一次部署额度。
func (c *ControlPlane) Rollback(tenantID, clusterID, deviceID, targetVersion string) (*fleet.DeploymentRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, d, err := c.lookup(tenantID, clusterID, deviceID)
	if err != nil {
		return nil, err
	}
	var target *fleet.DeploymentRecord
	for i := range d.history {
		if d.history[i].Spec.Artifact.Version == targetVersion {
			target = &d.history[i] // 取最后一条匹配记录，保证与当前历史语义一致
		}
	}
	if target == nil {
		return nil, fleet.NewNotFound("device %q has no historical version %q", deviceID, targetVersion)
	}
	targetFP := contentFingerprint(target.Spec.Artifact)
	// 幂等：当前生效的就是该内容（重复回滚 / 内容已在此版本），原样返回当前生效记录。
	if d.currentFingerprint == targetFP && d.currentRecord != nil {
		cp := *d.currentRecord
		return &cp, nil
	}
	// 配额校验（只有真正产生新记录时才扣额度）。
	if t.quota.MaxDeployments > 0 && t.deployCount >= t.quota.MaxDeployments {
		return nil, fleet.NewQuotaExceeded("tenant %q deployment quota %d exceeded", tenantID, t.quota.MaxDeployments)
	}
	// 可复现：使用历史记录中的原始 Spec，仅替换幂等键避免与首次下发混淆。
	// contentFirst 保留该内容最早生效的记录，回滚不覆盖“第一次生效”。
	spec := target.Spec
	spec.IdempotencyKey = ""
	rec := c.appendRecord(t, d, spec, targetVersion)
	cp := *rec
	return &cp, nil
}

// IngestResult 是一次 Ingest 的结果：确认位点 + 本批次新触发/更新的冲突报告。
type IngestResult struct {
	Checkpoint fleet.Checkpoint
	Conflicts  []fleet.ConflictReport
}

// Ingest 接收网关重连后续传的遥测；按位点去重、乱序容忍、冲突确定性收敛。
// 收敛规则（同一租户+集群下的同一台设备、同一个键）：
//   - 位点不同：位点大者胜，较低位点的值过期、不覆盖（与到达顺序无关）；
//   - 位点相同但值不同（多路上报同一位置）：按值字典序取最大者为采纳值，
//     只由内容决定，与谁先到/批次切分/并发路数/重跑次数无关；
//   - 被淘汰的值不会静默消失：记录在 ConflictReport 中，可通过 Conflicts 查询，
//     报告含采纳值、淘汰值与裁决规则码。
func (c *ControlPlane) Ingest(tenantID string, batch []fleet.Telemetry) (IngestResult, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var res IngestResult
	touched := map[conflictKey]*conflictAgg{}
	for _, rec := range batch {
		_, d, err := c.lookup(tenantID, rec.ClusterID, rec.DeviceID)
		if err != nil {
			return IngestResult{}, err
		}
		ck := conflictKey{key: rec.Key, seq: rec.Seq}
		agg := d.conflicts[ck] // 只有已出现过分歧的位置才有聚合
		var prevValue string
		havePrev := false
		if agg == nil {
			// 尚未冲突：若该 (键,位点) 此前以单值出现过，firstValue 里有记录。
			if v, ok := d.firstValue[ck]; ok {
				prevValue, havePrev = v, true
			}
		}
		switch {
		case agg != nil:
			// 已知冲突位置：并入候选值（重复值幂等）。
			if !agg.values[rec.Value] {
				agg.values[rec.Value] = true
				winner, losers := reconcileSameSeq(agg.values)
				agg.report = fleet.ConflictReport{
					TenantID:  tenantID,
					ClusterID: d.clusterID,
					DeviceID:  rec.DeviceID,
					Key:       rec.Key,
					Seq:       rec.Seq,
					Winner:    winner,
					Losers:    losers,
					Rule:      fleet.RuleSameSeqDistinctValues,
				}
				touched[ck] = agg
			}
		case havePrev && prevValue != rec.Value:
			// 同一位置同位点第一次出现不同值：升级为冲突，纯内容裁决并记录。
			agg = &conflictAgg{values: map[string]bool{prevValue: true, rec.Value: true}}
			d.conflicts[ck] = agg
			delete(d.firstValue, ck)
			winner, losers := reconcileSameSeq(agg.values)
			agg.report = fleet.ConflictReport{
				TenantID:  tenantID,
				ClusterID: d.clusterID,
				DeviceID:  rec.DeviceID,
				Key:       rec.Key,
				Seq:       rec.Seq,
				Winner:    winner,
				Losers:    losers,
				Rule:      fleet.RuleSameSeqDistinctValues,
			}
			touched[ck] = agg
		default:
			// 单值位置：记录首值（仅用于后续冲突检测），不产生冲突报告。
			d.firstValue[ck] = rec.Value
		}
		// 状态收敛：高位点胜；同位点冲突取裁决后的采纳值。
		cur := d.state[rec.Key]
		switch {
		case rec.Seq > cur.seq:
			if agg != nil {
				winner, _ := reconcileSameSeq(agg.values)
				d.state[rec.Key] = stateEntry{value: winner, seq: rec.Seq}
			} else {
				d.state[rec.Key] = stateEntry{value: rec.Value, seq: rec.Seq}
			}
		case rec.Seq == cur.seq && agg != nil:
			if winner, _ := reconcileSameSeq(agg.values); cur.value != winner {
				d.state[rec.Key] = stateEntry{value: winner, seq: rec.Seq}
			}
		}
		// seq < cur.seq：过期值，不覆盖。
		if !d.receivedSeq[rec.Seq] {
			d.receivedSeq[rec.Seq] = true
			// 推进连续确认位点。
			for d.receivedSeq[d.ackedSeq+1] {
				d.ackedSeq++
			}
		}
		// 无论是否去重，都回报该设备当前确认位点（幂等查询语义）。
		res.Checkpoint = fleet.Checkpoint{DeviceID: rec.DeviceID, AckedSeq: d.ackedSeq}
	}
	res.Conflicts = buildTouchedReports(touched)
	return res, nil
}

// buildTouchedReports 按 (键, 位点) 稳定顺序展开本批次触及的冲突报告。
func buildTouchedReports(set map[conflictKey]*conflictAgg) []fleet.ConflictReport {
	out := make([]fleet.ConflictReport, 0, len(set))
	for _, agg := range set {
		out = append(out, agg.report)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key != out[j].Key {
			return out[i].Key < out[j].Key
		}
		return out[i].Seq < out[j].Seq
	})
	return out
}

// State 读取设备某状态键的确定值。
func (c *ControlPlane) State(tenantID, clusterID, deviceID, key string) (string, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, d, err := c.lookup(tenantID, clusterID, deviceID)
	if err != nil {
		return "", false, err
	}
	e, ok := d.state[key]
	return e.value, ok, nil
}

// Conflicts 返回设备上记录在案的同位点冲突裁决报告（按键、位点升序）。
// 报告说明：采纳值（Winner）、被淘汰值（Losers）、判定规则码（Rule）。
// 被淘汰的上报不会静默消失；同一组内容重复重放不会产生重复报告。
func (c *ControlPlane) Conflicts(tenantID, clusterID, deviceID string) ([]fleet.ConflictReport, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, d, err := c.lookup(tenantID, clusterID, deviceID)
	if err != nil {
		return nil, err
	}
	return buildConflictReports(d.conflicts), nil
}

// History 返回设备的部署历史（按 Seq 升序）。
func (c *ControlPlane) History(tenantID, clusterID, deviceID string) ([]fleet.DeploymentRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, d, err := c.lookup(tenantID, clusterID, deviceID)
	if err != nil {
		return nil, err
	}
	out := make([]fleet.DeploymentRecord, len(d.history))
	copy(out, d.history)
	return out, nil
}

// CurrentVersion 返回设备当前部署版本。
func (c *ControlPlane) CurrentVersion(tenantID, clusterID, deviceID string) (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, d, err := c.lookup(tenantID, clusterID, deviceID)
	if err != nil {
		return "", err
	}
	return d.currentVersion, nil
}
