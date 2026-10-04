// Package controlplane 模拟云端控制面：多租户/多集群的部署下发、
// 幂等去重、配额与权限校验、回滚以及遥测汇聚。
package controlplane

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/fleet"
)

// Quota 是租户级配额。
type Quota struct {
	MaxDevices     int
	MaxDeployments int
}

// deviceState 内的冲突状态按 (Key -> Seq -> Value -> 代表性上报) 全量保留：
// 收敛结果只由这些上报内容决定，与到达顺序、批次切分、并发路数无关。
type deviceState struct {
	caps           fleet.Capabilities
	clusterID      string
	history        []fleet.DeploymentRecord
	currentVersion string
	contentHash    string          // 当前生效内容的规范化指纹
	seenContent    map[string]bool // 本设备上已生效过的内容指纹集合
	idempotency    map[string]*fleet.DeploymentRecord
	receivedSeq    map[uint64]bool // 已接收的遥测位点（用于连续确认位点推进）
	seenTelemetry  map[string]bool // 已接收的 (Key,Seq,Value) 去重索引
	ackedSeq       uint64          // 已确认的最大连续位点
	values         map[string]map[uint64]map[string]fleet.Telemetry
	conflicts      map[string]fleet.ConflictReport // 冲突身份 -> 规范化冲突记录（身份只由内容决定）
}

type tenantState struct {
	quota       Quota
	deployCount int
	devices     map[string]*deviceState // "clusterID\x00deviceID" -> state
}

// ControlPlane 是云端控制面的内存实现，并发安全。
type ControlPlane struct {
	mu        sync.Mutex
	tenants   map[string]*tenantState
	deviceIdx map[string]map[string]string // deviceID -> (tenantID -> clusterID)，用于区分跨租户/跨集群与不存在
	nextSeq   int64
}

// New 创建空控制面。
func New() *ControlPlane {
	return &ControlPlane{
		tenants:   make(map[string]*tenantState),
		deviceIdx: make(map[string]map[string]string),
	}
}

// deviceKey 是租户内的设备复合键：集群 + 设备。
func deviceKey(clusterID, deviceID string) string { return clusterID + "\x00" + deviceID }

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

// RegisterDevice 注册设备到租户与集群。设备的有效标识是 (租户, 集群, 设备ID)：
// 同租户不同集群下的同名设备相互独立；不同租户复用同名设备一律拒绝。
func (c *ControlPlane) RegisterDevice(tenantID, clusterID, deviceID string, caps fleet.Capabilities) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.tenants[tenantID]
	if t == nil {
		t = &tenantState{devices: make(map[string]*deviceState)}
		c.tenants[tenantID] = t
	}
	// 跨租户：该 deviceID 已被其他租户注册。
	if owners := c.deviceIdx[deviceID]; owners != nil {
		if owner := owners[tenantID]; owner == "" {
			for other := range owners {
				return fleet.NewForbidden("device %q belongs to tenant %q, not %q", deviceID, other, tenantID)
			}
		}
	}
	key := deviceKey(clusterID, deviceID)
	if _, ok := t.devices[key]; ok {
		return nil // 重复注册幂等
	}
	if t.quota.MaxDevices > 0 && len(t.devices) >= t.quota.MaxDevices {
		return fleet.NewQuotaExceeded("tenant %q device quota %d exceeded", tenantID, t.quota.MaxDevices)
	}
	t.devices[key] = &deviceState{
		caps:          caps,
		clusterID:     clusterID,
		seenContent:   make(map[string]bool),
		idempotency:   make(map[string]*fleet.DeploymentRecord),
		receivedSeq:   make(map[uint64]bool),
		seenTelemetry: make(map[string]bool),
		values:        make(map[string]map[uint64]map[string]fleet.Telemetry),
		conflicts:     make(map[string]fleet.ConflictReport),
	}
	if c.deviceIdx[deviceID] == nil {
		c.deviceIdx[deviceID] = make(map[string]string)
	}
	c.deviceIdx[deviceID][tenantID] = clusterID
	return nil
}

var versionRe = regexp.MustCompile(`^v\d+\.\d+\.\d+$`)

// lookup 在持锁状态下定位设备。
// clusterID 为空且该租户内存在唯一匹配设备时回退到唯一集群；
// 跨租户访问返回 FORBIDDEN（与 NOT_FOUND 明确区分），其余找不到返回 NOT_FOUND。
func (c *ControlPlane) lookup(tenantID, clusterID, deviceID string) (*tenantState, *deviceState, error) {
	t := c.tenants[tenantID]
	if t == nil {
		if owners := c.deviceIdx[deviceID]; owners != nil && owners[tenantID] == "" {
			for other := range owners {
				return nil, nil, fleet.NewForbidden("device %q belongs to tenant %q", deviceID, other)
			}
		}
		return nil, nil, fleet.NewNotFound("tenant %q not found", tenantID)
	}
	if clusterID != "" {
		d := t.devices[deviceKey(clusterID, deviceID)]
		if d != nil {
			return t, d, nil
		}
		if owners := c.deviceIdx[deviceID]; owners != nil && owners[tenantID] == "" {
			for other := range owners {
				return nil, nil, fleet.NewForbidden("device %q belongs to tenant %q", deviceID, other)
			}
		}
		return nil, nil, fleet.NewNotFound("device %q not found in tenant %q cluster %q", deviceID, tenantID, clusterID)
	}
	// clusterID 缺省：跨设备唯一标识仍由 deviceID 保证，查找唯一集群。
	var found *deviceState
	for k, d := range t.devices {
		if keyDeviceID(k) == deviceID {
			if found != nil {
				return nil, nil, fleet.NewNotFound("device %q is ambiguous in tenant %q across clusters; specify cluster", deviceID, tenantID)
			}
			found = d
		}
	}
	if found != nil {
		return t, found, nil
	}
	if owners := c.deviceIdx[deviceID]; owners != nil && owners[tenantID] == "" {
		for other := range owners {
			return nil, nil, fleet.NewForbidden("device %q belongs to tenant %q", deviceID, other)
		}
	}
	return nil, nil, fleet.NewNotFound("device %q not found in tenant %q", deviceID, tenantID)
}

// keyDeviceID 从复合键中取出 deviceID（复合键形如 clusterID\x00deviceID）。
func keyDeviceID(key string) string {
	if i := strings.IndexByte(key, 0); i >= 0 {
		return key[i+1:]
	}
	return key
}

// contentFingerprint 返回下发内容（版本 + 制品属性 + 依赖特性，特性按名排序后）的规范化指纹。
// 不包含 TenantID/ClusterID/DeviceID/IdempotencyKey：内容相同即视为同一内容，
// 但生效范围由调用处的 (租户, 集群, 设备) 限定，互不顶掉。
func contentFingerprint(a fleet.Artifact) string {
	feats := append([]string(nil), a.RequireFeature...)
	sort.Strings(feats)
	return fmt.Sprintf("v=%s|os=%s|arch=%s|size=%d|feat=%s",
		a.Version, a.OS, a.Arch, a.SizeBytes, strings.Join(feats, ","))
}

// Deploy 版本化部署下发；幂等、校验版本与能力、配额控制。
//
// 幂等分两层：
//  1. 显式幂等键：同键重复返回该请求第一次的结果（历史行为保留）。
//  2. 内容级幂等：在同一 (租户, 集群, 设备) 上，只要下发内容此前在该设备上生效过
//     （版本/OS/架构/大小/依赖特性均未变），无论是否带标识、标识是否变化，
//     该请求都是空操作：不切换当前生效版本、不追加历史、不消耗部署额度；
//     返回值始终描述设备“当前实际生效”的那一条记录（历史末条），
//     而不是该内容第一次生效时的旧记录——保证返回结果与设备真实状态一致。
//     若想把当前版本切回某个历史版本，应显式调用 Rollback（那是一次真正的切换）。
//     只有内容真正变化才算一次新部署；配额已满时重复内容仍允许生效。
//
// 校验顺序保证被拒请求无副作用：先做版本/能力校验，再判内容幂等，最后才查配额并落库。
func (c *ControlPlane) Deploy(spec fleet.DeploymentSpec) (*fleet.DeploymentRecord, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t, d, err := c.lookup(spec.TenantID, spec.ClusterID, spec.DeviceID)
	if err != nil {
		return nil, err
	}
	// 层 1：显式幂等键直接返回首次结果。
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
	// 层 2：内容级幂等。该内容只要在本设备上生效过，本次就是空操作：
	// 不切换版本、不追加历史、不消耗额度（配额已满也允许）；
	// 返回设备当前实际生效的记录（历史末条），使响应与真实状态严格一致。
	fp := contentFingerprint(spec.Artifact)
	if d.seenContent[fp] {
		cp := d.history[len(d.history)-1]
		return &cp, nil
	}
	// 配额校验（只有真正新增的一次部署才走到这里）。
	if t.quota.MaxDeployments > 0 && t.deployCount >= t.quota.MaxDeployments {
		return nil, fleet.NewQuotaExceeded("tenant %q deployment quota %d exceeded", spec.TenantID, t.quota.MaxDeployments)
	}
	rec := c.appendRecord(t, d, spec, "", fp)
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

func (c *ControlPlane) appendRecord(t *tenantState, d *deviceState, spec fleet.DeploymentSpec, rollbackOf, fp string) *fleet.DeploymentRecord {
	c.nextSeq++
	rec := fleet.DeploymentRecord{
		Seq:        c.nextSeq,
		Spec:       spec,
		RollbackOf: rollbackOf,
		AppliedAt:  time.Now().UTC(),
	}
	d.history = append(d.history, rec)
	d.currentVersion = spec.Artifact.Version
	d.contentHash = fp
	if d.seenContent == nil {
		d.seenContent = make(map[string]bool)
	}
	d.seenContent[fp] = true
	t.deployCount++
	return &d.history[len(d.history)-1]
}

// Rollback 将指定 (租户, 集群, 设备) 可复现地恢复到指定历史版本：复用历史中记录的完整 Spec。
// 设备的有效标识是 (租户, 集群, 设备ID)：同租户不同集群下的同名设备必须显式给出 clusterID，
// 回滚只影响该集群上的那台设备，其他集群的同名设备保持原样。
//
// 幂等规则与部署一致：
//   - 当前已是目标版本时（对同一版本重复触发回滚，或当前内容与目标相同），
//     直接返回设备当前实际生效的记录（历史末条），不追加历史、不消耗额度；
//   - 目标版本从未在该设备历史中出现返回 NOT_FOUND；
//   - 只有真正把当前版本切换到目标版本的第一次回滚才追加一条 RollbackOf 记录并计额度。
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
			target = &d.history[i]
		}
	}
	if target == nil {
		return nil, fleet.NewNotFound("device %q has no historical version %q", deviceID, targetVersion)
	}
	// 可复现：使用历史记录中的原始 Spec，剥离幂等键（内容级幂等不再依赖它）。
	spec := target.Spec
	spec.IdempotencyKey = ""
	fp := contentFingerprint(spec.Artifact)
	// 幂等：当前生效内容就是目标内容，返回当前实际生效的记录（历史末条），
	// 保证返回结果与设备真实状态严格一致；不追加历史、不消耗额度。
	if d.contentHash == fp {
		cp := d.history[len(d.history)-1]
		return &cp, nil
	}
	if t.quota.MaxDeployments > 0 && t.deployCount >= t.quota.MaxDeployments {
		return nil, fleet.NewQuotaExceeded("tenant %q deployment quota %d exceeded", tenantID, t.quota.MaxDeployments)
	}
	rec := c.appendRecord(t, d, spec, targetVersion, fp)
	cp := *rec
	return &cp, nil
}

// conflictIdentity 是一条冲突淘汰记录的内容身份：位置 + 位点 + 胜方值 + 败方值。
// 同一身份在任何顺序、批次、并发、重放下至多规范化记录一次，且记录内容逐字一致。
func conflictIdentity(r fleet.ConflictReport) string {
	return fmt.Sprintf("%s\x00%d\x00%s\x00%s", r.Key, r.Seq, r.Winner.Value, r.Loser.Value)
}

// repLess 为同一值选取唯一的“代表性上报”，顺序规则也只来自内容本身：
// 先比通道来源 Source，再比上报时间。首个到达者不享受任何特权，
// 因此代表性上报与到达顺序、批次、并发路数均无关。
func repLess(a, b fleet.Telemetry) bool {
	if a.Source != b.Source {
		return a.Source < b.Source
	}
	if !a.At.Equal(b.At) {
		return a.At.Before(b.At)
	}
	return false
}

// basisText 生成逐字稳定的人类可读判定依据，只包含上报内容。
func basisText(key string, seq uint64, winner, loser string) string {
	return fmt.Sprintf(
		"同一位置 %q 位点 %d 出现不同值；按 (Seq, Value) 字典序取较大值 %q，%q 淘汰（仅依据上报内容，与到达顺序/批次/并发无关）",
		key, seq, winner, loser)
}

// reconcileKey 依据 d.values 中该 Key 的全量内容，规范化重算它在“最高位点”上的
// 同位点多值冲突记录：最高位点取值字典序最大者胜，其余不同值各为一条败方记录。
// 位点更低的组属于正常版本推进/过期，不产生冲突。
//
// 冲突以 (Key,Seq,Winner.Value,Loser.Value) 为身份幂等登记：每次先清掉同 (Key,Seq) 的
// 旧身份，再只登记最终胜方对各败方的记录。这样“值尚未到齐”时产生的中间淘汰不会残留，
// 被淘汰记录与判定依据只由最终上报内容决定，与到达顺序、批次切分、并发路数无关。
// 所有计算都是持锁纯内容计算。
func (c *ControlPlane) reconcileKey(d *deviceState, key string) {
	seqs := d.values[key]
	if len(seqs) == 0 {
		return
	}
	var top uint64
	for s := range seqs {
		if s > top {
			top = s
		}
	}
	vals := seqs[top]
	if len(vals) < 2 {
		return // 最高位点只有一个值：没有同位点冲突
	}
	// 不同值按字典序排列，末位为胜方，其余为败方。
	var sorted []string
	for v := range vals {
		sorted = append(sorted, v)
	}
	sort.Strings(sorted)
	winVal := sorted[len(sorted)-1]
	winRep := vals[winVal]
	// 该 Key 在 top 位点的规范化冲突身份集合只有一个胜方（字典序最大值）：
	// 先清掉所有同 (Key,Seq) 的旧身份，再只登记最终胜方对各败方的记录。
	// 这样“值尚未到齐”时产生的中间淘汰不会残留，保证被淘汰记录与判定依据
	// 只由最终上报内容决定，与到达顺序/批次/并发完全无关。
	for id := range d.conflicts {
		r := d.conflicts[id]
		if r.Key == key && r.Seq == top {
			delete(d.conflicts, id)
		}
	}
	for _, lv := range sorted[:len(sorted)-1] {
		rep := fleet.ConflictReport{
			ClusterID: d.clusterID,
			DeviceID:  winRep.DeviceID,
			Key:       key,
			Seq:       top,
			Winner:    winRep,
			Loser:     vals[lv],
			Basis:     basisText(key, top, winVal, lv),
			Reason:    fleet.ReasonConflictResolved,
			At:        time.Now().UTC(),
		}
		d.conflicts[conflictIdentity(rep)] = rep
	}
}

// canonicalConflicts 返回设备累积冲突记录的规范化切片，按 位置/位点/败方值 排序，
// 保证多次运行、不同顺序下 Conflicts 的输出也完全一致。
func (c *ControlPlane) canonicalConflicts(d *deviceState) []fleet.ConflictReport {
	out := make([]fleet.ConflictReport, 0, len(d.conflicts))
	for _, r := range d.conflicts {
		out = append(out, r)
	}
	sort.Slice(out, func(i, j int) bool { return conflictIdentity(out[i]) < conflictIdentity(out[j]) })
	return out
}

// applyOne 在持锁状态下处理一条上报，返回该设备最新确认位点。
// 完全相同的 (ClusterID,DeviceID,Key,Seq,Value) 重复到达直接跳过（去重，不算冲突、不重复记录）。
func (c *ControlPlane) applyOne(d *deviceState, rec fleet.Telemetry) uint64 {
	tid := telemetryID(rec)
	if d.seenTelemetry[tid] {
		return d.ackedSeq
	}
	d.seenTelemetry[tid] = true
	d.receivedSeq[rec.Seq] = true

	if d.values[rec.Key] == nil {
		d.values[rec.Key] = make(map[uint64]map[string]fleet.Telemetry)
	}
	if d.values[rec.Key][rec.Seq] == nil {
		d.values[rec.Key][rec.Seq] = make(map[string]fleet.Telemetry)
	}
	// 同一值可能经多个来源到达：保留内容规范化后的唯一代表性上报。
	vm := d.values[rec.Key][rec.Seq]
	if cur, ok := vm[rec.Value]; !ok || repLess(rec, cur) {
		vm[rec.Value] = rec
	}
	c.reconcileKey(d, rec.Key)

	// 推进连续确认位点。
	for d.receivedSeq[d.ackedSeq+1] {
		d.ackedSeq++
	}
	return d.ackedSeq
}

// telemetryID 是一条上报内容的唯一标识（集群 + 设备 + 位置 + 位点 + 值）。
func telemetryID(r fleet.Telemetry) string {
	return fmt.Sprintf("%s\x00%s\x00%s\x00%d\x00%s", r.ClusterID, r.DeviceID, r.Key, r.Seq, r.Value)
}

// IngestDetailed 接收网关重连后续传的遥测；按内容去重、乱序容忍、确定性冲突收敛，
// 并回报本次调用后“规范化成立、且本批开始前尚不存在”的冲突淘汰记录。
// 一整批按原子视角处理：批内中间态（胜方尚未到齐时的临时淘汰）不会出现在回报中；
// 同一内容整批重放不产生任何回报，也不新增累积记录。
func (c *ControlPlane) IngestDetailed(tenantID string, batch []fleet.Telemetry) (fleet.Checkpoint, []fleet.ConflictReport, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 两阶段：先解析全部设备归属，任一不合法则整批不产生副作用。
	resolved := make([]*deviceState, len(batch))
	for i, rec := range batch {
		_, d, err := c.lookup(tenantID, rec.ClusterID, rec.DeviceID)
		if err != nil {
			return fleet.Checkpoint{}, nil, err
		}
		resolved[i] = d
	}
	// 快照本批涉及设备批前的冲突身份集合，用于批后差集。
	touched := make(map[*deviceState]map[string]bool)
	for _, d := range resolved {
		if _, ok := touched[d]; ok {
			continue
		}
		before := make(map[string]bool, len(d.conflicts))
		for id := range d.conflicts {
			before[id] = true
		}
		touched[d] = before
	}
	var cp fleet.Checkpoint
	for i, rec := range batch {
		acked := c.applyOne(resolved[i], rec)
		cp = fleet.Checkpoint{DeviceID: rec.DeviceID, AckedSeq: acked}
	}
	// 本次回报 = 批后规范化身份 − 批前身份；中间态身份批后已不存在，自然被排除。
	var reports []fleet.ConflictReport
	seen := make(map[string]bool)
	for _, d := range resolved {
		before := touched[d]
		for id, rep := range d.conflicts {
			if before[id] || seen[id] {
				continue
			}
			seen[id] = true
			reports = append(reports, rep)
		}
	}
	// 本次回报集合也只由内容决定：按身份排序，保证任意批次切分下顺序稳定。
	sort.Slice(reports, func(i, j int) bool { return conflictIdentity(reports[i]) < conflictIdentity(reports[j]) })
	return cp, reports, nil
}

// Ingest 接收网关重连后续传的遥测；按位点去重、乱序容忍、冲突确定性收敛。
// 收敛规则：同一 (device,key) 按 (Seq, Value) 字典序收敛——位点大者胜；
// 位点相同取值字典序较大者。收敛结果与到达顺序、批次切分、并发路数无关，
// 同位点不同值的被淘汰方可通过 Conflicts 查询其淘汰原因。
func (c *ControlPlane) Ingest(tenantID string, batch []fleet.Telemetry) (fleet.Checkpoint, error) {
	cp, _, err := c.IngestDetailed(tenantID, batch)
	return cp, err
}

// State 读取设备某状态键的确定值：取最高位点、同位点取值字典序最大者。
func (c *ControlPlane) State(tenantID, clusterID, deviceID, key string) (string, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, d, err := c.lookup(tenantID, clusterID, deviceID)
	if err != nil {
		return "", false, err
	}
	seqs := d.values[key]
	if len(seqs) == 0 {
		return "", false, nil
	}
	var top uint64
	for s := range seqs {
		if s > top {
			top = s
		}
	}
	var winVal string
	for v := range seqs[top] {
		if v > winVal {
			winVal = v
		}
	}
	return winVal, true, nil
}

// Conflicts 返回设备累积的冲突淘汰记录（规范化排序），用于解释最终值的由来。
// 记录集合与每条内容只由上报内容决定，与到达顺序、批次、并发、重放均无关。
func (c *ControlPlane) Conflicts(tenantID, clusterID, deviceID string) ([]fleet.ConflictReport, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_, d, err := c.lookup(tenantID, clusterID, deviceID)
	if err != nil {
		return nil, err
	}
	return c.canonicalConflicts(d), nil
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

// DeployCount 返回租户已生效的部署计数（重复内容不计入），供观测与测试使用。
func (c *ControlPlane) DeployCount(tenantID string) (int, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := c.tenants[tenantID]
	if t == nil {
		return 0, fleet.NewNotFound("tenant %q not found", tenantID)
	}
	return t.deployCount, nil
}
