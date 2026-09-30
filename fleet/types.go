// Package fleet 定义边缘舰队编排系统的共享领域类型与可区分错误原因。
package fleet

import (
	"errors"
	"fmt"
	"time"
)

// Reason 是可区分的失败原因码，用于测试与运维判定。
type Reason string

const (
	ReasonNone             Reason = "NONE"
	ReasonInvalidVersion   Reason = "INVALID_VERSION"   // 版本格式非法
	ReasonIncompatible     Reason = "INCOMPATIBLE"      // 与设备能力不兼容
	ReasonNotFound         Reason = "NOT_FOUND"         // 资源不存在
	ReasonForbidden        Reason = "FORBIDDEN"         // 跨租户访问
	ReasonQuotaExceeded    Reason = "QUOTA_EXCEEDED"    // 超出配额
	ReasonDuplicate        Reason = "DUPLICATE"         // 重复请求（幂等命中）
	ReasonConflictResolved Reason = "CONFLICT_RESOLVED" // 冲突已按规则收敛
)

// Error 携带可区分原因的领域错误。
type Error struct {
	Reason  Reason
	Message string
}

func (e *Error) Error() string { return fmt.Sprintf("%s: %s", e.Reason, e.Message) }

// ReasonOf 从错误中提取失败原因；非领域错误返回空串。
func ReasonOf(err error) Reason {
	var de *Error
	if errors.As(err, &de) {
		return de.Reason
	}
	return ""
}

func newError(r Reason, format string, args ...any) *Error {
	return &Error{Reason: r, Message: fmt.Sprintf(format, args...)}
}

// NewInvalidVersion 构造版本非法错误。
func NewInvalidVersion(format string, args ...any) *Error {
	return newError(ReasonInvalidVersion, format, args...)
}

// NewIncompatible 构造能力不兼容错误。
func NewIncompatible(format string, args ...any) *Error {
	return newError(ReasonIncompatible, format, args...)
}

// NewNotFound 构造资源不存在错误。
func NewNotFound(format string, args ...any) *Error { return newError(ReasonNotFound, format, args...) }

// NewForbidden 构造跨租户访问错误。
func NewForbidden(format string, args ...any) *Error {
	return newError(ReasonForbidden, format, args...)
}

// NewQuotaExceeded 构造配额超限错误。
func NewQuotaExceeded(format string, args ...any) *Error {
	return newError(ReasonQuotaExceeded, format, args...)
}

// Capabilities 描述设备能力，用于版本兼容性校验。
type Capabilities struct {
	Arch        string   // 设备架构，如 arm64 / amd64
	OS          string   // 设备操作系统
	MaxArtifact int64    // 设备可接受的最大制品字节数
	Features    []string // 设备支持的特性集合
}

// Supports 判断设备是否支持某特性。
func (c Capabilities) Supports(feature string) bool {
	for _, f := range c.Features {
		if f == feature {
			return true
		}
	}
	return false
}

// Artifact 是一个待下发的版本化制品。
type Artifact struct {
	Version        string   // 语义化版本，如 v1.2.3
	Arch           string   // 目标架构
	OS             string   // 目标操作系统
	SizeBytes      int64    // 制品大小
	RequireFeature []string // 依赖的设备特性
}

// DeploymentSpec 描述一次部署下发的内容。
type DeploymentSpec struct {
	TenantID       string
	ClusterID      string
	DeviceID       string
	Artifact       Artifact
	IdempotencyKey string // 幂等键：同键重复下发返回首次结果
}

// DeploymentRecord 是控制面记录的部署历史条目（回滚的依据）。
type DeploymentRecord struct {
	Seq        int64 // 全局单调递增序号
	Spec       DeploymentSpec
	RollbackOf string // 若是回滚产生，记录目标版本；否则为空
	AppliedAt  time.Time
}

// Telemetry 是设备/网关上报的一条遥测或状态变更。
type Telemetry struct {
	TenantID  string // 上报所属租户（由网关填充，控制面校验时以批次租户为准）
	ClusterID string // 上报所属集群：状态位置按 (租户,集群,设备,键) 隔离
	DeviceID  string
	Seq       uint64 // 设备侧单调递增位点
	Key       string // 状态键，如 "temperature" 或 "deployed_version"
	Value     string
	At        time.Time
}

// 冲突裁决规则码：只依赖上报内容本身，与到达顺序/批次切分/并发路数无关。
const (
	// RuleSameSeqDistinctValues：同一 (设备,键,位点) 上报了多个不同值，
	// 按值的字典序取最大者为采纳值，其余为被淘汰值。
	RuleSameSeqDistinctValues = "SAME_SEQ_DISTINCT_VALUES: lexicographically greatest value wins"
)

// ConflictReport 描述一次状态冲突的确定性裁决结果，用于解释最终值的来源。
// 同一份内容无论按什么顺序、分多少批、并发多少路重放，报告内容都一致。
type ConflictReport struct {
	TenantID  string
	ClusterID string
	DeviceID  string
	Key       string
	Seq       uint64
	Winner    string   // 被采纳的值
	Losers    []string // 被淘汰的值（按字典序升序）
	Rule      string   // 裁决规则码（判定依据）
}

// Checkpoint 记录某设备已确认同步的位点。
type Checkpoint struct {
	DeviceID string
	AckedSeq uint64 // 已被控制面确认的最大连续位点
}
