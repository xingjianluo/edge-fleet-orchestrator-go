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
	ClusterID string // 上报所属集群（同租户不同集群下的同名设备相互独立）
	DeviceID  string
	Source    string // 上报通道/网关标识；不参与收敛判定，仅用于冲突可解释
	Seq       uint64 // 设备侧单调递增位点
	Key       string // 状态键，如 "temperature" 或 "deployed_version"
	Value     string
	At        time.Time
}

// ConflictReport 描述同一状态位置上在冲突中被淘汰的一方，以及确定性判定依据。
// 上报被去重（完全相同的内容重复到达）不产生冲突报告；只有同位置不同值才产生。
type ConflictReport struct {
	ClusterID string
	DeviceID  string
	Key       string
	Seq       uint64 // 冲突发生的状态位点
	Winner    Telemetry
	Loser     Telemetry
	Basis     string // 人类可读的判定依据（只基于上报内容）
	Reason    Reason // 固定为 ReasonConflictResolved
	At        time.Time
}

// Checkpoint 记录某设备已确认同步的位点。
type Checkpoint struct {
	DeviceID string
	AckedSeq uint64 // 已被控制面确认的最大连续位点
}
