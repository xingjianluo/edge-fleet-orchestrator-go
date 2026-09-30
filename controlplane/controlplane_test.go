package controlplane_test

import (
	"fmt"
	"reflect"
	"sync"
	"testing"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/controlplane"
	"github.com/xingjianluo/edge-fleet-orchestrator-go/fleet"
)

var caps = fleet.Capabilities{Arch: "arm64", OS: "linux", MaxArtifact: 1 << 20, Features: []string{"gpu"}}

func artifact(v string) fleet.Artifact {
	return fleet.Artifact{Version: v, Arch: "arm64", OS: "linux", SizeBytes: 1024}
}

func setup(t *testing.T) *controlplane.ControlPlane {
	t.Helper()
	cp := controlplane.New()
	cp.SetQuota("tenant-a", controlplane.Quota{MaxDevices: 10, MaxDeployments: 100})
	if err := cp.RegisterDevice("tenant-a", "cluster-1", "dev-1", caps); err != nil {
		t.Fatalf("register: %v", err)
	}
	return cp
}

func TestDeployIdempotent(t *testing.T) {
	cp := setup(t)
	spec := fleet.DeploymentSpec{TenantID: "tenant-a", ClusterID: "cluster-1", DeviceID: "dev-1",
		Artifact: artifact("v1.0.0"), IdempotencyKey: "req-1"}
	r1, err := cp.Deploy(spec)
	if err != nil {
		t.Fatalf("first deploy: %v", err)
	}
	r2, err := cp.Deploy(spec) // 同幂等键重复下发
	if err != nil {
		t.Fatalf("duplicate deploy: %v", err)
	}
	t.Logf("输入: 同幂等键 %q 下发两次; 判定依据: 两次返回同一 Seq 且历史只有 1 条", spec.IdempotencyKey)
	t.Logf("结果: seq1=%d seq2=%d", r1.Seq, r2.Seq)
	if r1.Seq != r2.Seq {
		t.Fatalf("idempotent deploy produced different seq: %d vs %d", r1.Seq, r2.Seq)
	}
	hist, _ := cp.History("tenant-a", "cluster-1", "dev-1")
	if len(hist) != 1 {
		t.Fatalf("expected 1 history record, got %d", len(hist))
	}
}

func TestDeployInvalidVersion(t *testing.T) {
	cp := setup(t)
	spec := fleet.DeploymentSpec{TenantID: "tenant-a", ClusterID: "cluster-1", DeviceID: "dev-1",
		Artifact: artifact("1.0"), IdempotencyKey: "req-bad"}
	_, err := cp.Deploy(spec)
	t.Logf("输入: 版本 %q; 判定依据: 不满足 vMAJOR.MINOR.PATCH 应拒绝", spec.Artifact.Version)
	t.Logf("结果: err=%v reason=%s", err, fleet.ReasonOf(err))
	if fleet.ReasonOf(err) != fleet.ReasonInvalidVersion {
		t.Fatalf("expected INVALID_VERSION, got %v", err)
	}
}

func TestDeployIncompatible(t *testing.T) {
	cp := setup(t)
	spec := fleet.DeploymentSpec{TenantID: "tenant-a", ClusterID: "cluster-1", DeviceID: "dev-1",
		Artifact: fleet.Artifact{Version: "v1.0.0", Arch: "amd64", OS: "linux", SizeBytes: 1}}
	_, err := cp.Deploy(spec)
	t.Logf("输入: 制品 amd64/linux, 设备 arm64/linux; 判定依据: 架构不匹配应拒绝")
	t.Logf("结果: err=%v reason=%s", err, fleet.ReasonOf(err))
	if fleet.ReasonOf(err) != fleet.ReasonIncompatible {
		t.Fatalf("expected INCOMPATIBLE, got %v", err)
	}
}

func TestRollbackReproducible(t *testing.T) {
	cp := setup(t)
	for i, v := range []string{"v1.0.0", "v1.1.0", "v2.0.0"} {
		_, err := cp.Deploy(fleet.DeploymentSpec{TenantID: "tenant-a", ClusterID: "cluster-1",
			DeviceID: "dev-1", Artifact: artifact(v), IdempotencyKey: fmt.Sprintf("req-%d", i)})
		if err != nil {
			t.Fatalf("deploy %s: %v", v, err)
		}
	}
	rec, err := cp.Rollback("tenant-a", "cluster-1", "dev-1", "v1.0.0")
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	cur, _ := cp.CurrentVersion("tenant-a", "cluster-1", "dev-1")
	t.Logf("输入: 历史 v1.0.0->v1.1.0->v2.0.0, 回滚到 v1.0.0; 判定依据: 回滚记录复用历史 Spec 且当前版本为 v1.0.0")
	t.Logf("结果: rollbackOf=%s current=%s", rec.RollbackOf, cur)
	if cur != "v1.0.0" || rec.RollbackOf != "v1.0.0" {
		t.Fatalf("rollback failed: current=%s record=%+v", cur, rec)
	}
	// 再次回滚到同一目标：结果可复现（同一目标版本、同一制品内容）。
	rec2, err := cp.Rollback("tenant-a", "cluster-1", "dev-1", "v1.0.0")
	if err != nil {
		t.Fatalf("second rollback: %v", err)
	}
	if !reflect.DeepEqual(rec2.Spec.Artifact, rec.Spec.Artifact) {
		t.Fatalf("rollback not reproducible: %+v vs %+v", rec.Spec.Artifact, rec2.Spec.Artifact)
	}
	// 回滚到不存在的版本应报 NOT_FOUND。
	_, err = cp.Rollback("tenant-a", "cluster-1", "dev-1", "v9.9.9")
	t.Logf("输入: 回滚到未部署过的 v9.9.9; 判定依据: 历史无此版本应 NOT_FOUND, 实际 reason=%s", fleet.ReasonOf(err))
	if fleet.ReasonOf(err) != fleet.ReasonNotFound {
		t.Fatalf("expected NOT_FOUND, got %v", err)
	}
}

func TestCrossTenantForbidden(t *testing.T) {
	cp := setup(t)
	cp.SetQuota("tenant-b", controlplane.Quota{MaxDevices: 10, MaxDeployments: 100})
	_, err := cp.Deploy(fleet.DeploymentSpec{TenantID: "tenant-b", ClusterID: "cluster-1",
		DeviceID: "dev-1", Artifact: artifact("v1.0.0")})
	t.Logf("输入: tenant-b 操作 tenant-a 的设备 dev-1; 判定依据: 跨租户应 FORBIDDEN, 实际 reason=%s", fleet.ReasonOf(err))
	if fleet.ReasonOf(err) != fleet.ReasonForbidden {
		t.Fatalf("expected FORBIDDEN, got %v", err)
	}
}

func TestQuotaExceeded(t *testing.T) {
	cp := controlplane.New()
	cp.SetQuota("tenant-q", controlplane.Quota{MaxDevices: 1, MaxDeployments: 1})
	if err := cp.RegisterDevice("tenant-q", "c", "d1", caps); err != nil {
		t.Fatalf("register d1: %v", err)
	}
	err := cp.RegisterDevice("tenant-q", "c", "d2", caps)
	t.Logf("输入: 设备配额=1 注册第 2 台; 判定依据: 应 QUOTA_EXCEEDED, 实际 reason=%s", fleet.ReasonOf(err))
	if fleet.ReasonOf(err) != fleet.ReasonQuotaExceeded {
		t.Fatalf("expected QUOTA_EXCEEDED for device, got %v", err)
	}
	spec := fleet.DeploymentSpec{TenantID: "tenant-q", ClusterID: "c", DeviceID: "d1", Artifact: artifact("v1.0.0")}
	if _, err := cp.Deploy(spec); err != nil {
		t.Fatalf("first deploy: %v", err)
	}
	spec.Artifact.Version = "v1.0.1"
	_, err = cp.Deploy(spec)
	t.Logf("输入: 部署配额=1 发起第 2 次部署; 判定依据: 应 QUOTA_EXCEEDED, 实际 reason=%s", fleet.ReasonOf(err))
	if fleet.ReasonOf(err) != fleet.ReasonQuotaExceeded {
		t.Fatalf("expected QUOTA_EXCEEDED for deploy, got %v", err)
	}
}

func TestConcurrentDeploysDeterministic(t *testing.T) {
	cp := setup(t)
	const n = 32
	var wg sync.WaitGroup
	recs := make([]*fleet.DeploymentRecord, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			// 两种不同内容交替，各带不同/缺失的幂等键：
			// 同内容无论键怎么换（含无键）都只生效一次，历史应恰为 2 条。
			v := "v1.0.0"
			if i%2 == 1 {
				v = "v1.0.1"
			}
			key := ""
			if i%4 >= 2 {
				key = fmt.Sprintf("key-%d", i) // 每次都不一样，模拟重试链路换键
			}
			recs[i], errs[i] = cp.Deploy(fleet.DeploymentSpec{TenantID: "tenant-a", ClusterID: "cluster-1",
				DeviceID: "dev-1", Artifact: artifact(v), IdempotencyKey: key})
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("deploy %d: %v", i, err)
		}
	}
	hist, _ := cp.History("tenant-a", "cluster-1", "dev-1")
	t.Logf("输入: %d 并发下发、两种相同内容、幂等键缺失或各不相同; 判定依据: 内容级去重后历史应恰为 2 条且 Seq 唯一, 实际 %d 条", n, len(hist))
	if len(hist) != 2 {
		t.Fatalf("expected 2 records, got %d", len(hist))
	}
	seen := map[int64]bool{}
	for _, h := range hist {
		if seen[h.Seq] {
			t.Fatalf("duplicate seq %d", h.Seq)
		}
		seen[h.Seq] = true
	}
	// 同内容的所有并发响应必须是同一条首次记录（与调度顺序无关）。
	firstSeq := map[string]int64{}
	for i, r := range recs {
		v := r.Spec.Artifact.Version
		if s, ok := firstSeq[v]; ok && s != r.Seq {
			t.Fatalf("content %q returned inconsistent seq: %d vs %d (deploy %d)", v, s, r.Seq, i)
		}
		firstSeq[v] = r.Seq
	}
	if firstSeq["v1.0.0"] == firstSeq["v1.0.1"] {
		t.Fatalf("distinct contents collapsed to same seq %d", firstSeq["v1.0.0"])
	}
	t.Logf("判定依据: v1.0.0 全部返回 seq=%d, v1.0.1 全部返回 seq=%d", firstSeq["v1.0.0"], firstSeq["v1.0.1"])
}
