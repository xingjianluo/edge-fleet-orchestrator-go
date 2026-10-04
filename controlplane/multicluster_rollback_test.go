package controlplane_test

import (
	"fmt"
	"testing"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/controlplane"
	"github.com/xingjianluo/edge-fleet-orchestrator-go/fleet"
)

// 多集群同名设备：回滚必须按 (租户, 集群, 设备) 精确定位——
// 对 cluster-a 的 edge-01 回滚只影响 cluster-a，cluster-b 的同名设备保持原样；
// 对同一集群重复回滚同一目标仍只生效一次（不多历史、不占额度）。
func TestMultiClusterSameNameDeviceRollback(t *testing.T) {
	cp := controlplane.New()
	cp.SetQuota("tenant-m", controlplane.Quota{MaxDevices: 10, MaxDeployments: 100})
	for _, cluster := range []string{"cluster-a", "cluster-b"} {
		if err := cp.RegisterDevice("tenant-m", cluster, "edge-01", fullCaps); err != nil {
			t.Fatalf("register %s: %v", cluster, err)
		}
	}
	// 两个集群的同名设备各自走出不同的版本轨迹。
	for i, v := range []string{"v1.0.0", "v1.1.0", "v2.0.0"} {
		mustDeploy(t, cp, deploy("tenant-m", "cluster-a", "edge-01", fmt.Sprintf("a-%d", i), fullArtifact(v)))
	}
	for i, v := range []string{"v1.0.0", "v3.0.0"} {
		mustDeploy(t, cp, deploy("tenant-m", "cluster-b", "edge-01", fmt.Sprintf("b-%d", i), fullArtifact(v)))
	}
	countBefore, _ := cp.DeployCount("tenant-m")
	t.Logf("业务: 多集群同名设备分别回滚; 输入: cluster-a/cluster-b 各有 edge-01, 轨迹分别为 v1.0.0->v1.1.0->v2.0.0 与 v1.0.0->v3.0.0")
	t.Logf("判定依据: 回滚 cluster-a 的 edge-01 到 v1.0.0 只改 cluster-a, cluster-b 保持 v3.0.0; 重复回滚不多历史、不占额度")

	// 对 cluster-a 的 edge-01 回滚到 v1.0.0：真实切换，追加一条记录。
	rb, err := cp.Rollback("tenant-m", "cluster-a", "edge-01", "v1.0.0")
	if err != nil {
		t.Fatalf("rollback cluster-a: %v", err)
	}
	curA, _ := cp.CurrentVersion("tenant-m", "cluster-a", "edge-01")
	curB, _ := cp.CurrentVersion("tenant-m", "cluster-b", "edge-01")
	histA, _ := cp.History("tenant-m", "cluster-a", "edge-01")
	histB, _ := cp.History("tenant-m", "cluster-b", "edge-01")
	countAfter, _ := cp.DeployCount("tenant-m")
	t.Logf("结果: cluster-a 当前=%q 历史=%d 条, cluster-b 当前=%q 历史=%d 条, 额度 %d->%d, 回滚记录 RollbackOf=%q",
		curA, len(histA), curB, len(histB), countBefore, countAfter, rb.RollbackOf)
	if curA != "v1.0.0" || rb.RollbackOf != "v1.0.0" {
		t.Fatalf("cluster-a rollback failed: cur=%q rb=%+v", curA, rb)
	}
	if curB != "v3.0.0" {
		t.Fatalf("cluster-b same-name device affected: cur=%q want v3.0.0", curB)
	}
	if len(histA) != 4 || len(histB) != 2 {
		t.Fatalf("history isolation broken: cluster-a=%d want 4, cluster-b=%d want 2", len(histA), len(histB))
	}
	if countAfter != countBefore+1 {
		t.Fatalf("first rollback must charge exactly once: %d -> %d", countBefore, countAfter)
	}

	// 对 cluster-a 重复回滚同一目标：幂等，不追加历史、不占额度，返回该内容首次生效记录。
	rb2, err := cp.Rollback("tenant-m", "cluster-a", "edge-01", "v1.0.0")
	if err != nil {
		t.Fatalf("repeat rollback cluster-a: %v", err)
	}
	histA2, _ := cp.History("tenant-m", "cluster-a", "edge-01")
	countAfter2, _ := cp.DeployCount("tenant-m")
	t.Logf("输入: cluster-a 再次回滚 v1.0.0; 判定依据: 返回首次生效 Seq 且历史/额度不变, 实际 Seq=%d 历史=%d 额度=%d",
		rb2.Seq, len(histA2), countAfter2)
	if len(histA2) != 4 || countAfter2 != countAfter {
		t.Fatalf("repeat rollback not idempotent: hist=%d count=%d", len(histA2), countAfter2)
	}
	if rb2.Spec.Artifact.Version != "v1.0.0" {
		t.Fatalf("repeat rollback returned %+v, want v1.0.0 content", rb2)
	}

	// cluster-b 的同名设备可独立回滚到它自己的历史版本，互不影响。
	rbB, err := cp.Rollback("tenant-m", "cluster-b", "edge-01", "v1.0.0")
	if err != nil {
		t.Fatalf("rollback cluster-b: %v", err)
	}
	curA2, _ := cp.CurrentVersion("tenant-m", "cluster-a", "edge-01")
	curB2, _ := cp.CurrentVersion("tenant-m", "cluster-b", "edge-01")
	t.Logf("输入: cluster-b 回滚 v1.0.0; 判定依据: cluster-b 当前=%q 且 cluster-a 仍为 %q", curB2, curA2)
	if curB2 != "v1.0.0" || rbB.RollbackOf != "v1.0.0" || curA2 != "v1.0.0" {
		t.Fatalf("cluster-b rollback broken: curA=%q curB=%q rb=%+v", curA2, curB2, rbB)
	}

	// 目标版本只存在于另一个集群的历史里：本集群视角必须是 NOT_FOUND。
	_, err = cp.Rollback("tenant-m", "cluster-a", "edge-01", "v3.0.0")
	t.Logf("输入: cluster-a 回滚只存在于 cluster-b 历史的 v3.0.0; 判定依据: NOT_FOUND, 实际 reason=%s", fleet.ReasonOf(err))
	if fleet.ReasonOf(err) != fleet.ReasonNotFound {
		t.Fatalf("cross-cluster version leak: reason=%s err=%v", fleet.ReasonOf(err), err)
	}
}
