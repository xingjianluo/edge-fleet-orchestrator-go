package controlplane_test

import (
	"fmt"
	"testing"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/fleet"
)

// 历史内容重复下发：不追加历史、不占额度，且返回的记录必须与设备当前实际生效内容一致
// （重复提交 = 再次声明同一期望状态，设备当前版本随之对齐）。
func TestRedeployHistoricalContentAlignsCurrentState(t *testing.T) {
	cp := setupFull(t)
	r1 := mustDeploy(t, cp, deploy("tenant-a", "cluster-1", "dev-1", "req-1", fullArtifact("v1.0.0")))
	r2 := mustDeploy(t, cp, deploy("tenant-a", "cluster-1", "dev-1", "req-2", fullArtifact("v2.0.0")))
	countBefore, _ := cp.DeployCount("tenant-a")
	t.Logf("业务: 历史内容重复下发的状态一致性; 输入: 历史 v1.0.0(Seq=%d)->v2.0.0(Seq=%d), 当前 v2.0.0", r1.Seq, r2.Seq)

	// 再提交与 v1.0.0 内容完全一致的下发（换一个新幂等键）。
	r3, err := cp.Deploy(deploy("tenant-a", "cluster-1", "dev-1", "req-3", fullArtifact("v1.0.0")))
	if err != nil {
		t.Fatalf("redeliver v1.0.0: %v", err)
	}
	cur, _ := cp.CurrentVersion("tenant-a", "cluster-1", "dev-1")
	hist, _ := cp.History("tenant-a", "cluster-1", "dev-1")
	count, _ := cp.DeployCount("tenant-a")
	t.Logf("判定依据: 返回 v1.0.0 首次生效记录 Seq=%d, 当前版本随之为 v1.0.0, 历史=2 额度=%d", r1.Seq, countBefore)
	t.Logf("结果: 返回 Seq=%d 版本=%q, 当前=%q, 历史=%d 条, 额度=%d", r3.Seq, r3.Spec.Artifact.Version, cur, len(hist), count)
	if r3.Seq != r1.Seq {
		t.Fatalf("redelivery returned seq %d, want first-effective %d", r3.Seq, r1.Seq)
	}
	if cur != r3.Spec.Artifact.Version {
		t.Fatalf("returned record %q inconsistent with current version %q", r3.Spec.Artifact.Version, cur)
	}
	if len(hist) != 2 || count != countBefore {
		t.Fatalf("redelivery side effects: hist=%d count=%d, want 2/%d", len(hist), count, countBefore)
	}

	// 再提交 v2.0.0 内容：当前版本对齐回 v2.0.0，仍无任何新增。
	r4, err := cp.Deploy(deploy("tenant-a", "cluster-1", "dev-1", "req-4", fullArtifact("v2.0.0")))
	if err != nil {
		t.Fatalf("redeliver v2.0.0: %v", err)
	}
	cur, _ = cp.CurrentVersion("tenant-a", "cluster-1", "dev-1")
	hist, _ = cp.History("tenant-a", "cluster-1", "dev-1")
	count, _ = cp.DeployCount("tenant-a")
	t.Logf("输入: 再下发 v2.0.0 同内容; 判定依据: 返回 Seq=%d 且当前=%q, 历史/额度不变", r2.Seq, r4.Spec.Artifact.Version)
	if r4.Seq != r2.Seq || cur != "v2.0.0" || len(hist) != 2 || count != countBefore {
		t.Fatalf("redeliver v2.0.0: seq=%d cur=%q hist=%d count=%d", r4.Seq, cur, len(hist), count)
	}

	// 同幂等键的旧请求重试（层 1 命中）同样对齐当前状态。
	if _, err := cp.Deploy(deploy("tenant-a", "cluster-1", "dev-1", "req-1", fullArtifact("v1.0.0"))); err != nil {
		t.Fatalf("retry old key: %v", err)
	}
	cur, _ = cp.CurrentVersion("tenant-a", "cluster-1", "dev-1")
	t.Logf("输入: 用首次请求的幂等键 req-1 重试 v1.0.0; 判定依据: 当前版本对齐为 v1.0.0, 实际 %q", cur)
	if cur != "v1.0.0" {
		t.Fatalf("idempotent-key retry did not align current version: %q", cur)
	}
}

// 重复回滚同一目标保持同样语义：第一次真实切换并计一次额度；
// 之后每次重复都返回与当前生效内容一致的记录，不多历史、不占额度；
// 与重复下发交错进行时语义不变。
func TestRepeatedRollbackKeepsConsistentState(t *testing.T) {
	cp := setupFull(t)
	recs := map[string]*fleet.DeploymentRecord{}
	for i, v := range []string{"v1.0.0", "v1.1.0", "v2.0.0"} {
		recs[v] = mustDeploy(t, cp, deploy("tenant-a", "cluster-1", "dev-1", fmt.Sprintf("req-%d", i), fullArtifact(v)))
	}
	count0, _ := cp.DeployCount("tenant-a")
	t.Logf("业务: 重复回滚的状态一致性; 输入: 历史 v1.0.0->v1.1.0->v2.0.0, 当前 v2.0.0, 额度=%d", count0)

	// 第一次回滚到 v1.1.0：真实切换。
	rb1, err := cp.Rollback("tenant-a", "cluster-1", "dev-1", "v1.1.0")
	if err != nil {
		t.Fatalf("rollback1: %v", err)
	}
	cur, _ := cp.CurrentVersion("tenant-a", "cluster-1", "dev-1")
	count1, _ := cp.DeployCount("tenant-a")
	t.Logf("判定依据: 首次回滚追加 1 条 RollbackOf 记录并计 1 次额度; 结果: 当前=%q 额度 %d->%d", cur, count0, count1)
	if cur != "v1.1.0" || rb1.RollbackOf != "v1.1.0" || count1 != count0+1 {
		t.Fatalf("first rollback: cur=%q rb=%+v count=%d", cur, rb1, count1)
	}

	// 重复回滚同一目标 3 次：每次都返回与当前生效内容一致的记录，历史/额度不变。
	for i := 0; i < 3; i++ {
		rb, err := cp.Rollback("tenant-a", "cluster-1", "dev-1", "v1.1.0")
		if err != nil {
			t.Fatalf("repeat rollback %d: %v", i, err)
		}
		cur, _ := cp.CurrentVersion("tenant-a", "cluster-1", "dev-1")
		hist, _ := cp.History("tenant-a", "cluster-1", "dev-1")
		count, _ := cp.DeployCount("tenant-a")
		if rb.Spec.Artifact.Version != cur || cur != "v1.1.0" {
			t.Fatalf("repeat %d: returned %q but current %q", i, rb.Spec.Artifact.Version, cur)
		}
		if len(hist) != 4 || count != count1 {
			t.Fatalf("repeat %d side effects: hist=%d count=%d, want 4/%d", i, len(hist), count, count1)
		}
	}
	t.Logf("结果: 3 次重复回滚均返回 v1.1.0 生效记录, 历史恒为 4 条, 额度恒为 %d", count1)

	// 交错：重复下发当前内容（v1.1.0）再回滚 v1.1.0，语义不变。
	if _, err := cp.Deploy(deploy("tenant-a", "cluster-1", "dev-1", "req-x", fullArtifact("v1.1.0"))); err != nil {
		t.Fatalf("redeliver current: %v", err)
	}
	rb, err := cp.Rollback("tenant-a", "cluster-1", "dev-1", "v1.1.0")
	if err != nil {
		t.Fatalf("rollback after redeliver: %v", err)
	}
	cur, _ = cp.CurrentVersion("tenant-a", "cluster-1", "dev-1")
	hist, _ := cp.History("tenant-a", "cluster-1", "dev-1")
	count, _ := cp.DeployCount("tenant-a")
	t.Logf("输入: 重复下发 v1.1.0 后再回滚 v1.1.0; 判定依据: 返回记录版本=当前版本=%q, 历史=4 额度=%d", cur, count1)
	if rb.Spec.Artifact.Version != cur || len(hist) != 4 || count != count1 {
		t.Fatalf("interleaved semantics broken: rb=%q cur=%q hist=%d count=%d",
			rb.Spec.Artifact.Version, cur, len(hist), count)
	}

	// 回滚到另一历史版本 v1.0.0 仍是真实变化；之后对该目标重复回滚重新幂等。
	if _, err := cp.Rollback("tenant-a", "cluster-1", "dev-1", "v1.0.0"); err != nil {
		t.Fatalf("rollback v1.0.0: %v", err)
	}
	rbAgain, err := cp.Rollback("tenant-a", "cluster-1", "dev-1", "v1.0.0")
	if err != nil {
		t.Fatalf("repeat rollback v1.0.0: %v", err)
	}
	cur, _ = cp.CurrentVersion("tenant-a", "cluster-1", "dev-1")
	hist, _ = cp.History("tenant-a", "cluster-1", "dev-1")
	count, _ = cp.DeployCount("tenant-a")
	t.Logf("输入: 回滚 v1.0.0 并重复一次; 判定依据: 首次计额度, 重复返回 Seq=%d(v1.0.0 首次生效) 且历史=5 额度=%d",
		recs["v1.0.0"].Seq, count1+1)
	if rbAgain.Seq != recs["v1.0.0"].Seq || cur != "v1.0.0" || len(hist) != 5 || count != count1+1 {
		t.Fatalf("rollback v1.0.0 semantics: seq=%d cur=%q hist=%d count=%d",
			rbAgain.Seq, cur, len(hist), count)
	}
}
