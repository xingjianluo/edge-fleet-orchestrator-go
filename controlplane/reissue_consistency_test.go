package controlplane_test

import (
	"testing"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/fleet"
)

// 把一个与历史版本内容完全一致、但当前并非该版本的规格再 Deploy 一次：
// 必须是空操作——不切版本、不增历史、不占额度；
// 返回结果与设备当前实际生效的记录（历史末条）严格一致。
func TestReissueHistoricalContentKeepsReturnConsistentWithState(t *testing.T) {
	cp := setupFull(t)
	r1 := mustDeploy(t, cp, deploy("tenant-a", "cluster-1", "dev-1", "req-1", fullArtifact("v1.0.0")))
	r2 := mustDeploy(t, cp, deploy("tenant-a", "cluster-1", "dev-1", "req-2", fullArtifact("v1.1.0")))

	hist, _ := cp.History("tenant-a", "", "dev-1")
	nBefore, _ := cp.DeployCount("tenant-a")

	// 再下发 v1.0.0（带新键/无键都应一样）。
	r3, err := cp.Deploy(deploy("tenant-a", "cluster-1", "dev-1", "brand-new-key", fullArtifact("v1.0.0")))
	if err != nil {
		t.Fatalf("reissue: %v", err)
	}
	r4, err := cp.Deploy(deploy("tenant-a", "cluster-1", "dev-1", "", fullArtifact("v1.0.0")))
	if err != nil {
		t.Fatalf("reissue no-key: %v", err)
	}
	cur, _ := cp.CurrentVersion("tenant-a", "", "dev-1")
	histAfter, _ := cp.History("tenant-a", "", "dev-1")
	nAfter, _ := cp.DeployCount("tenant-a")

	t.Logf("输入: 当前 v1.1.0(Seq=%d) 时重复下发旧内容 v1.0.0(首次 Seq=%d) 两次(新键/无键); 判定依据: 空操作, 两次都返回当前生效 Seq=%d, 当前仍 %q, 历史 %d 条, 额度 %d",
		r2.Seq, r1.Seq, r2.Seq, cur, len(histAfter), nAfter)
	if cur != "v1.1.0" {
		t.Fatalf("current version must stay v1.1.0, got %q", cur)
	}
	for i, r := range []*fleet.DeploymentRecord{r3, r4} {
		if r.Seq != r2.Seq {
			t.Fatalf("return %d seq=%d, must equal current record %d", i+3, r.Seq, r2.Seq)
		}
		if r.Spec.Artifact.Version != cur {
			t.Fatalf("return %d describes %q but device is %q", i+3, r.Spec.Artifact.Version, cur)
		}
	}
	if len(histAfter) != len(hist) || nAfter != nBefore {
		t.Fatalf("side effects: history %d->%d count %d->%d", len(hist), len(histAfter), nBefore, nAfter)
	}
}

// 对同一目标重复回滚：只有第一次真正切换（计一次额度、增一条历史）；
// 之后无论重复回滚多少次，都返回设备当前实际生效记录，状态、历史、额度完全不变。
func TestRepeatedRollbackConsistent(t *testing.T) {
	cp := setupFull(t)
	mustDeploy(t, cp, deploy("tenant-a", "cluster-1", "dev-1", "a", fullArtifact("v1.0.0")))
	mustDeploy(t, cp, deploy("tenant-a", "cluster-1", "dev-1", "b", fullArtifact("v1.1.0")))
	mustDeploy(t, cp, deploy("tenant-a", "cluster-1", "dev-1", "c", fullArtifact("v2.0.0")))

	rb1, err := cp.Rollback("tenant-a", "cluster-1", "dev-1", "v1.0.0")
	if err != nil {
		t.Fatalf("rollback1: %v", err)
	}
	h1, _ := cp.History("tenant-a", "", "dev-1")
	n1, _ := cp.DeployCount("tenant-a")

	for i := 0; i < 5; i++ {
		rb, err := cp.Rollback("tenant-a", "cluster-1", "dev-1", "v1.0.0")
		if err != nil {
			t.Fatalf("repeat rollback %d: %v", i, err)
		}
		cur, _ := cp.CurrentVersion("tenant-a", "", "dev-1")
		h, _ := cp.History("tenant-a", "", "dev-1")
		n, _ := cp.DeployCount("tenant-a")
		if rb.Seq != rb1.Seq || rb.Spec.Artifact.Version != "v1.0.0" ||
			cur != "v1.0.0" || len(h) != len(h1) || n != n1 {
			t.Fatalf("repeat %d changed state: seq=%d/%d cur=%s hist=%d/%d count=%d/%d",
				i, rb.Seq, rb1.Seq, cur, len(h), len(h1), n, n1)
		}
	}
	t.Logf("输入: v1.0.0->v1.1.0->v2.0.0 后回滚到 v1.0.0 再重复回滚 5 次; 判定依据: 仅首次回滚生效(Seq=%d, 历史=%d, 额度=%d), 重复均返回当前生效记录且无副作用",
		rb1.Seq, len(h1), n1)

	// 回滚到另一个历史版本是真正切换：各自只计一次。
	rb2, err := cp.Rollback("tenant-a", "cluster-1", "dev-1", "v2.0.0")
	if err != nil {
		t.Fatalf("rollback2: %v", err)
	}
	cur2, _ := cp.CurrentVersion("tenant-a", "", "dev-1")
	h2, _ := cp.History("tenant-a", "", "dev-1")
	n2, _ := cp.DeployCount("tenant-a")
	if cur2 != "v2.0.0" || rb2.RollbackOf != "v2.0.0" || len(h2) != len(h1)+1 || n2 != n1+1 {
		t.Fatalf("second real rollback wrong: cur=%s rb=%+v hist=%d count=%d", cur2, rb2, len(h2), n2)
	}
	rb2dup, _ := cp.Rollback("tenant-a", "cluster-1", "dev-1", "v2.0.0")
	h3, _ := cp.History("tenant-a", "", "dev-1")
	n3, _ := cp.DeployCount("tenant-a")
	t.Logf("输入: 再回滚到 v2.0.0 并重复一次; 判定依据: 第二次切换生效 Seq=%d, 重复回滚返回 Seq=%d, 历史=%d 额度=%d",
		rb2.Seq, rb2dup.Seq, len(h3), n3)
	if rb2dup.Seq != rb2.Seq || len(h3) != len(h2) || n3 != n2 {
		t.Fatalf("repeat rollback2 not idempotent: seq=%d/%d hist=%d/%d count=%d/%d",
			rb2dup.Seq, rb2.Seq, len(h3), len(h2), n3, n2)
	}
}

// 失败请求（含目标版本不存在的回滚）不留任何历史/额度/版本副作用，
// 且回滚到不存在版本返回 NOT_FOUND。
func TestFailedRollbackNoSideEffects(t *testing.T) {
	cp := setupFull(t)
	mustDeploy(t, cp, deploy("tenant-a", "cluster-1", "dev-1", "a", fullArtifact("v1.0.0")))
	h0, _ := cp.History("tenant-a", "", "dev-1")
	n0, _ := cp.DeployCount("tenant-a")
	v0, _ := cp.CurrentVersion("tenant-a", "", "dev-1")

	_, err := cp.Rollback("tenant-a", "cluster-1", "dev-1", "v8.8.8")
	if fleet.ReasonOf(err) != fleet.ReasonNotFound {
		t.Fatalf("missing target reason=%s", fleet.ReasonOf(err))
	}
	h, _ := cp.History("tenant-a", "", "dev-1")
	n, _ := cp.DeployCount("tenant-a")
	v, _ := cp.CurrentVersion("tenant-a", "", "dev-1")
	t.Logf("输入: 回滚到从未出现的 v8.8.8; 判定依据: NOT_FOUND 且 历史=%d 版本=%q 额度=%d 保持不变", len(h), v, n)
	if len(h) != len(h0) || n != n0 || v != v0 {
		t.Fatalf("failed rollback had side effects: %d/%q/%d vs %d/%q/%d", len(h), v, n, len(h0), v0, n0)
	}
}
