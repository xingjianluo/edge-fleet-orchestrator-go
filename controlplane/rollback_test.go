package controlplane_test

import (
	"testing"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/controlplane"
	"github.com/xingjianluo/edge-fleet-orchestrator-go/fleet"
)

func deployVer(t *testing.T, cp *controlplane.ControlPlane, device, v string) *fleet.DeploymentRecord {
	t.Helper()
	r, err := cp.Deploy(specOf("tenant-a", "cluster-1", device, v, ""))
	if err != nil {
		t.Fatalf("deploy %s: %v", v, err)
	}
	return r
}

// 对同一目标版本重复回滚只生效一次：不新增历史、不多占额度、返回同一条记录。
func TestRollbackIdempotent(t *testing.T) {
	cp := setup(t)
	v1 := deployVer(t, cp, "dev-1", "v1.0.0")
	deployVer(t, cp, "dev-1", "v1.1.0")
	deployVer(t, cp, "dev-1", "v2.0.0")

	rb1, err := cp.Rollback("tenant-a", "cluster-1", "dev-1", "v1.0.0")
	if err != nil {
		t.Fatalf("rollback: %v", err)
	}
	if rb1.Seq == v1.Seq || rb1.RollbackOf != "v1.0.0" {
		t.Fatalf("first rollback should create rollback record, got %+v", rb1)
	}
	usedAfterFirst := cp.DeploymentCount("tenant-a")
	histAfterFirst, _ := cp.History("tenant-a", "cluster-1", "dev-1")
	if len(histAfterFirst) != 4 || usedAfterFirst != 4 {
		t.Fatalf("after first rollback: hist=%d used=%d, want 4/4", len(histAfterFirst), usedAfterFirst)
	}
	// 重复回滚到同一版本：原样返回当前生效记录。
	rb2, err := cp.Rollback("tenant-a", "cluster-1", "dev-1", "v1.0.0")
	if err != nil {
		t.Fatalf("second rollback: %v", err)
	}
	rb3, err := cp.Rollback("tenant-a", "cluster-1", "dev-1", "v1.0.0")
	if err != nil {
		t.Fatalf("third rollback: %v", err)
	}
	hist, _ := cp.History("tenant-a", "cluster-1", "dev-1")
	cur, _ := cp.CurrentVersion("tenant-a", "cluster-1", "dev-1")
	used := cp.DeploymentCount("tenant-a")
	t.Logf("输入: v1->v1.1->v2 后连续 3 次回滚 v1.0.0; 判定依据: 3 次返回同一 seq=%d, 历史=%d, 额度=%d, 当前=%s",
		rb1.Seq, len(hist), used, cur)
	if rb2.Seq != rb1.Seq || rb3.Seq != rb1.Seq {
		t.Fatalf("repeat rollback must return same seq %d, got %d,%d", rb1.Seq, rb2.Seq, rb3.Seq)
	}
	if len(hist) != 4 || used != 4 || cur != "v1.0.0" {
		t.Fatalf("repeat rollback grew state: hist=%d used=%d cur=%s", len(hist), used, cur)
	}
}

// 把与历史版本内容一致的规格再下发一次，不重复记录，且返回第一次生效的记录。
func TestRedeployHistoricalContentDedupes(t *testing.T) {
	cp := setup(t)
	v1 := deployVer(t, cp, "dev-1", "v1.0.0")
	deployVer(t, cp, "dev-1", "v2.0.0")
	// 当前是 v2；再次下发 v1 的完整规格（无键、换新键都一样）。
	r1, err := cp.Deploy(specOf("tenant-a", "cluster-1", "dev-1", "v1.0.0", ""))
	if err != nil {
		t.Fatalf("redeploy: %v", err)
	}
	r2, _ := cp.Deploy(specOf("tenant-a", "cluster-1", "dev-1", "v1.0.0", "retry-x"))
	hist, _ := cp.History("tenant-a", "cluster-1", "dev-1")
	cur, _ := cp.CurrentVersion("tenant-a", "cluster-1", "dev-1")
	used := cp.DeploymentCount("tenant-a")
	t.Logf("输入: v1->v2 后再下发 v1 规格两次（无键/换键）; 判定依据: 均返回首次 seq=%d, 历史=%d, 额度=%d, 当前=%s",
		v1.Seq, len(hist), used, cur)
	if r1.Seq != v1.Seq || r2.Seq != v1.Seq || len(hist) != 2 || used != 2 || cur != "v1.0.0" {
		t.Fatalf("redeploy historical content: seqs=%d,%d first=%d hist=%d used=%d cur=%s",
			r1.Seq, r2.Seq, v1.Seq, len(hist), used, cur)
	}
}

// 回滚前滚后再回滚同一目标：每次“内容真正发生切换”才记录，回到相同内容不记录。
func TestRollbackAlternating(t *testing.T) {
	cp := setup(t)
	deployVer(t, cp, "dev-1", "v1.0.0")
	deployVer(t, cp, "dev-1", "v2.0.0")
	rb1, _ := cp.Rollback("tenant-a", "cluster-1", "dev-1", "v1.0.0") // v2->v1: 新记录
	deployVer(t, cp, "dev-1", "v2.0.0")                               // v1->v2: 命中 v2 首次记录，不加历史
	rb2, _ := cp.Rollback("tenant-a", "cluster-1", "dev-1", "v1.0.0") // v2->v1: 内容切换 => 新回滚记录
	rb3, _ := cp.Rollback("tenant-a", "cluster-1", "dev-1", "v1.0.0") // 已在 v1: 幂等
	hist, _ := cp.History("tenant-a", "cluster-1", "dev-1")
	t.Logf("输入: v1->v2->rb1->v2(再下发)->rb2->rb3; 判定依据: rb1.seq=%d, rb2 为新记录, rb3==rb2, 历史=%d",
		rb1.Seq, len(hist))
	if rb2.Seq == rb1.Seq {
		t.Fatalf("rollback after real content switch must create a new record: %d", rb2.Seq)
	}
	if rb3.Seq != rb2.Seq {
		t.Fatalf("immediate repeat rollback must return %d, got %d", rb2.Seq, rb3.Seq)
	}
	if len(hist) != 4 { // v1,v2,rb1,rb2
		t.Fatalf("expected 4 history records, got %d", len(hist))
	}
}

// 回滚到历史中不存在的版本：NOT_FOUND，历史/当前/额度不变。
func TestRollbackUnknownVersion(t *testing.T) {
	cp := setup(t)
	deployVer(t, cp, "dev-1", "v1.0.0")
	before, _ := cp.History("tenant-a", "cluster-1", "dev-1")
	_, err := cp.Rollback("tenant-a", "cluster-1", "dev-1", "v9.9.9")
	if fleet.ReasonOf(err) != fleet.ReasonNotFound {
		t.Fatalf("want NOT_FOUND, got %v", err)
	}
	after, _ := cp.History("tenant-a", "cluster-1", "dev-1")
	cur, _ := cp.CurrentVersion("tenant-a", "cluster-1", "dev-1")
	t.Logf("输入: 回滚到从未部署的 v9.9.9; 判定依据: NOT_FOUND 且历史仍 %d 条当前 %s, 实际 %d 条 %s",
		len(before), "v1.0.0", len(after), cur)
	if len(after) != len(before) || cur != "v1.0.0" {
		t.Fatalf("failed rollback mutated state: hist %d->%d cur=%s", len(before), len(after), cur)
	}
}
