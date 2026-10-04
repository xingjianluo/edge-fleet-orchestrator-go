package controlplane_test

import (
	"fmt"
	"testing"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/controlplane"
	"github.com/xingjianluo/edge-fleet-orchestrator-go/fleet"
)

func mustDeploy(t *testing.T, cp *controlplane.ControlPlane, s fleet.DeploymentSpec) *fleet.DeploymentRecord {
	t.Helper()
	r, err := cp.Deploy(s)
	if err != nil {
		t.Fatalf("deploy %s: %v", s.Artifact.Version, err)
	}
	return r
}

// 重复回滚到同一历史版本只生效一次；再下发与历史版本一致的内容也不重复记录。
func TestRollbackIdempotent(t *testing.T) {
	cp := setupFull(t)
	for i, v := range []string{"v1.0.0", "v1.1.0", "v2.0.0"} {
		mustDeploy(t, cp, deploy("tenant-a", "cluster-1", "dev-1", fmt.Sprintf("req-%d", i), fullArtifact(v)))
	}
	hist, _ := cp.History("tenant-a", "", "dev-1")
	if len(hist) != 3 {
		t.Fatalf("setup history=%d", len(hist))
	}
	countBefore, _ := cp.DeployCount("tenant-a")

	// 第一次回滚 v1.0.0：真正切换版本，追加一条 RollbackOf 记录。
	rb1, err := cp.Rollback("tenant-a", "cluster-1", "dev-1", "v1.0.0")
	if err != nil {
		t.Fatalf("rollback1: %v", err)
	}
	cur, _ := cp.CurrentVersion("tenant-a", "", "dev-1")
	if cur != "v1.0.0" || rb1.RollbackOf != "v1.0.0" {
		t.Fatalf("rollback1 cur=%s rb=%+v", cur, rb1)
	}
	hist, _ = cp.History("tenant-a", "", "dev-1")
	countAfter1, _ := cp.DeployCount("tenant-a")
	t.Logf("输入: 首次回滚到 v1.0.0; 判定依据: 历史 %d->%d, 额度 %d->%d, Seq=%d",
		3, len(hist), countBefore, countAfter1, rb1.Seq)
	if len(hist) != 4 || countAfter1 != countBefore+1 {
		t.Fatalf("first rollback must append+charge: hist=%d count=%d", len(hist), countAfter1)
	}

	// 第二次回滚同一目标：幂等，返回该内容第一次生效的记录（Seq=1，首次部署那条），
	// 不增长历史、不增加额度、不改当前版本。
	rb2, err := cp.Rollback("tenant-a", "cluster-1", "dev-1", "v1.0.0")
	if err != nil {
		t.Fatalf("rollback2: %v", err)
	}
	hist, _ = cp.History("tenant-a", "", "dev-1")
	countAfter2, _ := cp.DeployCount("tenant-a")
	cur2, _ := cp.CurrentVersion("tenant-a", "", "dev-1")
	t.Logf("输入: 再次回滚 v1.0.0; 判定依据: 返回内容首次生效 Seq=1, 实际 Seq=%d, 历史=%d 额度=%d 当前=%q",
		rb2.Seq, len(hist), countAfter2, cur2)
	if rb2.Seq != 1 || len(hist) != 4 || countAfter2 != countAfter1 || cur2 != "v1.0.0" {
		t.Fatalf("repeat rollback not idempotent: seq=%d hist=%d count=%d cur=%s", rb2.Seq, len(hist), countAfter2, cur2)
	}

	// 用与历史版本内容一致的规格再下发一次：同样不重复记录、不占额度，
	// 且返回该内容第一次生效的记录（Seq=1，首次部署 v1.0.0 的那条）。
	r3, err := cp.Deploy(deploy("tenant-a", "cluster-1", "dev-1", "brand-new-key", fullArtifact("v1.0.0")))
	if err != nil {
		t.Fatalf("reissue: %v", err)
	}
	hist, _ = cp.History("tenant-a", "", "dev-1")
	countAfter3, _ := cp.DeployCount("tenant-a")
	t.Logf("输入: 回滚后再下发 v1.0.0 同内容规格; 判定依据: 返回首次生效 Seq=1, 实际 Seq=%d, 历史=%d 额度=%d",
		r3.Seq, len(hist), countAfter3)
	if r3.Seq != 1 || len(hist) != 4 || countAfter3 != countAfter1 {
		t.Fatalf("reissue duplicated: seq=%d hist=%d count=%d", r3.Seq, len(hist), countAfter3)
	}

	// 回滚到从未出现过的版本：NOT_FOUND，且无副作用。
	before := len(hist)
	_, err = cp.Rollback("tenant-a", "cluster-1", "dev-1", "v9.9.9")
	if fleet.ReasonOf(err) != fleet.ReasonNotFound {
		t.Fatalf("missing version reason=%s", fleet.ReasonOf(err))
	}
	hist, _ = cp.History("tenant-a", "", "dev-1")
	if len(hist) != before {
		t.Fatalf("failed rollback changed history: %d -> %d", before, len(hist))
	}
}

// 所有失败路径（版本非法/不兼容/无权限/超配额）都不得改动历史、当前版本、额度，
// 且失败原因可区分；重复内容在配额已满时仍可生效。
func TestRejectedRequestsHaveNoSideEffects(t *testing.T) {
	cp := controlplane.New()
	cp.SetQuota("tenant-a", Quota11())
	if err := cp.RegisterDevice("tenant-a", "cluster-1", "dev-1", fullCaps); err != nil {
		t.Fatalf("register: %v", err)
	}
	// 唯一一次合法部署，额度用尽。
	mustDeploy(t, cp, deploy("tenant-a", "cluster-1", "dev-1", "ok", fullArtifact("v1.0.0")))
	snapshot := func() (int, string, int) {
		h, _ := cp.History("tenant-a", "", "dev-1")
		cur, _ := cp.CurrentVersion("tenant-a", "", "dev-1")
		n, _ := cp.DeployCount("tenant-a")
		return len(h), cur, n
	}
	h0, v0, n0 := snapshot()

	reject := func(name string, s fleet.DeploymentSpec, want fleet.Reason) {
		t.Helper()
		_, err := cp.Deploy(s)
		got := fleet.ReasonOf(err)
		h, v, n := snapshot()
		t.Logf("输入[%s]: %v; 判定依据: reason=%s 历史=%d 版本=%q 额度=%d (应保持 %d/%q/%d)",
			name, s.Artifact, got, h, v, n, h0, v0, n0)
		if got != want {
			t.Fatalf("%s: reason=%s want %s (err=%v)", name, got, want, err)
		}
		if h != h0 || v != v0 || n != n0 {
			t.Fatalf("%s mutated state: %d/%q/%d -> %d/%q/%d", name, h0, v0, n0, h, v, n)
		}
	}

	badVersion := fullArtifact("v1.0.0")
	badVersion.Version = "1.0"
	reject("invalid version", deploy("tenant-a", "cluster-1", "dev-1", "k1", badVersion), fleet.ReasonInvalidVersion)

	incompat := fullArtifact("v1.0.1")
	incompat.Arch = "amd64"
	reject("incompatible", deploy("tenant-a", "cluster-1", "dev-1", "k2", incompat), fleet.ReasonIncompatible)

	// 超配额：内容是全新的 v1.0.1（兼容、合法），但额度已满。
	reject("quota exceeded", deploy("tenant-a", "cluster-1", "dev-1", "k3", fullArtifact("v1.0.1")), fleet.ReasonQuotaExceeded)

	// 跨租户：FORBIDDEN，与 NOT_FOUND 区分，且无副作用。
	cp.SetQuota("tenant-b", Quota11())
	reject("forbidden", func() fleet.DeploymentSpec {
		s := deploy("tenant-b", "cluster-1", "dev-1", "k4", fullArtifact("v1.0.1"))
		return s
	}(), fleet.ReasonForbidden)
	// 资源不存在。
	reject("not found", deploy("tenant-a", "cluster-1", "ghost", "k5", fullArtifact("v1.0.1")), fleet.ReasonNotFound)

	// 配额已满，但重复下发已生效内容：必须仍然成功（幂等不消耗额度）。
	r, err := cp.Deploy(deploy("tenant-a", "cluster-1", "dev-1", "retried-key", fullArtifact("v1.0.0")))
	if err != nil || r.Seq != 1 {
		t.Fatalf("duplicate under exhausted quota: seq=%d err=%v", r.Seq, err)
	}
	if h, v, n := snapshot(); h != h0 || v != v0 || n != n0 {
		t.Fatalf("idempotent deploy changed state: %d/%q/%d", h, v, n)
	}
	t.Logf("输入: 配额耗尽后重复下发 v1.0.0; 判定依据: 成功返回 Seq=%d 且历史/额度不变", r.Seq)

	// 回滚也遵守配额边界：额度满时重复回滚当前版本不占额度，必须成功且返回首次记录。
	rb, err := cp.Rollback("tenant-a", "cluster-1", "dev-1", "v1.0.0")
	if err != nil || rb.Seq != 1 {
		t.Fatalf("idempotent rollback under exhausted quota: %+v %v", rb, err)
	}
	t.Logf("输入: 额度耗尽时重复回滚当前 v1.0.0; 判定依据: 成功返回 Seq=%d 且无副作用", rb.Seq)

	// 回滚到一个与当前不同的版本才是“真正变化”：额度耗尽时必须被拒绝。
	// 先造历史：提高额度部署 v1.0.1，再压回额度 1。
	cp.SetQuota("tenant-a", controlplane.Quota{MaxDevices: 1, MaxDeployments: 10})
	if _, err := cp.Deploy(deploy("tenant-a", "cluster-1", "dev-1", "more", fullArtifact("v1.0.1"))); err != nil {
		t.Fatalf("deploy v1.0.1: %v", err)
	}
	cp.SetQuota("tenant-a", controlplane.Quota{MaxDevices: 1, MaxDeployments: 1})
	_, err = cp.Rollback("tenant-a", "cluster-1", "dev-1", "v1.0.0")
	if fleet.ReasonOf(err) != fleet.ReasonQuotaExceeded {
		t.Fatalf("rollback changing version reason=%s err=%v", fleet.ReasonOf(err), err)
	}
	h, v, n := snapshot()
	t.Logf("输入: 额度耗尽时回滚到不同版本 v1.0.0; 判定依据: QUOTA_EXCEEDED 且状态保持 %d/%q/%d, 实际 %d/%q/%d",
		h0, v0, n0, h, v, n)
	// 注意当前版本此刻是 v1.0.1（成功部署所致），仅校验失败回滚未把版本切走、未新增历史。
	cur, _ := cp.CurrentVersion("tenant-a", "", "dev-1")
	hh, _ := cp.History("tenant-a", "", "dev-1")
	if cur != "v1.0.1" || len(hh) != 2 {
		t.Fatalf("rejected rollback mutated state: cur=%s hist=%d", cur, len(hh))
	}
}

func Quota11() controlplane.Quota { return controlplane.Quota{MaxDevices: 1, MaxDeployments: 1} }
