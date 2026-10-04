package controlplane_test

import (
	"testing"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/controlplane"
	"github.com/xingjianluo/edge-fleet-orchestrator-go/fleet"
)

// 同租户不同集群各有一台同名设备：回滚必须能按 (租户, 集群, 设备) 分别指定，
// 只影响目标集群那一台；另一集群同名设备保持原样。
func TestRollbackSameNameDevicePerCluster(t *testing.T) {
	cp := controlplane.New()
	cp.SetQuota("tenant-a", controlplane.Quota{MaxDevices: 100, MaxDeployments: 1000})
	if err := cp.RegisterDevice("tenant-a", "cluster-1", "dev-x", fullCaps); err != nil {
		t.Fatalf("register c1: %v", err)
	}
	if err := cp.RegisterDevice("tenant-a", "cluster-2", "dev-x", fullCaps); err != nil {
		t.Fatalf("register c2: %v", err)
	}
	art := func(v string) fleet.Artifact { return fullArtifact(v) }
	spec := func(cluster, v string) fleet.DeploymentSpec {
		return deploy("tenant-a", cluster, "dev-x", "k-"+cluster+"-"+v, art(v))
	}
	// 两台同名设备各自走过 v1.0.0 -> v1.1.0。
	for _, c := range []string{"cluster-1", "cluster-2"} {
		mustDeploy(t, cp, spec(c, "v1.0.0"))
		mustDeploy(t, cp, spec(c, "v1.1.0"))
	}

	// 只对 cluster-1 的同名设备回滚到 v1.0.0。
	rb, err := cp.Rollback("tenant-a", "cluster-1", "dev-x", "v1.0.0")
	if err != nil {
		t.Fatalf("rollback c1: %v", err)
	}
	cur1, _ := cp.CurrentVersion("tenant-a", "cluster-1", "dev-x")
	cur2, _ := cp.CurrentVersion("tenant-a", "cluster-2", "dev-x")
	t.Logf("输入: 同名设备 dev-x 分布在 cluster-1/2 且都在 v1.1.0, 仅回滚 cluster-1 到 v1.0.0; 判定依据: c1=v1.0.0 c2 保持 v1.1.0, 回滚记录 Seq=%d RollbackOf=%s",
		rb.Seq, rb.RollbackOf)
	if cur1 != "v1.0.0" || rb.RollbackOf != "v1.0.0" {
		t.Fatalf("cluster-1 not rolled back: cur=%s rb=%+v", cur1, rb)
	}
	if cur2 != "v1.1.0" {
		t.Fatalf("cluster-2 same-name device must stay v1.1.0, got %q", cur2)
	}

	// 再对 cluster-2 回滚：互不影响，各自产生自己的回滚记录。
	rb2, err := cp.Rollback("tenant-a", "cluster-2", "dev-x", "v1.0.0")
	if err != nil {
		t.Fatalf("rollback c2: %v", err)
	}
	cur2, _ = cp.CurrentVersion("tenant-a", "cluster-2", "dev-x")
	if cur2 != "v1.0.0" || rb2.Seq == rb.Seq {
		t.Fatalf("cluster-2 rollback isolated: cur=%s seq=%d vs %d", cur2, rb2.Seq, rb.Seq)
	}

	// 对 cluster-1 重复回滚同一目标：只生效一次，不多历史、不多额度。
	h1, _ := cp.History("tenant-a", "cluster-1", "dev-x")
	nBefore, _ := cp.DeployCount("tenant-a")
	rbDup, err := cp.Rollback("tenant-a", "cluster-1", "dev-x", "v1.0.0")
	if err != nil {
		t.Fatalf("dup rollback c1: %v", err)
	}
	h1After, _ := cp.History("tenant-a", "cluster-1", "dev-x")
	nAfter, _ := cp.DeployCount("tenant-a")
	t.Logf("输入: 对 cluster-1 重复回滚 v1.0.0; 判定依据: 返回当前生效 Seq=%d, 历史 %d 条不变, 额度 %d->%d",
		rbDup.Seq, len(h1), nBefore, nAfter)
	if rbDup.Seq != rb.Seq || len(h1After) != len(h1) || nAfter != nBefore {
		t.Fatalf("repeat rollback must be idempotent: seq=%d/%d hist=%d/%d count=%d/%d",
			rbDup.Seq, rb.Seq, len(h1After), len(h1), nAfter, nBefore)
	}

	// 不指定集群时，同名设备有歧义：返回 NOT_FOUND（提示需指定集群），不能误伤任一台。
	_, err = cp.Rollback("tenant-a", "", "dev-x", "v1.0.0")
	if fleet.ReasonOf(err) != fleet.ReasonNotFound {
		t.Fatalf("ambiguous rollback reason=%s err=%v", fleet.ReasonOf(err), err)
	}
	cur1b, _ := cp.CurrentVersion("tenant-a", "cluster-1", "dev-x")
	cur2b, _ := cp.CurrentVersion("tenant-a", "cluster-2", "dev-x")
	if cur1b != "v1.0.0" || cur2b != "v1.0.0" {
		t.Fatalf("ambiguous rollback must have no effect: %s/%s", cur1b, cur2b)
	}
	t.Logf("输入: 不指定集群回滚同名设备; 判定依据: NOT_FOUND(需指定集群) 且两台状态不变 c1=%q c2=%q", cur1b, cur2b)
}
