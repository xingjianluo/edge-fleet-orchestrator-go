package controlplane_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/controlplane"
	"github.com/xingjianluo/edge-fleet-orchestrator-go/fleet"
)

func specOf(tenant, cluster, device, version string, key string) fleet.DeploymentSpec {
	return fleet.DeploymentSpec{
		TenantID:       tenant,
		ClusterID:      cluster,
		DeviceID:       device,
		Artifact:       artifact(version),
		IdempotencyKey: key,
	}
}

func featSpecFor(device, version string, features ...string) fleet.DeploymentSpec {
	a := artifact(version)
	a.RequireFeature = features
	return fleet.DeploymentSpec{TenantID: "tenant-a", ClusterID: "cluster-1", DeviceID: device, Artifact: a}
}

// 内容级幂等：不带标识、带标识、重试换标识的重复提交都只生效一次。
func TestContentIdempotencyAcrossKeys(t *testing.T) {
	cp := setup(t)
	first, err := cp.Deploy(specOf("tenant-a", "cluster-1", "dev-1", "v1.0.0", "")) // 无键
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	attempts := []string{"", "", "retry-1", "retry-2", "another-key"}
	for i, key := range attempts {
		r, err := cp.Deploy(specOf("tenant-a", "cluster-1", "dev-1", "v1.0.0", key))
		if err != nil {
			t.Fatalf("attempt %d key=%q: %v", i, key, err)
		}
		if r.Seq != first.Seq {
			t.Fatalf("attempt %d key=%q got seq %d, want first seq %d", i, key, r.Seq, first.Seq)
		}
	}
	hist, _ := cp.History("tenant-a", "cluster-1", "dev-1")
	t.Logf("输入: 同一内容 v1.0.0 提交 6 次（无键/换键）; 判定依据: 全部返回首次 seq=%d, 历史仅 1 条, 实际 %d 条",
		first.Seq, len(hist))
	if len(hist) != 1 {
		t.Fatalf("history grew to %d", len(hist))
	}
}

// 只有版本或制品属性真正变化才算新部署。
func TestContentChangeIsNewDeployment(t *testing.T) {
	cp := setup(t)
	devCaps := fleet.Capabilities{Arch: "arm64", OS: "linux", MaxArtifact: 1 << 20, Features: []string{"gpu", "npu"}}
	if err := cp.RegisterDevice("tenant-a", "cluster-1", "dev-feat", devCaps); err != nil {
		t.Fatal(err)
	}
	r1, _ := cp.Deploy(featSpecFor("dev-feat", "v1.0.0"))
	r2, _ := cp.Deploy(featSpecFor("dev-feat", "v1.0.1")) // 版本变了
	if r2.Seq == r1.Seq {
		t.Fatalf("version change must produce new record")
	}
	// 给制品增加依赖特性：属性变化 => 新内容 => 新记录。
	sA := featSpecFor("dev-feat", "v1.0.1", "gpu", "npu")
	// 仅特性书写顺序不同但集合相同 => 内容一致 => 不新增。
	sB := featSpecFor("dev-feat", "v1.0.1", "npu", "gpu")
	r3, err := cp.Deploy(sA)
	if err != nil {
		t.Fatalf("deploy features: %v", err)
	}
	r4, _ := cp.Deploy(sB)
	hist, _ := cp.History("tenant-a", "cluster-1", "dev-feat")
	t.Logf("输入: v1.0.0 -> v1.0.1 -> v1.0.1[gpu,npu] -> v1.0.1[npu,gpu]; 判定依据: 属性新增后历史 3 条, 纯换序不再新增, 实际 %d 条 r3=%d r4=%d", len(hist), r3.Seq, r4.Seq)
	if len(hist) != 3 || r3.Seq != r4.Seq {
		t.Fatalf("feature set reorder should dedupe: hist=%d seqs=%d,%d", len(hist), r3.Seq, r4.Seq)
	}
	// 制品大小变化 => 内容变了 => 新部署。
	big := specOf("tenant-a", "cluster-1", "dev-feat", "v1.0.1", "")
	big.Artifact.SizeBytes = 2048
	r5, _ := cp.Deploy(big)
	if r5.Seq == r4.Seq {
		t.Fatalf("artifact size change must produce new record")
	}
	hist, _ = cp.History("tenant-a", "cluster-1", "dev-feat")
	if len(hist) != 4 {
		t.Fatalf("expected 4 records after real content changes, got %d", len(hist))
	}
	// 回到曾经生效过的内容：仍返回第一次生效记录，不新增。
	back, _ := cp.Deploy(specOf("tenant-a", "cluster-1", "dev-feat", "v1.0.0", "round-trip"))
	if back.Seq != r1.Seq {
		t.Fatalf("re-deploy of prior content must return first seq %d, got %d", r1.Seq, back.Seq)
	}
	hist, _ = cp.History("tenant-a", "cluster-1", "dev-feat")
	cur, _ := cp.CurrentVersion("tenant-a", "cluster-1", "dev-feat")
	t.Logf("输入: 再下发曾生效的 v1.0.0; 判定依据: 返回首次 seq=%d、历史仍为 4 条、当前版本回到 v1.0.0; 实际 hist=%d cur=%s",
		r1.Seq, len(hist), cur)
	if len(hist) != 4 || cur != "v1.0.0" {
		t.Fatalf("round-trip dedupe failed: hist=%d cur=%s", len(hist), cur)
	}
}

// 作用域：不同租户的设备、同租户不同集群的同名设备，即使规格完全相同也各自独立生效。
func TestIdempotencyScopedByTenantAndCluster(t *testing.T) {
	cp := controlplane.New()
	for _, tn := range []string{"tenant-a", "tenant-b"} {
		cp.SetQuota(tn, controlplane.Quota{MaxDevices: 100, MaxDeployments: 100})
	}
	// 同租户、不同集群下的同名设备。
	if err := cp.RegisterDevice("tenant-a", "cluster-1", "same-dev", caps); err != nil {
		t.Fatal(err)
	}
	if err := cp.RegisterDevice("tenant-a", "cluster-2", "same-dev", caps); err != nil {
		t.Fatal(err)
	}
	// 另一租户下的设备（设备标识全局唯一，属于不同租户）。
	if err := cp.RegisterDevice("tenant-b", "cluster-1", "b-dev", caps); err != nil {
		t.Fatal(err)
	}
	rA1, _ := cp.Deploy(specOf("tenant-a", "cluster-1", "same-dev", "v1.0.0", ""))
	rA2, _ := cp.Deploy(specOf("tenant-a", "cluster-2", "same-dev", "v1.0.0", ""))
	rB, err := cp.Deploy(specOf("tenant-b", "cluster-1", "b-dev", "v1.0.0", ""))
	if err != nil {
		t.Fatalf("tenant-b deploy: %v", err)
	}
	seqs := []int64{rA1.Seq, rA2.Seq, rB.Seq}
	t.Logf("输入: 相同内容分别下发到同租户不同集群的同名设备与另一租户设备; 判定依据: 三条记录 Seq 两两不同, 实际 %v", seqs)
	if rA1.Seq == rA2.Seq || rA1.Seq == rB.Seq || rA2.Seq == rB.Seq {
		t.Fatalf("deployments must be independent across tenant/cluster scope: %v", seqs)
	}
	// 各自范围内重复下发仍然只命中自己的首次记录。
	againA1, _ := cp.Deploy(specOf("tenant-a", "cluster-1", "same-dev", "v1.0.0", "x"))
	againA2, _ := cp.Deploy(specOf("tenant-a", "cluster-2", "same-dev", "v1.0.0", "x"))
	if againA1.Seq != rA1.Seq || againA2.Seq != rA2.Seq {
		t.Fatalf("within-scope dedupe broken: %d,%d vs %d,%d", againA1.Seq, againA2.Seq, rA1.Seq, rA2.Seq)
	}
	h1, _ := cp.History("tenant-a", "cluster-1", "same-dev")
	h2, _ := cp.History("tenant-a", "cluster-2", "same-dev")
	if len(h1) != 1 || len(h2) != 1 {
		t.Fatalf("each scope must have exactly 1 history, got %d,%d", len(h1), len(h2))
	}
	// 设备在本租户存在但在别的集群：该集群视角为 NOT_FOUND（不与别的集群互相顶掉）。
	_, err = cp.History("tenant-a", "cluster-9", "same-dev")
	if fleet.ReasonOf(err) != fleet.ReasonNotFound {
		t.Fatalf("device in another cluster of same tenant must read NOT_FOUND, got %v", err)
	}
	// 资源不存在：NOT_FOUND。
	_, err = cp.History("tenant-b", "cluster-1", "ghost-dev")
	if fleet.ReasonOf(err) != fleet.ReasonNotFound {
		t.Fatalf("missing resource must be NOT_FOUND, got %v", err)
	}
	// 跨租户访问属于别人的设备：FORBIDDEN，且与 NOT_FOUND 可区分。
	_, err = cp.History("tenant-b", "cluster-1", "same-dev")
	t.Logf("输入: tenant-b 访问属于 tenant-a 的设备 same-dev; 判定依据: FORBIDDEN, 实际 reason=%s", fleet.ReasonOf(err))
	if fleet.ReasonOf(err) != fleet.ReasonForbidden {
		t.Fatalf("cross-tenant must be FORBIDDEN, got %v", err)
	}
	_, err = cp.Deploy(specOf("tenant-a", "cluster-1", "b-dev", "v1.0.0", ""))
	if fleet.ReasonOf(err) != fleet.ReasonForbidden {
		t.Fatalf("cross-tenant deploy must be FORBIDDEN, got %v", err)
	}
}

// 并发：相同内容从多路并发提交（无键/换键），只能有一次真正生效。
func TestConcurrentContentIdempotency(t *testing.T) {
	cp := setup(t)
	const n = 40
	var wg sync.WaitGroup
	out := make([]*fleet.DeploymentRecord, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := ""
			if i%3 == 0 {
				key = fmt.Sprintf("changed-on-retry-%d", i)
			}
			r, err := cp.Deploy(specOf("tenant-a", "cluster-1", "dev-1", "v1.0.0", key))
			if err != nil {
				t.Errorf("deploy %d: %v", i, err)
				return
			}
			out[i] = r
		}(i)
	}
	wg.Wait()
	for i, r := range out {
		if r.Seq != out[0].Seq {
			t.Fatalf("deploy %d seq %d != first seq %d", i, r.Seq, out[0].Seq)
		}
	}
	hist, _ := cp.History("tenant-a", "cluster-1", "dev-1")
	if len(hist) != 1 {
		t.Fatalf("expected exactly 1 effective deployment, got %d", len(hist))
	}
	t.Logf("输入: %d 路并发提交相同内容（部分换键/无键）; 判定依据: 全部返回同一 seq=%d、历史 1 条", n, out[0].Seq)
}

// 被拒请求（非法版本/不兼容/超配额/无权限）不得改历史、不改当前版本、不消耗额度。
func TestRejectedRequestsHaveNoSideEffects(t *testing.T) {
	cp := controlplane.New()
	cp.SetQuota("tenant-q", controlplane.Quota{MaxDevices: 10, MaxDeployments: 2})
	if err := cp.RegisterDevice("tenant-q", "c", "d1", caps); err != nil {
		t.Fatal(err)
	}
	snapshot := func() (int, string, int) {
		h, _ := cp.History("tenant-q", "c", "d1")
		cur, _ := cp.CurrentVersion("tenant-q", "c", "d1")
		return len(h), cur, cp.DeploymentCount("tenant-q")
	}
	_, _ = cp.Deploy(specOf("tenant-q", "c", "d1", "v1.0.0", ""))
	_, _ = cp.Deploy(specOf("tenant-q", "c", "d1", "v1.1.0", ""))
	beforeH, beforeCur, beforeUsed := snapshot()
	if beforeH != 2 || beforeUsed != 2 {
		t.Fatalf("setup: hist=%d used=%d", beforeH, beforeUsed)
	}
	cases := []struct {
		name string
		spec fleet.DeploymentSpec
		want fleet.Reason
	}{
		{"invalid-version", specOf("tenant-q", "c", "d1", "1.0", "k1"), fleet.ReasonInvalidVersion},
		{"incompatible-arch", fleet.DeploymentSpec{TenantID: "tenant-q", ClusterID: "c", DeviceID: "d1",
			Artifact: fleet.Artifact{Version: "v9.0.0", Arch: "mips", OS: "linux", SizeBytes: 1}}, fleet.ReasonIncompatible},
		{"quota-exceeded", specOf("tenant-q", "c", "d1", "v2.0.0", "k2"), fleet.ReasonQuotaExceeded},
		{"forbidden", specOf("tenant-a", "c", "d1", "v2.0.0", "k3"), fleet.ReasonForbidden},
	}
	for _, tc := range cases {
		_, err := cp.Deploy(tc.spec)
		if fleet.ReasonOf(err) != tc.want {
			t.Fatalf("%s: want reason %s, got %v", tc.name, tc.want, err)
		}
		h, cur, used := snapshot()
		t.Logf("输入: 被拒请求 %s (reason=%s); 判定依据: 历史=%d 当前=%s 已用额度=%d 与拒前 (%d,%s,%d) 一致",
			tc.name, tc.want, h, cur, used, beforeH, beforeCur, beforeUsed)
		if h != beforeH || cur != beforeCur || used != beforeUsed {
			t.Fatalf("%s caused side effects: before=(%d,%s,%d) after=(%d,%s,%d)",
				tc.name, beforeH, beforeCur, beforeUsed, h, cur, used)
		}
	}
}
