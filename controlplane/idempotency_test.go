package controlplane_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/controlplane"
	"github.com/xingjianluo/edge-fleet-orchestrator-go/fleet"
)

var fullCaps = fleet.Capabilities{Arch: "arm64", OS: "linux", MaxArtifact: 1 << 20, Features: []string{"gpu", "npu"}}

func fullArtifact(v string) fleet.Artifact {
	return fleet.Artifact{Version: v, Arch: "arm64", OS: "linux", SizeBytes: 2048,
		RequireFeature: []string{"gpu", "npu"}}
}

// setupFull 注册一台支持 gpu/npu 的设备。
func setupFull(t *testing.T) *controlplane.ControlPlane {
	t.Helper()
	cp := controlplane.New()
	cp.SetQuota("tenant-a", controlplane.Quota{MaxDevices: 10, MaxDeployments: 100})
	if err := cp.RegisterDevice("tenant-a", "cluster-1", "dev-1", fullCaps); err != nil {
		t.Fatalf("register: %v", err)
	}
	return cp
}

func deploy(tenant, cluster, device, key string, a fleet.Artifact) fleet.DeploymentSpec {
	return fleet.DeploymentSpec{TenantID: tenant, ClusterID: cluster, DeviceID: device,
		Artifact: a, IdempotencyKey: key}
}

// 重试 / 不带标识 / 换标识的内容级重复下发只生效一次。
func TestContentLevelIdempotency(t *testing.T) {
	cp := setupFull(t)
	base := fullArtifact("v1.0.0")
	r1, err := cp.Deploy(deploy("tenant-a", "cluster-1", "dev-1", "req-1", base))
	if err != nil {
		t.Fatalf("first: %v", err)
	}
	// 重试：完全相同、同一幂等键。
	r2, err := cp.Deploy(deploy("tenant-a", "cluster-1", "dev-1", "req-1", base))
	if err != nil {
		t.Fatalf("retry same key: %v", err)
	}
	// 不带任何请求标识。
	r3, err := cp.Deploy(deploy("tenant-a", "cluster-1", "dev-1", "", base))
	if err != nil {
		t.Fatalf("no key: %v", err)
	}
	// 重试链路把标识换掉。
	r4, err := cp.Deploy(deploy("tenant-a", "cluster-1", "dev-1", "req-retried-XYZ", base))
	if err != nil {
		t.Fatalf("rotated key: %v", err)
	}
	// 依赖特性顺序不同但集合相同，内容仍视为一致。
	shuffled := base
	shuffled.RequireFeature = []string{"npu", "gpu"}
	r5, err := cp.Deploy(deploy("tenant-a", "cluster-1", "dev-1", "req-2", shuffled))
	if err != nil {
		t.Fatalf("shuffled features: %v", err)
	}
	t.Logf("输入: 同一内容以 [同键重试/无键/换键/特性乱序] 5 次下发; 判定依据: 均返回首次 Seq=%d 且历史不增长、额度不增加", r1.Seq)
	t.Logf("结果: seqs=%d,%d,%d,%d,%d", r1.Seq, r2.Seq, r3.Seq, r4.Seq, r5.Seq)
	for i, r := range []*fleet.DeploymentRecord{r2, r3, r4, r5} {
		if r.Seq != r1.Seq {
			t.Fatalf("call %d returned seq %d, want first seq %d", i+2, r.Seq, r1.Seq)
		}
	}
	hist, _ := cp.History("tenant-a", "", "dev-1")
	if len(hist) != 1 {
		t.Fatalf("history grew to %d records", len(hist))
	}
	if n, _ := cp.DeployCount("tenant-a"); n != 1 {
		t.Fatalf("quota usage = %d, want 1", n)
	}
	cur, _ := cp.CurrentVersion("tenant-a", "", "dev-1")
	if cur != "v1.0.0" {
		t.Fatalf("current version changed to %q", cur)
	}

	// 内容真正变化才算新部署。
	next := fullArtifact("v1.1.0")
	r6, err := cp.Deploy(deploy("tenant-a", "cluster-1", "dev-1", "req-3", next))
	if err != nil {
		t.Fatalf("changed content: %v", err)
	}
	t.Logf("输入: 版本变为 v1.1.0; 判定依据: 新 Seq=%d 历史=2 额度=2", r6.Seq)
	if r6.Seq == r1.Seq {
		t.Fatalf("changed content returned first seq")
	}
	hist, _ = cp.History("tenant-a", "", "dev-1")
	if len(hist) != 2 {
		t.Fatalf("history = %d, want 2", len(hist))
	}
	if n, _ := cp.DeployCount("tenant-a"); n != 2 {
		t.Fatalf("quota usage = %d, want 2", n)
	}

	// 再下发与历史内容一致的旧规格：仍不重复记录，且返回该内容第一次生效的记录。
	r7, err := cp.Deploy(deploy("tenant-a", "cluster-1", "dev-1", "req-4", base))
	if err != nil {
		t.Fatalf("reissue old content: %v", err)
	}
	t.Logf("输入: 切到 v1.1.0 后再下发 v1.0.0 旧规格; 判定依据: 返回 v1.0.0 首次 Seq=%d 而非新建记录, 实际 Seq=%d", r1.Seq, r7.Seq)
	if r7.Seq != r1.Seq {
		t.Fatalf("old content seq = %d, want %d", r7.Seq, r1.Seq)
	}
	hist, _ = cp.History("tenant-a", "", "dev-1")
	if len(hist) != 2 {
		t.Fatalf("history grew to %d after reissuing old content", len(hist))
	}
	if n, _ := cp.DeployCount("tenant-a"); n != 2 {
		t.Fatalf("quota usage grew to %d, want 2", n)
	}
}

// 内容变化的判定维度：版本、OS/架构、大小、依赖特性任一不同即新部署。
func TestContentFingerprintDimensions(t *testing.T) {
	cases := []struct {
		name   string
		mutate func(fleet.Artifact) fleet.Artifact
	}{
		{"version", func(a fleet.Artifact) fleet.Artifact { a.Version = "v1.0.1"; return a }},
		{"size", func(a fleet.Artifact) fleet.Artifact { a.SizeBytes = 4096; return a }},
		{"feature", func(a fleet.Artifact) fleet.Artifact { a.RequireFeature = []string{"gpu"}; return a }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cp := setupFull(t)
			a := fullArtifact("v1.0.0")
			r1, err := cp.Deploy(deploy("tenant-a", "cluster-1", "dev-1", "", a))
			if err != nil {
				t.Fatalf("first: %v", err)
			}
			b := tc.mutate(a)
			if tc.name != "feature" {
				b.RequireFeature = []string{"gpu", "npu"}
			}
			r2, err := cp.Deploy(deploy("tenant-a", "cluster-1", "dev-1", "whatever", b))
			if err != nil {
				t.Fatalf("second: %v", err)
			}
			t.Logf("输入: 仅 %s 变化; 判定依据: 应产生新 Seq, 实际 %d -> %d", tc.name, r1.Seq, r2.Seq)
			if r2.Seq == r1.Seq {
				t.Fatalf("%s change treated as duplicate", tc.name)
			}
		})
	}
}

// 内容级幂等限定在同一租户、同一集群、同一设备范围内。
func TestIdempotencyScopedByTenantAndCluster(t *testing.T) {
	cp := controlplane.New()
	cp.SetQuota("tenant-a", controlplane.Quota{MaxDevices: 100, MaxDeployments: 100})
	cp.SetQuota("tenant-b", controlplane.Quota{MaxDevices: 100, MaxDeployments: 100})
	a := fullArtifact("v1.0.0")
	// 同租户不同集群下的同名设备：各自独立生效，不能互相顶掉。
	if err := cp.RegisterDevice("tenant-a", "cluster-1", "dev-x", fullCaps); err != nil {
		t.Fatalf("register c1: %v", err)
	}
	if err := cp.RegisterDevice("tenant-a", "cluster-2", "dev-x", fullCaps); err != nil {
		t.Fatalf("register c2: %v", err)
	}
	r1, err := cp.Deploy(deploy("tenant-a", "cluster-1", "dev-x", "k", a))
	if err != nil {
		t.Fatalf("deploy c1: %v", err)
	}
	r2, err := cp.Deploy(deploy("tenant-a", "cluster-2", "dev-x", "k", a))
	if err != nil {
		t.Fatalf("deploy c2: %v", err)
	}
	t.Logf("输入: 同租户不同集群同名设备、完全相同内容与键; 判定依据: 各自独立 Seq, 实际 %d vs %d", r1.Seq, r2.Seq)
	if r1.Seq == r2.Seq {
		t.Fatalf("deployments across clusters collapsed")
	}
	h1, _ := cp.History("tenant-a", "cluster-1", "dev-x")
	h2, _ := cp.History("tenant-a", "cluster-2", "dev-x")
	if len(h1) != 1 || len(h2) != 1 {
		t.Fatalf("expected independent histories, got %d and %d", len(h1), len(h2))
	}
	// 同集群内重复仍是幂等。
	r3, err := cp.Deploy(deploy("tenant-a", "cluster-1", "dev-x", "other-key", a))
	if err != nil || r3.Seq != r1.Seq {
		t.Fatalf("same-cluster repeat not idempotent: seq=%d err=%v", r3.Seq, err)
	}

	// 不同租户下的同名设备：deviceID 全局唯一，跨租户注册/访问必须 FORBIDDEN。
	err = cp.RegisterDevice("tenant-b", "cluster-1", "dev-x", fullCaps)
	if fleet.ReasonOf(err) != fleet.ReasonForbidden {
		t.Fatalf("cross-tenant register reason=%s err=%v", fleet.ReasonOf(err), err)
	}
	_, err = cp.Deploy(deploy("tenant-b", "cluster-1", "dev-x", "", a))
	t.Logf("输入: tenant-b 访问 tenant-a 的 dev-x; 判定依据: FORBIDDEN 且与 NOT_FOUND 可区分, 实际 reason=%s", fleet.ReasonOf(err))
	if fleet.ReasonOf(err) != fleet.ReasonForbidden {
		t.Fatalf("cross-tenant deploy reason=%s err=%v", fleet.ReasonOf(err), err)
	}
	// 不存在的资源仍为 NOT_FOUND，二者可区分。
	_, err = cp.Deploy(deploy("tenant-a", "cluster-1", "ghost", "", a))
	if fleet.ReasonOf(err) != fleet.ReasonNotFound {
		t.Fatalf("missing device reason=%s err=%v", fleet.ReasonOf(err), err)
	}
}

// 并发重复下发同内容（键各异/无键）：只有一路生效，其余全部返回同一 Seq。
func TestConcurrentContentIdempotency(t *testing.T) {
	cp := setupFull(t)
	const n = 40
	var wg sync.WaitGroup
	recs := make([]*fleet.DeploymentRecord, n)
	errs := make([]error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := ""
			if i%2 == 0 {
				key = fmt.Sprintf("rotated-key-%d", i) // 每路键都不同
			}
			recs[i], errs[i] = cp.Deploy(deploy("tenant-a", "cluster-1", "dev-1", key, fullArtifact("v1.0.0")))
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("deploy %d: %v", i, err)
		}
	}
	want := recs[0].Seq
	for i := 1; i < n; i++ {
		if recs[i].Seq != want {
			t.Fatalf("record %d seq=%d want %d", i, recs[i].Seq, want)
		}
	}
	hist, _ := cp.History("tenant-a", "", "dev-1")
	if n, _ := cp.DeployCount("tenant-a"); n != 1 || len(hist) != 1 {
		t.Fatalf("count=%d history=%d, want 1/1", n, len(hist))
	}
	t.Logf("输入: %d 路并发、内容相同、键各异或为空; 判定依据: 仅 1 条历史、全部返回 Seq=%d", n, want)
}
