package edge_test

import (
	"path/filepath"
	"sync"
	"testing"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/controlplane"
	"github.com/xingjianluo/edge-fleet-orchestrator-go/edge"
	"github.com/xingjianluo/edge-fleet-orchestrator-go/fleet"
)

// 同一集群部署两台不同上行通道的网关，并发上报同一台设备同一状态位置、同位点不同值。
// 收敛只看内容（beta 字典序胜），且可从冲突报告解释败方与判定依据；反复运行稳定。
func TestMultiGatewayConflictConverges(t *testing.T) {
	cp := controlplane.New()
	cp.SetQuota("tenant-a", controlplane.Quota{MaxDevices: 100, MaxDeployments: 100})
	if err := cp.RegisterDevice("tenant-a", "cluster-1", "dev-1", caps); err != nil {
		t.Fatalf("register: %v", err)
	}
	st1, err := edge.OpenStore(filepath.Join(t.TempDir(), "g1.json"))
	if err != nil {
		t.Fatalf("store1: %v", err)
	}
	st2, err := edge.OpenStore(filepath.Join(t.TempDir(), "g2.json"))
	if err != nil {
		t.Fatalf("store2: %v", err)
	}
	gw1 := edge.NewClusterGateway("tenant-a", "cluster-1", "uplink-A", cp, st1)
	gw2 := edge.NewClusterGateway("tenant-a", "cluster-1", "uplink-B", cp, st2)

	const perGateway = 20
	var wg sync.WaitGroup
	round := func() {
		for g := 0; g < 2; g++ {
			gw := []*edge.Gateway{gw1, gw2}[g]
			val := []string{"alpha", "beta"}[g]
			wg.Add(1)
			go func(gw *edge.Gateway, val string) {
				defer wg.Done()
				for i := 0; i < perGateway; i++ {
					// 同一位置 mode、同位点 1、不同值；重复发送模拟链路重发。
					tel := fleet.Telemetry{DeviceID: "dev-1", Seq: 1, Key: "mode", Value: val}
					if err := gw.Report(tel); err != nil {
						t.Errorf("report via %s: %v", val, err)
						return
					}
				}
			}(gw, val)
		}
		wg.Wait()
	}
	for r := 0; r < 5; r++ {
		round()
	}
	v, ok, _ := cp.State("tenant-a", "cluster-1", "dev-1", "mode")
	reps, err := cp.Conflicts("tenant-a", "cluster-1", "dev-1")
	if err != nil {
		t.Fatalf("conflicts: %v", err)
	}
	t.Logf("输入: 两条上行通道各重发 %d 次、同位点 seq=1 的 alpha/beta; 判定依据: 恒收敛 %q, 实际 %q(ok=%v), 冲突报告 %d 条; 依据=%s",
		perGateway, "beta", v, ok, len(reps), func() string {
			if len(reps) > 0 {
				return reps[0].Basis
			}
			return "<none>"
		}())
	if !ok || v != "beta" {
		t.Fatalf("converged value=%q ok=%v, want beta", v, ok)
	}
	if len(reps) != 1 {
		t.Fatalf("conflict reports=%d, want 1", len(reps))
	}
	if reps[0].Winner.Source != "uplink-B" || reps[0].Loser.Source != "uplink-A" {
		t.Fatalf("conflict sources: winner=%q loser=%q", reps[0].Winner.Source, reps[0].Loser.Source)
	}
}
