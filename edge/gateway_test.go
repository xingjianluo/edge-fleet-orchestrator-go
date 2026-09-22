package edge_test

import (
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/controlplane"
	"github.com/xingjianluo/edge-fleet-orchestrator-go/edge"
	"github.com/xingjianluo/edge-fleet-orchestrator-go/fleet"
)

var caps = fleet.Capabilities{Arch: "arm64", OS: "linux", MaxArtifact: 1 << 20}

func newEnv(t *testing.T) (*controlplane.ControlPlane, *edge.Store, *edge.Gateway, *edge.Device, string) {
	t.Helper()
	cp := controlplane.New()
	cp.SetQuota("tenant-a", controlplane.Quota{MaxDevices: 100, MaxDeployments: 100})
	if err := cp.RegisterDevice("tenant-a", "cluster-1", "dev-1", caps); err != nil {
		t.Fatalf("register: %v", err)
	}
	path := filepath.Join(t.TempDir(), "gateway-store.json")
	st, err := edge.OpenStore(path)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	gw := edge.NewGateway("tenant-a", cp, st)
	dev := edge.NewDevice("dev-1", caps)
	return cp, st, gw, dev, path
}

func TestOfflineBufferAndReconnectResume(t *testing.T) {
	cp, _, gw, dev, _ := newEnv(t)
	gw.SetOnline(false) // 断网
	for i := 0; i < 5; i++ {
		if err := gw.Report(dev.Emit("temperature", fmt.Sprintf("2%d", i))); err != nil {
			t.Fatalf("report %d: %v", i, err)
		}
	}
	v, ok, _ := cp.State("tenant-a", "dev-1", "temperature")
	t.Logf("输入: 断网期间上报 5 条; 判定依据: 控制面应无数据, 实际 ok=%v value=%q", ok, v)
	if ok {
		t.Fatalf("control plane should have no data while offline, got %q", v)
	}
	gw.SetOnline(true) // 重连
	acked, err := gw.Sync("dev-1")
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	v, ok, _ = cp.State("tenant-a", "dev-1", "temperature")
	t.Logf("输入: 重连续传; 判定依据: 确认位点=5 且最终值为位点最大的 %q, 实际 acked=%d value=%q", "24", acked, v)
	if acked != 5 || !ok || v != "24" {
		t.Fatalf("expected acked=5 value=24, got acked=%d value=%q", acked, v)
	}
}

func TestDuplicateAndOutOfOrder(t *testing.T) {
	cp, _, gw, dev, _ := newEnv(t)
	// 构造乱序 + 重复批次：直接走控制面 Ingest 以模拟网关重试/乱序到达。
	recs := []fleet.Telemetry{
		dev.Emit("mode", "a"), // seq1
		dev.Emit("mode", "b"), // seq2
		dev.Emit("mode", "c"), // seq3
	}
	batch := []fleet.Telemetry{recs[2], recs[0], recs[1], recs[0], recs[2]} // 乱序+重复
	cp1, err := cp.Ingest("tenant-a", batch)
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	v, _, _ := cp.State("tenant-a", "dev-1", "mode")
	t.Logf("输入: 乱序[3,1,2]+重复[1,3]; 判定依据: 位点大者胜应为 %q, 连续位点应推进到 3, 实际 value=%q acked=%d", "c", v, cp1.AckedSeq)
	if v != "c" || cp1.AckedSeq != 3 {
		t.Fatalf("expected value=c acked=3, got value=%q acked=%d", v, cp1.AckedSeq)
	}
	// 重复上报整批：不得改变结果。
	cp2, err := cp.Ingest("tenant-a", batch)
	if err != nil {
		t.Fatalf("re-ingest: %v", err)
	}
	v2, _, _ := cp.State("tenant-a", "dev-1", "mode")
	t.Logf("输入: 整批重复上报; 判定依据: 状态不变且位点不回退, 实际 value=%q acked=%d", v2, cp2.AckedSeq)
	if v2 != "c" || cp2.AckedSeq != 3 {
		t.Fatalf("duplicate ingest changed state: value=%q acked=%d", v2, cp2.AckedSeq)
	}
	_ = gw
}

func TestConflictConvergesDeterministically(t *testing.T) {
	cp, _, _, _, _ := newEnv(t)
	// 同一键两个来源以相反顺序到达：高位点必须获胜，与到达顺序无关。
	a := fleet.Telemetry{DeviceID: "dev-1", Seq: 10, Key: "config", Value: "new"}
	b := fleet.Telemetry{DeviceID: "dev-1", Seq: 9, Key: "config", Value: "old"}
	if _, err := cp.Ingest("tenant-a", []fleet.Telemetry{a, b}); err != nil {
		t.Fatalf("ingest order1: %v", err)
	}
	v1, _, _ := cp.State("tenant-a", "dev-1", "config")
	// 换设备以相反顺序重放。
	if err := cp.RegisterDevice("tenant-a", "cluster-1", "dev-2", caps); err != nil {
		t.Fatalf("register dev-2: %v", err)
	}
	a2, b2 := a, b
	a2.DeviceID, b2.DeviceID = "dev-2", "dev-2"
	if _, err := cp.Ingest("tenant-a", []fleet.Telemetry{b2, a2}); err != nil {
		t.Fatalf("ingest order2: %v", err)
	}
	v2, _, _ := cp.State("tenant-a", "dev-2", "config")
	t.Logf("输入: seq10=%q 与 seq9=%q 以两种顺序到达; 判定依据: 两种顺序都应收敛到高位点值", a.Value, b.Value)
	t.Logf("结果: 顺序[10,9]->%q 顺序[9,10]->%q", v1, v2)
	if v1 != "new" || v2 != "new" {
		t.Fatalf("conflict did not converge deterministically: %q vs %q", v1, v2)
	}
}

func TestRestartRecovery(t *testing.T) {
	cp, _, gw, dev, path := newEnv(t)
	gw.SetOnline(false)
	for i := 0; i < 3; i++ {
		if err := gw.Report(dev.Emit("k", fmt.Sprintf("v%d", i))); err != nil {
			t.Fatalf("report: %v", err)
		}
	}
	// 模拟进程重启：用同一文件路径重新打开存储、重建网关。
	st2, err := edge.OpenStore(path)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	gw2 := edge.NewGateway("tenant-a", cp, st2)
	gw2.SetOnline(true)
	acked, err := gw2.Sync("dev-1")
	if err != nil {
		t.Fatalf("sync after restart: %v", err)
	}
	v, _, _ := cp.State("tenant-a", "dev-1", "k")
	t.Logf("输入: 断网缓冲 3 条后进程重启再重连; 判定依据: 数据不丢, acked=3 value=%q, 实际 acked=%d value=%q", "v2", acked, v)
	if acked != 3 || v != "v2" {
		t.Fatalf("restart lost data: acked=%d value=%q", acked, v)
	}
	// 重启后再次 Sync 应为空操作且不重复生效。
	acked2, err := gw2.Sync("dev-1")
	if err != nil {
		t.Fatalf("re-sync: %v", err)
	}
	if acked2 != 3 {
		t.Fatalf("re-sync regressed checkpoint: %d", acked2)
	}
}

func TestConcurrentReportsConsistent(t *testing.T) {
	cp, _, gw, _, _ := newEnv(t)
	devs := make([]*edge.Device, 4)
	for i := range devs {
		id := fmt.Sprintf("dev-c%d", i)
		if err := cp.RegisterDevice("tenant-a", "cluster-1", id, caps); err != nil {
			t.Fatalf("register %s: %v", id, err)
		}
		devs[i] = edge.NewDevice(id, caps)
	}
	const perDev = 25
	var wg sync.WaitGroup
	for _, d := range devs {
		wg.Add(1)
		go func(d *edge.Device) {
			defer wg.Done()
			for i := 0; i < perDev; i++ {
				if err := gw.Report(d.Emit("counter", fmt.Sprintf("%d", i))); err != nil {
					t.Errorf("report: %v", err)
					return
				}
			}
		}(d)
	}
	wg.Wait()
	// 校验每台设备：位点全部确认且最终值确定（等于最大位点的值）。
	for i := range devs {
		id := fmt.Sprintf("dev-c%d", i)
		acked, err := gw.Sync(id)
		if err != nil {
			t.Fatalf("sync %s: %v", id, err)
		}
		v, _, _ := cp.State("tenant-a", id, "counter")
		t.Logf("输入: 设备 %s 并发上报 %d 条; 判定依据: acked=%d 且 value 为最后一条 %q, 实际 value=%q",
			id, perDev, perDev, fmt.Sprintf("%d", perDev-1), v)
		if acked != perDev || v != fmt.Sprintf("%d", perDev-1) {
			t.Fatalf("device %s: acked=%d value=%q", id, acked, v)
		}
	}
}
