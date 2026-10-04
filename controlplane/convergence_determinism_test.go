package controlplane_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/controlplane"
	"github.com/xingjianluo/edge-fleet-orchestrator-go/fleet"
)

// conflictFingerprint 提取一条淘汰记录的判定要素（忽略时间戳），用于跨场景逐字比对。
func conflictFingerprint(r fleet.ConflictReport) string {
	return fmt.Sprintf("key=%s seq=%d winner=%s loser=%s reason=%s basis=%s",
		r.Key, r.Seq, r.Winner.Value, r.Loser.Value, r.Reason, r.Basis)
}

func conflictFingerprints(reps []fleet.ConflictReport) []string {
	out := make([]string, len(reps))
	for i, r := range reps {
		out[i] = conflictFingerprint(r)
	}
	return out
}

// 同一位置、同一位点、三个不同值：无论到达顺序、批次切分、并发路数、重放多少遍，
// 最终值、被淘汰方集合与判定依据都必须逐字一致；整批重放不得重复记录。
func TestConvergenceDeterministicAcrossOrdersBatchesConcurrency(t *testing.T) {
	vals := []string{"alpha", "gamma", "beta"} // 字典序最大者 gamma 恒胜
	makeRecs := func(order []int) []fleet.Telemetry {
		out := make([]fleet.Telemetry, len(order))
		for i, idx := range order {
			out[i] = fleet.Telemetry{
				ClusterID: "cluster-1", DeviceID: "dev-1",
				Source: fmt.Sprintf("uplink-%s", vals[idx]),
				Seq:    9, Key: "mode", Value: vals[idx],
			}
		}
		return out
	}

	// outcome 跑一次完整场景：最终值 + 累积淘汰记录指纹 + 重放后的新增报告数。
	outcome := func(name string, ingest func(cp *controlplane.ControlPlane)) (string, []string, int) {
		cp := setup(t)
		ingest(cp)
		v, ok, err := cp.State("tenant-a", "cluster-1", "dev-1", "mode")
		if err != nil || !ok {
			t.Fatalf("%s: state missing: %v", name, err)
		}
		reps, err := cp.Conflicts("tenant-a", "cluster-1", "dev-1")
		if err != nil {
			t.Fatalf("%s: conflicts: %v", name, err)
		}
		// 整批重放同样的全部内容：不得产生新报告，累积记录不得变化。
		_, replayReports, err := cp.IngestDetailed("tenant-a", makeRecs([]int{0, 1, 2}))
		if err != nil {
			t.Fatalf("%s: replay: %v", name, err)
		}
		reps2, _ := cp.Conflicts("tenant-a", "cluster-1", "dev-1")
		if len(reps2) != len(reps) {
			t.Fatalf("%s: replay grew conflicts %d -> %d", name, len(reps), len(reps2))
		}
		v2, _, _ := cp.State("tenant-a", "cluster-1", "dev-1", "mode")
		if v2 != v {
			t.Fatalf("%s: replay changed value %q -> %q", name, v, v2)
		}
		return v, conflictFingerprints(reps), len(replayReports)
	}

	scenarios := map[string]func(cp *controlplane.ControlPlane){
		// 单批、三种不同到达顺序。
		"order-alpha,gamma,beta": func(cp *controlplane.ControlPlane) {
			if _, _, err := cp.IngestDetailed("tenant-a", makeRecs([]int{0, 1, 2})); err != nil {
				t.Fatalf("ingest: %v", err)
			}
		},
		"order-gamma,beta,alpha": func(cp *controlplane.ControlPlane) {
			if _, _, err := cp.IngestDetailed("tenant-a", makeRecs([]int{1, 2, 0})); err != nil {
				t.Fatalf("ingest: %v", err)
			}
		},
		"order-beta,alpha,gamma": func(cp *controlplane.ControlPlane) {
			if _, _, err := cp.IngestDetailed("tenant-a", makeRecs([]int{2, 0, 1})); err != nil {
				t.Fatalf("ingest: %v", err)
			}
		},
		// 分三批、每批一个值。
		"batches-1+1+1": func(cp *controlplane.ControlPlane) {
			for _, idx := range []int{0, 1, 2} {
				if _, _, err := cp.IngestDetailed("tenant-a", makeRecs([]int{idx})); err != nil {
					t.Fatalf("ingest: %v", err)
				}
			}
		},
		// 分两批、乱序。
		"batches-2+1": func(cp *controlplane.ControlPlane) {
			if _, _, err := cp.IngestDetailed("tenant-a", makeRecs([]int{2, 0})); err != nil {
				t.Fatalf("ingest: %v", err)
			}
			if _, _, err := cp.IngestDetailed("tenant-a", makeRecs([]int{1})); err != nil {
				t.Fatalf("ingest: %v", err)
			}
		},
		// 并发：每个值由 8 路 goroutine 重复上报（含并发重放）。
		"concurrent-8x3": func(cp *controlplane.ControlPlane) {
			var wg sync.WaitGroup
			for w := 0; w < 8; w++ {
				for idx := range vals {
					wg.Add(1)
					go func(idx int) {
						defer wg.Done()
						if _, _, err := cp.IngestDetailed("tenant-a", makeRecs([]int{idx})); err != nil {
							t.Errorf("ingest: %v", err)
						}
					}(idx)
				}
			}
			wg.Wait()
		},
	}

	var wantValue string
	var wantReps []string
	for name, ingest := range scenarios {
		v, reps, replayNew := outcome(name, ingest)
		t.Logf("场景[%s]: 最终值=%q 淘汰记录=%d 条 重放新增=%d", name, v, len(reps), replayNew)
		for _, fp := range reps {
			t.Logf("  淘汰记录: %s", fp)
		}
		if replayNew != 0 {
			t.Fatalf("%s: replay produced %d new reports", name, replayNew)
		}
		if wantValue == "" {
			wantValue, wantReps = v, reps
			continue
		}
		if v != wantValue {
			t.Fatalf("%s: value %q != %q", name, v, wantValue)
		}
		if len(reps) != len(wantReps) {
			t.Fatalf("%s: %d reports != %d", name, len(reps), len(wantReps))
		}
		for i := range reps {
			if reps[i] != wantReps[i] {
				t.Fatalf("%s: report %d differs:\n%s\n%s", name, i, reps[i], wantReps[i])
			}
		}
	}
	t.Logf("判定依据: 6 种场景最终值恒为 %q, 淘汰记录逐字一致（%d 条）, 整批重放 0 新增", wantValue, len(wantReps))
	if wantValue != "gamma" {
		t.Fatalf("winner = %q, want gamma (lexicographic max)", wantValue)
	}
	if len(wantReps) != 2 { // alpha、beta 均被淘汰，各一条
		t.Fatalf("want 2 elimination records (alpha,beta), got %d", len(wantReps))
	}
}

// 同一组值并发多路 + 反复重跑：淘汰记录数与内容恒定（竞态检测下稳定）。
func TestConvergenceStableUnderRepeatedConcurrentRuns(t *testing.T) {
	const rounds = 10
	for round := 0; round < rounds; round++ {
		cp := setup(t)
		var wg sync.WaitGroup
		for w := 0; w < 16; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				vals := []string{"alpha", "gamma", "beta"}
				// 每路以不同顺序上报全部三个值（含重复内容，靠去重吸收）。
				batch := make([]fleet.Telemetry, 3)
				for i := range vals {
					idx := (i + w) % 3
					batch[i] = fleet.Telemetry{
						ClusterID: "cluster-1", DeviceID: "dev-1",
						Source: fmt.Sprintf("uplink-%d", w),
						Seq:    4, Key: "mode", Value: vals[idx],
					}
				}
				if _, _, err := cp.IngestDetailed("tenant-a", batch); err != nil {
					t.Errorf("round %d worker %d: %v", round, w, err)
				}
			}(w)
		}
		wg.Wait()
		v, _, _ := cp.State("tenant-a", "cluster-1", "dev-1", "mode")
		reps, _ := cp.Conflicts("tenant-a", "cluster-1", "dev-1")
		if v != "gamma" || len(reps) != 2 {
			t.Fatalf("round %d: value=%q reports=%d, want gamma/2", round, v, len(reps))
		}
		losers := map[string]bool{}
		for _, r := range reps {
			if r.Winner.Value != "gamma" || r.Reason != fleet.ReasonConflictResolved {
				t.Fatalf("round %d: bad report %+v", round, r)
			}
			losers[r.Loser.Value] = true
		}
		if !losers["alpha"] || !losers["beta"] {
			t.Fatalf("round %d: losers=%v, want alpha+beta", round, losers)
		}
	}
	t.Logf("业务: 并发收敛稳定性; 输入: 16 路并发 x 3 值同位点, 重复 %d 轮; 判定依据: 每轮恒收敛 gamma 且淘汰记录恰为 alpha/beta 各一条", rounds)
}
