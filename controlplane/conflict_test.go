package controlplane_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/fleet"
)

// 同一位置、同位点、不同值，以相反顺序到达：只按 (Seq, Value) 收敛，
// 值字典序较大者恒胜；败方必须留下可解释的冲突报告，且重放不重复记录。
func TestSamePositionConflictOrderIndependent(t *testing.T) {
	run := func(order string, batch []fleet.Telemetry) (string, []fleet.ConflictReport) {
		cp := setup(t)
		_, _, err := cp.IngestDetailed("tenant-a", batch)
		if err != nil {
			t.Fatalf("ingest %s: %v", order, err)
		}
		v, ok, _ := cp.State("tenant-a", "", "dev-1", "config")
		if !ok {
			t.Fatalf("%s: no state", order)
		}
		// 整批重放：结果与冲突报告集合都不得变化。
		_, reports2, err := cp.IngestDetailed("tenant-a", batch)
		if err != nil {
			t.Fatalf("replay %s: %v", order, err)
		}
		v2, _, _ := cp.State("tenant-a", "", "dev-1", "config")
		all, _ := cp.Conflicts("tenant-a", "", "dev-1")
		if v2 != v || len(reports2) != 0 || len(all) != 1 {
			t.Fatalf("%s replay changed outcome: v=%q->%q newReports=%d totalReports=%d",
				order, v, v2, len(reports2), len(all))
		}
		return v, all
	}

	alpha := fleet.Telemetry{ClusterID: "cluster-1", DeviceID: "dev-1", Seq: 7, Key: "config", Value: "alpha"}
	beta := fleet.Telemetry{ClusterID: "cluster-1", DeviceID: "dev-1", Seq: 7, Key: "config", Value: "beta"}

	v1, rep1 := run("alpha,beta", []fleet.Telemetry{alpha, beta})
	v2, rep2 := run("beta,alpha", []fleet.Telemetry{beta, alpha})
	t.Logf("输入: 同位点 seq=7 的 %q/%q 以两种顺序到达; 判定依据: (Seq,Value) 字典序取大 -> %q", alpha.Value, beta.Value, v1)
	t.Logf("结果: 顺序[alpha,beta]=%q 顺序[beta,alpha]=%q; 冲突依据: %s", v1, v2, rep1[0].Basis)
	if v1 != "beta" || v2 != "beta" {
		t.Fatalf("conflict did not converge to lexicographic winner: %q vs %q", v1, v2)
	}
	if rep1[0].Winner.Value != "beta" || rep1[0].Loser.Value != "alpha" || rep1[0].Reason != fleet.ReasonConflictResolved {
		t.Fatalf("bad conflict report: %+v", rep1[0])
	}
	// 两种顺序产出的判定依据必须逐字一致（只由内容决定）。
	if rep1[0].Basis != rep2[0].Basis || rep1[0].Winner.Value != rep2[0].Winner.Value {
		t.Fatalf("conflict basis differs by arrival order:\n%s\n%s", rep1[0].Basis, rep2[0].Basis)
	}
}

// 多路并发上报同一冲突位置：无论多少路、跑多少遍，最终值与冲突记录数恒定。
func TestConflictConcurrentConverges(t *testing.T) {
	const rounds = 8
	for round := 0; round < rounds; round++ {
		cp := setup(t)
		const n = 32
		var wg sync.WaitGroup
		for i := 0; i < n; i++ {
			wg.Add(1)
			go func(i int) {
				defer wg.Done()
				val := "alpha"
				if i%2 == 0 {
					val = "beta"
				}
				batch := []fleet.Telemetry{{
					ClusterID: "cluster-1", DeviceID: "dev-1",
					Source: fmt.Sprintf("uplink-%d", i), Seq: 5, Key: "mode", Value: val,
				}}
				if _, _, err := cp.IngestDetailed("tenant-a", batch); err != nil {
					t.Errorf("ingest %d: %v", i, err)
				}
			}(i)
		}
		wg.Wait()
		v, _, _ := cp.State("tenant-a", "", "dev-1", "mode")
		reps, _ := cp.Conflicts("tenant-a", "", "dev-1")
		if v != "beta" {
			t.Fatalf("round %d: value=%q want beta", round, v)
		}
		if len(reps) != 1 {
			t.Fatalf("round %d: conflict reports=%d want 1", round, len(reps))
		}
		if reps[0].Winner.Value != "beta" || reps[0].Loser.Value != "alpha" {
			t.Fatalf("round %d: unexpected conflict: %+v", round, reps[0])
		}
	}
	t.Logf("输入: %d 路上行并发、同位点 seq=5 的 beta/alpha, 重复 %d 轮; 判定依据: 恒收敛 beta 且仅 1 条冲突记录", 32, rounds)
}

// 位点不同不是冲突：高位点正常推进，过期值丢弃但不产生冲突报告。
func TestDifferentSeqIsNotConflict(t *testing.T) {
	cp := setup(t)
	_, reports, err := cp.IngestDetailed("tenant-a", []fleet.Telemetry{
		{ClusterID: "cluster-1", DeviceID: "dev-1", Seq: 10, Key: "k", Value: "new"},
		{ClusterID: "cluster-1", DeviceID: "dev-1", Seq: 9, Key: "k", Value: "old"},
	})
	if err != nil {
		t.Fatalf("ingest: %v", err)
	}
	v, _, _ := cp.State("tenant-a", "", "dev-1", "k")
	all, _ := cp.Conflicts("tenant-a", "", "dev-1")
	t.Logf("输入: seq10=new 与 seq9=old; 判定依据: 位点大者胜=%q 且不产生冲突报告, 实际报告 %d 条", v, len(all))
	if v != "new" || len(all) != 0 || len(reports) != 0 {
		t.Fatalf("different seq produced conflict: v=%s reports=%d/%d", v, len(reports), len(all))
	}
}
