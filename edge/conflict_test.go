package edge_test

import (
	"fmt"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/fleet"
)

// 同一 (设备,键,位点) 上报不同值：纯内容裁决（字典序最大者胜），与到达顺序无关。
func TestSameSeqConflictOrderIndependent(t *testing.T) {
	values := []string{"alpha", "gamma", "beta"}
	orders := [][]int{
		{0, 1, 2},
		{2, 1, 0},
		{1, 0, 2},
	}
	var finals []string
	var reports [][]fleet.ConflictReport
	norm := func(c fleet.ConflictReport) fleet.ConflictReport {
		c.DeviceID = "" // 各轮用不同设备，只比较裁决内容
		return c
	}
	for oi, ord := range orders {
		cp, _, _, _, _ := newEnv(t)
		devID := fmt.Sprintf("ord-%d", oi)
		if err := cp.RegisterDevice("tenant-a", "cluster-1", devID, caps); err != nil {
			t.Fatal(err)
		}
		batch := make([]fleet.Telemetry, 0, len(ord))
		for _, idx := range ord {
			batch = append(batch, fleet.Telemetry{ClusterID: "cluster-1", DeviceID: devID,
				Seq: 7, Key: "mode", Value: values[idx]})
		}
		res, err := cp.Ingest("tenant-a", batch)
		if err != nil {
			t.Fatalf("ingest: %v", err)
		}
		v, _, _ := cp.State("tenant-a", "cluster-1", devID, "mode")
		cr, err := cp.Conflicts("tenant-a", "cluster-1", devID)
		if err != nil {
			t.Fatal(err)
		}
		finals = append(finals, v)
		normed := make([]fleet.ConflictReport, len(cr))
		for i := range cr {
			normed[i] = norm(cr[i])
		}
		reports = append(reports, normed)
		t.Logf("输入: 顺序%v 值%v(seq=7); 判定依据: 字典序最大 gamma 获胜, 本批冲突数=%d, 持久冲突报告 winner=%q losers=%v rule=%q, 终值=%q",
			ord, pick(values, ord), len(res.Conflicts), cr[0].Winner, cr[0].Losers, cr[0].Rule, v)
	}
	for i, v := range finals {
		if v != "gamma" {
			t.Fatalf("order %d converged to %q, want gamma", i, v)
		}
	}
	for i := 1; i < len(reports); i++ {
		if !reflect.DeepEqual(reports[0], reports[i]) {
			t.Fatalf("conflict reports differ by arrival order:\n%+v\nvs\n%+v", reports[0], reports[i])
		}
	}
}

func pick(src []string, idx []int) []string {
	out := make([]string, len(idx))
	for i, j := range idx {
		out[i] = src[j]
	}
	return out
}

// 不同批次、先到先得的“先到值”不再影响结果：拆批到达必须收敛一致。
func TestSameSeqConflictAcrossBatches(t *testing.T) {
	cp, _, _, _, _ := newEnv(t)
	b1 := []fleet.Telemetry{{ClusterID: "cluster-1", DeviceID: "dev-1", Seq: 3, Key: "k", Value: "zzz"}}
	b2 := []fleet.Telemetry{{ClusterID: "cluster-1", DeviceID: "dev-1", Seq: 3, Key: "k", Value: "aaa"}}
	r1, err := cp.Ingest("tenant-a", b1)
	if err != nil {
		t.Fatal(err)
	}
	if len(r1.Conflicts) != 0 {
		t.Fatalf("single value is not a conflict, got %d reports", len(r1.Conflicts))
	}
	v1, _, _ := cp.State("tenant-a", "cluster-1", "dev-1", "k")
	r2, err := cp.Ingest("tenant-a", b2) // 第二批引入冲突
	if err != nil {
		t.Fatal(err)
	}
	v2, _, _ := cp.State("tenant-a", "cluster-1", "dev-1", "k")
	cr, _ := cp.Conflicts("tenant-a", "cluster-1", "dev-1")
	t.Logf("输入: seq=3 先到 zzz 后到 aaa（不同批次）; 判定依据: 最终值必须由内容决定为 zzz, 冲突报告 winner=%q losers=%v",
		cr[0].Winner, cr[0].Losers)
	if v1 != "zzz" {
		t.Fatalf("first batch value %q", v1)
	}
	if v2 != "zzz" || len(r2.Conflicts) != 1 || r2.Conflicts[0].Winner != "zzz" {
		t.Fatalf("cross-batch conflict did not reconcile by content: v=%q", v2)
	}
	// 被淘汰方可解释：aaa 在 losers 中且规则码正确。
	if len(cr) != 1 || !reflect.DeepEqual(cr[0].Losers, []string{"aaa"}) ||
		cr[0].Rule != fleet.RuleSameSeqDistinctValues {
		t.Fatalf("unexpected conflict report: %+v", cr)
	}
	// 重放相同批次：终值、报告都不变，不重复记录。
	if _, err := cp.Ingest("tenant-a", b1); err != nil {
		t.Fatal(err)
	}
	if _, err := cp.Ingest("tenant-a", b2); err != nil {
		t.Fatal(err)
	}
	v3, _, _ := cp.State("tenant-a", "cluster-1", "dev-1", "k")
	cr2, _ := cp.Conflicts("tenant-a", "cluster-1", "dev-1")
	if v3 != "zzz" || !reflect.DeepEqual(cr, cr2) {
		t.Fatalf("replay changed result: %q / %+v vs %+v", v3, cr2, cr)
	}
}

// 多路并发上报同一位置：终值与冲突报告必须确定，反复运行稳定。
func TestSameSeqConflictConcurrent(t *testing.T) {
	const ways = 16
	summaries := make([]string, 0, 3)
	for round := 0; round < 3; round++ {
		cp, _, _, _, _ := newEnv(t)
		devID := fmt.Sprintf("cc-%d", round)
		if err := cp.RegisterDevice("tenant-a", "cluster-1", devID, caps); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for w := 0; w < ways; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				// 偶数路报 "blue"，奇数路报 "red"，另有一路 "green"；全部同 seq。
				val := "blue"
				if w%2 == 1 {
					val = "red"
				}
				if w == 0 {
					val = "green"
				}
				if _, err := cp.Ingest("tenant-a", []fleet.Telemetry{{ClusterID: "cluster-1", DeviceID: devID,
					Seq: 5, Key: "color", Value: val}}); err != nil {
					t.Errorf("ingest: %v", err)
				}
			}(w)
		}
		wg.Wait()
		v, _, _ := cp.State("tenant-a", "cluster-1", devID, "color")
		cr, _ := cp.Conflicts("tenant-a", "cluster-1", devID)
		if v != "red" {
			t.Fatalf("round %d: want lexicographically greatest red, got %q", round, v)
		}
		if len(cr) != 1 || cr[0].Winner != "red" ||
			!reflect.DeepEqual(cr[0].Losers, []string{"blue", "green"}) {
			t.Fatalf("round %d unexpected report: %+v", round, cr)
		}
		losers := append([]string(nil), cr[0].Losers...)
		sort.Strings(losers)
		summaries = append(summaries, fmt.Sprintf("%s|%v|%s", cr[0].Winner, losers, cr[0].Rule))
		t.Logf("输入: round=%d %d 路并发同 seq=5 上报 blue/red/green; 判定依据: winner=%s losers=%v rule=%s 终值=%s",
			round, ways, cr[0].Winner, cr[0].Losers, cr[0].Rule, v)
	}
	for i := 1; i < len(summaries); i++ {
		if summaries[i] != summaries[0] {
			t.Fatalf("concurrent rounds not reproducible: %q vs %q", summaries[0], summaries[i])
		}
	}
}

// 不同位点仍然按高位点胜，且不报“冲突”（这是正常的版本推进）。
func TestDifferentSeqNoFalseConflict(t *testing.T) {
	cp, _, _, _, _ := newEnv(t)
	res, err := cp.Ingest("tenant-a", []fleet.Telemetry{
		{ClusterID: "cluster-1", DeviceID: "dev-1", Seq: 1, Key: "k", Value: "zzz"},
		{ClusterID: "cluster-1", DeviceID: "dev-1", Seq: 2, Key: "k", Value: "aaa"},
		{ClusterID: "cluster-1", DeviceID: "dev-1", Seq: 1, Key: "k", Value: "zzz"}, // 重复
	})
	if err != nil {
		t.Fatal(err)
	}
	v, _, _ := cp.State("tenant-a", "cluster-1", "dev-1", "k")
	cr, _ := cp.Conflicts("tenant-a", "cluster-1", "dev-1")
	t.Logf("输入: seq1=zzz seq2=aaa + seq1 重复; 判定依据: 高位点 aaa 胜且不算冲突, 终值=%q 冲突数=%d acked=%d",
		v, len(cr), res.Checkpoint.AckedSeq)
	if v != "aaa" || len(cr) != 0 || res.Checkpoint.AckedSeq != 2 {
		t.Fatalf("unexpected: value=%q conflicts=%d acked=%d", v, len(cr), res.Checkpoint.AckedSeq)
	}
}

// 冲突状态按 (租户,集群,设备,键) 隔离：同租户不同集群的同名设备互不影响。
func TestConflictScopedByCluster(t *testing.T) {
	cp, _, _, _, _ := newEnv(t)
	if err := cp.RegisterDevice("tenant-a", "cluster-2", "dev-1", caps); err != nil {
		t.Fatal(err)
	}
	if _, err := cp.Ingest("tenant-a", []fleet.Telemetry{
		{ClusterID: "cluster-1", DeviceID: "dev-1", Seq: 1, Key: "k", Value: "c1-a"},
		{ClusterID: "cluster-1", DeviceID: "dev-1", Seq: 1, Key: "k", Value: "c1-b"},
		{ClusterID: "cluster-2", DeviceID: "dev-1", Seq: 1, Key: "k", Value: "c2-only"},
	}); err != nil {
		t.Fatal(err)
	}
	v1, _, _ := cp.State("tenant-a", "cluster-1", "dev-1", "k")
	v2, _, _ := cp.State("tenant-a", "cluster-2", "dev-1", "k")
	cr1, _ := cp.Conflicts("tenant-a", "cluster-1", "dev-1")
	cr2, _ := cp.Conflicts("tenant-a", "cluster-2", "dev-1")
	t.Logf("输入: 同设备名两集群; 判定依据: cluster-1 冲突收敛到 %q 有 1 条报告, cluster-2 值 %q 无冲突; 实际 v1=%q v2=%q 冲突数=%d/%d",
		"c1-b", "c2-only", v1, v2, len(cr1), len(cr2))
	if v1 != "c1-b" || v2 != "c2-only" || len(cr1) != 1 || len(cr2) != 0 {
		t.Fatalf("cluster isolation broken: v1=%q v2=%q conflicts=%d/%d", v1, v2, len(cr1), len(cr2))
	}
}

// 第三个候选值晚到（字典序更大）必须改写裁决结果，并更新冲突报告。
func TestConflictLateArrivalReconciles(t *testing.T) {
	cp, _, _, _, _ := newEnv(t)
	ing := func(v string) fleet.ConflictReport {
		t.Helper()
		res, err := cp.Ingest("tenant-a", []fleet.Telemetry{{ClusterID: "cluster-1", DeviceID: "dev-1",
			Seq: 4, Key: "k", Value: v}})
		if err != nil {
			t.Fatal(err)
		}
		vv, _, _ := cp.State("tenant-a", "cluster-1", "dev-1", "k")
		if len(res.Conflicts) > 0 {
			return res.Conflicts[0]
		}
		cr, _ := cp.Conflicts("tenant-a", "cluster-1", "dev-1")
		t.Logf("（无新冲突）当前值=%q 历史冲突数=%d", vv, len(cr))
		return fleet.ConflictReport{}
	}
	if _, err := cp.Ingest("tenant-a", []fleet.Telemetry{{ClusterID: "cluster-1", DeviceID: "dev-1",
		Seq: 4, Key: "k", Value: "mid"}}); err != nil {
		t.Fatal(err)
	}
	r1 := ing("low") // mid > low，采纳 mid
	v1, _, _ := cp.State("tenant-a", "cluster-1", "dev-1", "k")
	r2 := ing("z-high") // z-high 最大，裁决改写
	v2, _, _ := cp.State("tenant-a", "cluster-1", "dev-1", "k")
	cr, _ := cp.Conflicts("tenant-a", "cluster-1", "dev-1")
	t.Logf("输入: seq=4 依次到 mid, low, z-high; 判定依据: r1.winner=%s losers=%v; r2.winner=%s losers=%v; 终值 %q->%q",
		r1.Winner, r1.Losers, r2.Winner, r2.Losers, v1, v2)
	if r1.Winner != "mid" || !reflect.DeepEqual(r1.Losers, []string{"low"}) {
		t.Fatalf("first reconciliation wrong: %+v", r1)
	}
	if r2.Winner != "z-high" || !reflect.DeepEqual(r2.Losers, []string{"low", "mid"}) || v2 != "z-high" {
		t.Fatalf("late arrival reconciliation wrong: %+v value=%q", r2, v2)
	}
	if len(cr) != 1 || cr[0].Winner != "z-high" {
		t.Fatalf("persisted report wrong: %+v", cr)
	}
}
