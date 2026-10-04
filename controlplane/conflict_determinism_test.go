package controlplane_test

import (
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/controlplane"
	"github.com/xingjianluo/edge-fleet-orchestrator-go/fleet"
)

// conflictSetup 构造同一设备同一位置同位点 3 个不同值（各来自不同上行通道）。
func conflictSetup(t *testing.T) (*controlplane.ControlPlane, []fleet.Telemetry) {
	t.Helper()
	cp := controlplane.New()
	cp.SetQuota("tenant-a", controlplane.Quota{MaxDevices: 10, MaxDeployments: 100})
	if err := cp.RegisterDevice("tenant-a", "cluster-1", "dev-1", fullCaps); err != nil {
		t.Fatalf("register: %v", err)
	}
	recs := []fleet.Telemetry{
		{ClusterID: "cluster-1", DeviceID: "dev-1", Source: "uplink-A", Seq: 3, Key: "mode", Value: "alpha"},
		{ClusterID: "cluster-1", DeviceID: "dev-1", Source: "uplink-B", Seq: 3, Key: "mode", Value: "beta"},
		{ClusterID: "cluster-1", DeviceID: "dev-1", Source: "uplink-C", Seq: 3, Key: "mode", Value: "gamma"},
	}
	return cp, recs
}

// snapshot 返回给定输入收敛后的 (最终值, 规范化冲突身份列表, 每条依据)。
func snapshot(t *testing.T, cp *controlplane.ControlPlane) (string, []string, []string) {
	t.Helper()
	v, ok, err := cp.State("tenant-a", "cluster-1", "dev-1", "mode")
	if err != nil || !ok {
		t.Fatalf("state: v=%q ok=%v err=%v", v, ok, err)
	}
	reps, err := cp.Conflicts("tenant-a", "cluster-1", "dev-1")
	if err != nil {
		t.Fatalf("conflicts: %v", err)
	}
	var ids, bases []string
	for _, r := range reps {
		ids = append(ids, fmt.Sprintf("seq=%d winner=%s loser=%s wsrc=%s lsrc=%s",
			r.Seq, r.Winner.Value, r.Loser.Value, r.Winner.Source, r.Loser.Source))
		bases = append(bases, r.Basis)
	}
	return v, ids, bases
}

// 两个以上不同值：枚举全部到达排列，最终值、败方集合与判定依据必须逐字一致。
func TestConflictThreeValuesAllPermutations(t *testing.T) {
	var wantV string
	var wantIDs, wantBases, wantReturned []string
	permute(3, func(p []int) {
		cp, recs := conflictSetup(t)
		batch := []fleet.Telemetry{recs[p[0]], recs[p[1]], recs[p[2]]}
		_, returned, err := cp.IngestDetailed("tenant-a", batch)
		if err != nil {
			t.Fatalf("ingest %v: %v", p, err)
		}
		var retIDs []string
		for _, r := range returned {
			retIDs = append(retIDs, fmt.Sprintf("seq=%d winner=%s loser=%s", r.Seq, r.Winner.Value, r.Loser.Value))
		}
		v, ids, bases := snapshot(t, cp)
		if wantIDs == nil {
			wantV, wantIDs, wantBases, wantReturned = v, ids, bases, retIDs
		} else {
			if v != wantV || !reflect.DeepEqual(ids, wantIDs) || !reflect.DeepEqual(bases, wantBases) {
				t.Fatalf("permutation %v differs:\n v=%q want %q\n ids=%v\nwant=%v", p, v, wantV, ids, wantIDs)
			}
			if !reflect.DeepEqual(retIDs, wantReturned) {
				t.Fatalf("permutation %v returned reports differ:\n %v\nwant %v", p, retIDs, wantReturned)
			}
		}
	})
	t.Logf("输入: 同位点 seq=3 的 alpha/beta/gamma 全部 6 种到达排列(整批一次提交); 判定依据: 最终值=%q, 本批回报=%v, 累积败方=%v, 依据逐字一致",
		wantV, wantReturned, wantIDs)
	if wantV != "gamma" {
		t.Fatalf("want gamma, got %q", wantV)
	}
	if len(wantIDs) != 2 {
		t.Fatalf("want 2 loser reports (alpha,beta), got %d: %v", len(wantIDs), wantIDs)
	}
	if len(wantReturned) != 2 {
		t.Fatalf("single batch must return only the 2 canonical reports (no intermediate), got %d: %v",
			len(wantReturned), wantReturned)
	}
}

// 不同批次切分（含 1 条一批、2+1、随机切分）下累积冲突集合一致；
// 整批重放不重复回报、不重复记录。
func TestConflictBatchSplittingAndReplay(t *testing.T) {
	var wantIDs []string
	check := func(name string, cp *controlplane.ControlPlane, recs []fleet.Telemetry, batches [][]fleet.Telemetry) {
		t.Helper()
		var totalReturned int
		for _, b := range batches {
			_, reps, err := cp.IngestDetailed("tenant-a", b)
			if err != nil {
				t.Fatalf("ingest %s: %v", name, err)
			}
			totalReturned += len(reps)
		}
		v, ids, _ := snapshot(t, cp)
		if wantIDs == nil {
			wantIDs = ids
		} else if !reflect.DeepEqual(ids, wantIDs) {
			t.Fatalf("%s conflicts differ: %v want %v", name, ids, wantIDs)
		}
		if v != "gamma" {
			t.Fatalf("%s value=%q want gamma", name, v)
		}
		// 整批重放：本次回报必须为空，累积记录数量不变。
		full := []fleet.Telemetry{recs[0], recs[1], recs[2]}
		_, replayReps, err := cp.IngestDetailed("tenant-a", full)
		if err != nil {
			t.Fatalf("replay %s: %v", name, err)
		}
		_, idsAfter, _ := snapshot(t, cp)
		if len(replayReps) != 0 || !reflect.DeepEqual(idsAfter, ids) {
			t.Fatalf("%s replay duplicated: returned=%d before=%v after=%v",
				name, len(replayReps), ids, idsAfter)
		}
		t.Logf("输入[%s]: %d 个批次; 判定依据: 累积败方=%v(本批共回报%d), 整批重放回报0条且不新增记录",
			name, len(batches), ids, totalReturned)
	}

	// 每条一批。
	{
		cp, recs := conflictSetup(t)
		check("one-by-one", cp, recs, [][]fleet.Telemetry{{recs[0]}, {recs[1]}, {recs[2]}})
	}
	// 2+1。
	{
		cp, recs := conflictSetup(t)
		check("two-plus-one", cp, recs, [][]fleet.Telemetry{{recs[0], recs[1]}, {recs[2]}})
	}
	// 随机乱序 + 随机切分，多跑若干遍。
	rng := rand.New(rand.NewSource(42))
	for round := 0; round < 10; round++ {
		cp, recs := conflictSetup(t)
		shuffled := append([]fleet.Telemetry(nil), recs...)
		rng.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		var batches [][]fleet.Telemetry
		i := 0
		for i < len(shuffled) {
			n := 1 + rng.Intn(2)
			if i+n > len(shuffled) {
				n = len(shuffled) - i
			}
			batches = append(batches, shuffled[i:i+n])
			i += n
		}
		check(fmt.Sprintf("random-round-%d", round), cp, recs, batches)
	}
}

// 多路并发上报、反复多轮：最终值、败方集合、每条依据都稳定，整批重放不重复回报。
func TestConflictConcurrentDeterministic(t *testing.T) {
	const rounds = 12
	const ways = 24
	var wantIDs, wantBases []string
	for round := 0; round < rounds; round++ {
		cp, recs := conflictSetup(t)
		var wg sync.WaitGroup
		for w := 0; w < ways; w++ {
			wg.Add(1)
			go func(w int) {
				defer wg.Done()
				// 每个值都有多路并发上报；内容完全相同的副本（含换通道重发）由去重层处理。
				r := recs[w%len(recs)]
				if _, _, err := cp.IngestDetailed("tenant-a", []fleet.Telemetry{r}); err != nil {
					t.Errorf("round %d way %d: %v", round, w, err)
				}
			}(w)
		}
		wg.Wait()
		// 整批重放：不重复回报、不新增记录。
		_, replay, err := cp.IngestDetailed("tenant-a", recs)
		if err != nil {
			t.Fatalf("replay round %d: %v", round, err)
		}
		v, ids, bases := snapshot(t, cp)
		if v != "gamma" || len(replay) != 0 {
			t.Fatalf("round %d: v=%q replayReturned=%d", round, v, len(replay))
		}
		if wantIDs == nil {
			wantIDs, wantBases = ids, bases
		} else if !reflect.DeepEqual(ids, wantIDs) || !reflect.DeepEqual(bases, wantBases) {
			t.Fatalf("round %d differs:\n ids=%v\nwant=%v", round, ids, wantIDs)
		}
	}
	t.Logf("输入: %d 路并发 × %d 轮（同位点 alpha/beta/gamma）+ 每轮整批重放; 判定依据: 恒收敛 gamma, 败方=%v, 重放回报0条",
		ways, rounds, wantIDs)
	if len(wantIDs) != 2 {
		t.Fatalf("want 2 loser reports, got %v", wantIDs)
	}
}

// permute 枚举 [0..n) 的全排列。
func permute(n int, fn func([]int)) {
	idx := make([]int, n)
	for i := range idx {
		idx[i] = i
	}
	var rec func(int)
	rec = func(k int) {
		if k == n {
			fn(append([]int(nil), idx...))
			return
		}
		for i := k; i < n; i++ {
			idx[k], idx[i] = idx[i], idx[k]
			rec(k + 1)
			idx[k], idx[i] = idx[i], idx[k]
		}
	}
	rec(0)
}
