package controlplane

import (
	"sort"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/fleet"
)

// reconcileSameSeq 是冲突收敛的纯内容裁决：
// 同一 (设备,键,位点) 出现多个不同值时，取值字典序最大者为采纳值，其余淘汰。
// 规则只依赖值本身，与谁先到、批次如何切分、并发路数、重放次数完全无关。
func reconcileSameSeq(values map[string]bool) (winner string, losers []string) {
	all := make([]string, 0, len(values))
	for v := range values {
		all = append(all, v)
	}
	sort.Strings(all) // 全序排序：最大者获胜，输出也因此确定
	winner = all[len(all)-1]
	losers = append([]string(nil), all[:len(all)-1]...)
	return winner, losers
}

// buildConflictReports 把冲突聚合按 (键升序, 位点升序) 展开为稳定的报告列表。
// 同一组内容无论何时展开、展开几次，结果逐字节一致。
func buildConflictReports(m map[conflictKey]*conflictAgg) []fleet.ConflictReport {
	out := make([]fleet.ConflictReport, 0, len(m))
	for _, agg := range m {
		out = append(out, agg.report)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Key != out[j].Key {
			return out[i].Key < out[j].Key
		}
		return out[i].Seq < out[j].Seq
	})
	return out
}
