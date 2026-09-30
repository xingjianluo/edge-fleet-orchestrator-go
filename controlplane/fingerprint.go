package controlplane

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"sort"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/fleet"
)

// contentFingerprint 计算一次下发的“内容指纹”：
// 只取决于版本与制品属性（OS/架构/大小/依赖特性），与幂等键、提交者、时间无关。
// 依赖特性是集合语义：排序后参与摘要，保证 ["a","b"] 与 ["b","a"] 指纹相同。
func contentFingerprint(a fleet.Artifact) string {
	feats := append([]string(nil), a.RequireFeature...)
	sort.Strings(feats)
	h := sha256.New()
	h.Write([]byte("version=" + a.Version + "\n"))
	h.Write([]byte("os=" + a.OS + "\n"))
	h.Write([]byte("arch=" + a.Arch + "\n"))
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(a.SizeBytes))
	h.Write(size[:])
	for _, f := range feats {
		h.Write([]byte("feature=" + f + "\n"))
	}
	return hex.EncodeToString(h.Sum(nil))
}
