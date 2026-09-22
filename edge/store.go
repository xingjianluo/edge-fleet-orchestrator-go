package edge

import (
	"encoding/json"
	"os"
	"sort"
	"sync"

	"github.com/xingjianluo/edge-fleet-orchestrator-go/fleet"
)

// storeData 是落盘的持久化结构。
type storeData struct {
	Pending map[string][]fleet.Telemetry `json:"pending"` // deviceID -> 待上传记录
	Acked   map[string]uint64            `json:"acked"`   // deviceID -> 已确认位点
}

// Store 是网关的本地持久化存储（JSON 文件 + 原子改名），
// 用于断网缓冲与进程重启后的位点恢复。
type Store struct {
	mu   sync.Mutex
	path string
	data storeData
}

// OpenStore 打开（或创建）位于 path 的本地存储并加载已有数据。
func OpenStore(path string) (*Store, error) {
	s := &Store{
		path: path,
		data: storeData{
			Pending: make(map[string][]fleet.Telemetry),
			Acked:   make(map[string]uint64),
		},
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return s, nil // 首次启动：空存储
		}
		return nil, err
	}
	if err := json.Unmarshal(raw, &s.data); err != nil {
		return nil, err
	}
	if s.data.Pending == nil {
		s.data.Pending = make(map[string][]fleet.Telemetry)
	}
	if s.data.Acked == nil {
		s.data.Acked = make(map[string]uint64)
	}
	return s, nil
}

// persist 原子写盘：先写临时文件再改名，避免崩溃产生半截文件。
func (s *Store) persist() error {
	raw, err := json.Marshal(&s.data)
	if err != nil {
		return err
	}
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, s.path)
}

// Append 追加一条待上传遥测（幂等：同 DeviceID+Seq 覆盖而非重复）。
func (s *Store) Append(t fleet.Telemetry) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	list := s.data.Pending[t.DeviceID]
	for i := range list {
		if list[i].Seq == t.Seq {
			list[i] = t // 同位点覆盖，保证幂等
			s.data.Pending[t.DeviceID] = list
			return s.persist()
		}
	}
	s.data.Pending[t.DeviceID] = append(list, t)
	return s.persist()
}

// Pending 返回位点大于 ackedSeq 的待上传记录（按 Seq 升序）。
func (s *Store) Pending(deviceID string, ackedSeq uint64) []fleet.Telemetry {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []fleet.Telemetry
	for _, t := range s.data.Pending[deviceID] {
		if t.Seq > ackedSeq {
			out = append(out, t)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Seq < out[j].Seq })
	return out
}

// Ack 记录设备已确认位点并持久化；同时清理已确认的缓冲记录。
func (s *Store) Ack(deviceID string, ackedSeq uint64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if ackedSeq > s.data.Acked[deviceID] {
		s.data.Acked[deviceID] = ackedSeq
	}
	kept := s.data.Pending[deviceID][:0]
	for _, t := range s.data.Pending[deviceID] {
		if t.Seq > ackedSeq {
			kept = append(kept, t)
		}
	}
	s.data.Pending[deviceID] = kept
	return s.persist()
}

// Checkpoint 读取设备已确认位点。
func (s *Store) Checkpoint(deviceID string) uint64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.data.Acked[deviceID]
}
