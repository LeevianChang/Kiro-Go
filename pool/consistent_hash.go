package pool

import (
	"hash/crc32"
	"sort"
	"sync"
)

// ConsistentHash 一致性哈希环
type ConsistentHash struct {
	mu           sync.RWMutex
	hashRing     []uint32          // 排序的哈希环
	nodeMap      map[uint32]string // 哈希值到节点ID的映射
	virtualNodes int               // 每个节点的虚拟节点数
}

// NewConsistentHash 创建一致性哈希实例
// virtualNodes: 每个真实节点对应的虚拟节点数量，建议 100-200
func NewConsistentHash(virtualNodes int) *ConsistentHash {
	if virtualNodes <= 0 {
		virtualNodes = 150 // 默认值
	}
	return &ConsistentHash{
		hashRing:     make([]uint32, 0),
		nodeMap:      make(map[uint32]string),
		virtualNodes: virtualNodes,
	}
}

// Add 添加节点到哈希环
func (ch *ConsistentHash) Add(nodeID string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	// 为每个真实节点创建多个虚拟节点
	for i := 0; i < ch.virtualNodes; i++ {
		virtualKey := ch.hashKey(nodeID, i)
		ch.hashRing = append(ch.hashRing, virtualKey)
		ch.nodeMap[virtualKey] = nodeID
	}

	// 重新排序哈希环
	sort.Slice(ch.hashRing, func(i, j int) bool {
		return ch.hashRing[i] < ch.hashRing[j]
	})
}

// Remove 从哈希环移除节点
func (ch *ConsistentHash) Remove(nodeID string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	// 移除所有虚拟节点
	newRing := make([]uint32, 0, len(ch.hashRing))
	for _, hash := range ch.hashRing {
		if ch.nodeMap[hash] != nodeID {
			newRing = append(newRing, hash)
		} else {
			delete(ch.nodeMap, hash)
		}
	}
	ch.hashRing = newRing
}

// Get 根据 key 获取对应的节点ID
func (ch *ConsistentHash) Get(key string) string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.hashRing) == 0 {
		return ""
	}

	hash := ch.hash(key)

	// 二分查找第一个 >= hash 的节点
	idx := sort.Search(len(ch.hashRing), func(i int) bool {
		return ch.hashRing[i] >= hash
	})

	// 如果没找到，返回第一个节点（环形）
	if idx == len(ch.hashRing) {
		idx = 0
	}

	return ch.nodeMap[ch.hashRing[idx]]
}

// GetWithFallback 获取节点，支持回退到下一个节点
// excludeIDs: 需要排除的节点ID列表（例如已冷却的节点）
func (ch *ConsistentHash) GetWithFallback(key string, excludeIDs map[string]bool) string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	if len(ch.hashRing) == 0 {
		return ""
	}

	hash := ch.hash(key)

	// 二分查找起始位置
	startIdx := sort.Search(len(ch.hashRing), func(i int) bool {
		return ch.hashRing[i] >= hash
	})
	if startIdx == len(ch.hashRing) {
		startIdx = 0
	}

	// 从起始位置开始查找可用节点
	seen := make(map[string]bool)
	for i := 0; i < len(ch.hashRing); i++ {
		idx := (startIdx + i) % len(ch.hashRing)
		nodeID := ch.nodeMap[ch.hashRing[idx]]

		// 跳过已经检查过的真实节点
		if seen[nodeID] {
			continue
		}
		seen[nodeID] = true

		// 检查是否需要排除
		if !excludeIDs[nodeID] {
			return nodeID
		}
	}

	return ""
}

// Rebuild 重建哈希环（用于批量更新节点）
func (ch *ConsistentHash) Rebuild(nodeIDs []string) {
	ch.mu.Lock()
	defer ch.mu.Unlock()

	ch.hashRing = make([]uint32, 0, len(nodeIDs)*ch.virtualNodes)
	ch.nodeMap = make(map[uint32]string)

	for _, nodeID := range nodeIDs {
		for i := 0; i < ch.virtualNodes; i++ {
			virtualKey := ch.hashKey(nodeID, i)
			ch.hashRing = append(ch.hashRing, virtualKey)
			ch.nodeMap[virtualKey] = nodeID
		}
	}

	sort.Slice(ch.hashRing, func(i, j int) bool {
		return ch.hashRing[i] < ch.hashRing[j]
	})
}

// hash 计算字符串的哈希值
func (ch *ConsistentHash) hash(key string) uint32 {
	return crc32.ChecksumIEEE([]byte(key))
}

// hashKey 计算虚拟节点的哈希值
func (ch *ConsistentHash) hashKey(nodeID string, index int) uint32 {
	// 使用节点ID和索引组合生成虚拟节点的唯一标识
	virtualKey := nodeID + "#" + string(rune(index))
	return ch.hash(virtualKey)
}

// Size 返回哈希环中的节点数量（包括虚拟节点）
func (ch *ConsistentHash) Size() int {
	ch.mu.RLock()
	defer ch.mu.RUnlock()
	return len(ch.hashRing)
}

// Nodes 返回所有真实节点ID
func (ch *ConsistentHash) Nodes() []string {
	ch.mu.RLock()
	defer ch.mu.RUnlock()

	seen := make(map[string]bool)
	nodes := make([]string, 0)

	for _, nodeID := range ch.nodeMap {
		if !seen[nodeID] {
			seen[nodeID] = true
			nodes = append(nodes, nodeID)
		}
	}

	return nodes
}
