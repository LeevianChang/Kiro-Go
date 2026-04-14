package pool

import (
	"fmt"
	"testing"
)

func TestConsistentHash_Basic(t *testing.T) {
	ch := NewConsistentHash(150)

	// 添加节点
	nodes := []string{"account-1", "account-2", "account-3"}
	for _, node := range nodes {
		ch.Add(node)
	}

	// 测试相同 key 返回相同节点
	key := "user-123"
	node1 := ch.Get(key)
	node2 := ch.Get(key)

	if node1 != node2 {
		t.Errorf("Expected same node for same key, got %s and %s", node1, node2)
	}

	// 测试不同 key 可能返回不同节点（使用更多 key 来确保分布）
	distribution := make(map[string]int)
	for i := 0; i < 100; i++ {
		k := fmt.Sprintf("user-%d", i)
		node := ch.Get(k)
		distribution[node]++
	}

	// 至少应该有 2 个节点被使用
	if len(distribution) < 2 {
		t.Errorf("Expected at least 2 nodes to be used, got %d", len(distribution))
	}

	t.Logf("Distribution: %v", distribution)
}

func TestConsistentHash_AddRemove(t *testing.T) {
	ch := NewConsistentHash(150)

	// 初始节点
	ch.Add("account-1")
	ch.Add("account-2")
	ch.Add("account-3")

	// 记录 100 个 key 的映射
	keys := make([]string, 100)
	originalMapping := make(map[string]string)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("user-%d", i)
		keys[i] = key
		originalMapping[key] = ch.Get(key)
	}

	// 添加新节点
	ch.Add("account-4")

	// 检查有多少映射发生了变化
	changed := 0
	for _, key := range keys {
		newNode := ch.Get(key)
		if newNode != originalMapping[key] {
			changed++
		}
	}

	// 一致性哈希的特性：添加节点后，大部分映射应该保持不变
	// 理论上只有约 25% 的 key 会重新映射（100 个 key，4 个节点）
	changeRate := float64(changed) / float64(len(keys))
	if changeRate > 0.4 {
		t.Errorf("Too many keys remapped after adding node: %.2f%% (expected < 40%%)", changeRate*100)
	}

	t.Logf("Added node: %.2f%% keys remapped", changeRate*100)

	// 移除节点
	ch.Remove("account-2")

	// 检查移除节点后的映射变化
	changedAfterRemove := 0
	for _, key := range keys {
		newNode := ch.Get(key)
		if newNode != originalMapping[key] {
			changedAfterRemove++
		}
	}

	changeRateAfterRemove := float64(changedAfterRemove) / float64(len(keys))
	t.Logf("Removed node: %.2f%% keys remapped from original", changeRateAfterRemove*100)
}

func TestConsistentHash_GetWithFallback(t *testing.T) {
	ch := NewConsistentHash(150)

	ch.Add("account-1")
	ch.Add("account-2")
	ch.Add("account-3")

	key := "user-123"
	primaryNode := ch.Get(key)

	// 排除主节点，应该返回下一个节点
	excludeIDs := map[string]bool{primaryNode: true}
	fallbackNode := ch.GetWithFallback(key, excludeIDs)

	if fallbackNode == primaryNode {
		t.Errorf("Fallback node should be different from primary node")
	}

	if fallbackNode == "" {
		t.Errorf("Should return a fallback node")
	}

	// 排除所有节点，应该返回空字符串
	excludeAll := map[string]bool{
		"account-1": true,
		"account-2": true,
		"account-3": true,
	}
	noNode := ch.GetWithFallback(key, excludeAll)
	if noNode != "" {
		t.Errorf("Expected empty string when all nodes excluded, got %s", noNode)
	}
}

func TestConsistentHash_Rebuild(t *testing.T) {
	ch := NewConsistentHash(150)

	// 初始构建
	nodes1 := []string{"account-1", "account-2", "account-3"}
	ch.Rebuild(nodes1)

	key := "user-123"
	node1 := ch.Get(key)

	// 重建相同节点，应该返回相同结果
	ch.Rebuild(nodes1)
	node2 := ch.Get(key)

	if node1 != node2 {
		t.Errorf("Expected same node after rebuild with same nodes, got %s and %s", node1, node2)
	}

	// 重建不同节点
	nodes2 := []string{"account-1", "account-2", "account-4"}
	ch.Rebuild(nodes2)

	// 验证 account-3 不再存在
	allNodes := ch.Nodes()
	for _, node := range allNodes {
		if node == "account-3" {
			t.Errorf("account-3 should not exist after rebuild")
		}
	}
}

func TestConsistentHash_Distribution(t *testing.T) {
	ch := NewConsistentHash(150)

	// 添加 5 个节点
	nodes := []string{"account-1", "account-2", "account-3", "account-4", "account-5"}
	for _, node := range nodes {
		ch.Add(node)
	}

	// 测试 1000 个 key 的分布
	distribution := make(map[string]int)
	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("user-%d", i)
		node := ch.Get(key)
		distribution[node]++
	}

	// 打印分布情况
	t.Logf("Distribution across %d nodes:", len(nodes))
	for node, count := range distribution {
		percentage := float64(count) / 10.0
		t.Logf("  %s: %d (%.1f%%)", node, count, percentage)
	}

	// 验证每个节点都被使用
	if len(distribution) != len(nodes) {
		t.Errorf("Expected all %d nodes to be used, got %d", len(nodes), len(distribution))
	}

	// 验证分布相对均匀（每个节点应该在 10%-30% 之间）
	for node, count := range distribution {
		percentage := float64(count) / 10.0
		if percentage < 10.0 || percentage > 30.0 {
			t.Errorf("Node %s has unbalanced distribution: %.1f%% (expected 10%%-30%%)", node, percentage)
		}
	}
}

func BenchmarkConsistentHash_Get(b *testing.B) {
	ch := NewConsistentHash(150)

	// 添加 10 个节点
	for i := 0; i < 10; i++ {
		ch.Add(fmt.Sprintf("account-%d", i))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("user-%d", i%1000)
		ch.Get(key)
	}
}

func BenchmarkConsistentHash_GetWithFallback(b *testing.B) {
	ch := NewConsistentHash(150)

	// 添加 10 个节点
	for i := 0; i < 10; i++ {
		ch.Add(fmt.Sprintf("account-%d", i))
	}

	excludeIDs := map[string]bool{
		"account-0": true,
		"account-1": true,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("user-%d", i%1000)
		ch.GetWithFallback(key, excludeIDs)
	}
}
