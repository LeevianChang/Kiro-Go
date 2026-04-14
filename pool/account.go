// Package pool 账号池管理
// 实现一致性哈希负载均衡、错误冷却、Token 刷新
package pool

import (
	"kiro-api-proxy/config"
	"sync"
	"sync/atomic"
	"time"
)

// AccountPool 账号池
type AccountPool struct {
	mu             sync.RWMutex
	accounts       []config.Account
	accountMap     map[string]*config.Account // ID -> Account 快速查找
	currentIndex   uint64
	cooldowns      map[string]time.Time // 账号冷却时间
	errorCounts    map[string]int       // 连续错误计数
	consistentHash *ConsistentHash      // 一致性哈希环
}

var (
	pool     *AccountPool
	poolOnce sync.Once
)

// GetPool 获取全局账号池单例
func GetPool() *AccountPool {
	poolOnce.Do(func() {
		pool = &AccountPool{
			accountMap:     make(map[string]*config.Account),
			cooldowns:      make(map[string]time.Time),
			errorCounts:    make(map[string]int),
			consistentHash: NewConsistentHash(150), // 每个节点150个虚拟节点
		}
		pool.Reload()
	})
	return pool
}

// Reload 从配置重新加载账号
// 构建加权列表：weight<=1 出现 1 次，weight>=2 出现 weight 次
// 同时重建一致性哈希环
func (p *AccountPool) Reload() {
	p.mu.Lock()
	defer p.mu.Unlock()
	enabled := config.GetEnabledAccounts()

	// 构建加权列表（用于轮询）
	var weighted []config.Account
	accountMap := make(map[string]*config.Account)
	nodeIDs := make([]string, 0, len(enabled))

	for _, a := range enabled {
		// 保存到 map 用于快速查找
		acc := a // 复制
		accountMap[a.ID] = &acc
		nodeIDs = append(nodeIDs, a.ID)

		// 构建加权列表
		w := a.Weight
		if w < 1 {
			w = 1
		}
		for j := 0; j < w; j++ {
			weighted = append(weighted, a)
		}
	}

	p.accounts = weighted
	p.accountMap = accountMap

	// 重建一致性哈希环
	p.consistentHash.Rebuild(nodeIDs)
}

// AccountSelection 账号选择结果
type AccountSelection struct {
	Account *config.Account
	Reason  string // 选择原因：primary, fallback:cooling, fallback:token_expiring, fallback:quota_exhausted, fallback:best_cooldown
}

// GetByHash 基于一致性哈希获取账号（实现账号亲和性）
// key 通常是 conversation ID 或 user ID
// 使用一致性哈希确保同一用户稳定路由到同一账号，即使账号增删也能保持大部分映射关系
func (p *AccountPool) GetByHash(key string) *config.Account {
	selection := p.GetByHashWithReason(key)
	if selection == nil {
		return nil
	}
	return selection.Account
}

// GetByHashWithReason 基于一致性哈希获取账号，并返回选择原因
func (p *AccountPool) GetByHashWithReason(key string) *AccountSelection {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.accountMap) == 0 {
		return nil
	}

	now := time.Now()

	// 首先获取首选账号（不考虑排除条件）
	primaryNodeID := p.consistentHash.Get(key)
	primaryAccount := p.accountMap[primaryNodeID]

	// 构建需要排除的账号列表，并记录排除原因
	excludeIDs := make(map[string]bool)
	excludeReasons := make(map[string]string)

	for id := range p.accountMap {
		acc := p.accountMap[id]

		// 排除冷却中的账号
		if cooldown, ok := p.cooldowns[id]; ok && now.Before(cooldown) {
			excludeIDs[id] = true
			excludeReasons[id] = "cooling"
			continue
		}

		// 排除即将过期的 Token
		if acc.ExpiresAt > 0 && time.Now().Unix() > acc.ExpiresAt-300 {
			excludeIDs[id] = true
			excludeReasons[id] = "token_expiring"
			continue
		}

		// 排除额度已用尽的账号
		if acc.UsageLimit > 0 && acc.UsageCurrent >= acc.UsageLimit {
			excludeIDs[id] = true
			excludeReasons[id] = "quota_exhausted"
			continue
		}
	}

	// 检查首选账号是否可用
	if !excludeIDs[primaryNodeID] {
		return &AccountSelection{
			Account: primaryAccount,
			Reason:  "primary",
		}
	}

	// 首选账号不可用，使用 fallback
	nodeID := p.consistentHash.GetWithFallback(key, excludeIDs)
	if nodeID == "" {
		// 无可用账号，返回冷却时间最短的（排除额度用尽的）
		acc := p.getBestCooldownAccount()
		if acc == nil {
			return nil
		}
		return &AccountSelection{
			Account: acc,
			Reason:  "fallback:best_cooldown",
		}
	}

	// 返回 fallback 账号及原因
	reason := "fallback:unknown"
	if primaryReason, ok := excludeReasons[primaryNodeID]; ok {
		reason = "fallback:" + primaryReason
	}

	return &AccountSelection{
		Account: p.accountMap[nodeID],
		Reason:  reason,
	}
}

// getBestCooldownAccount 获取冷却时间最短的账号（排除额度用尽的）
func (p *AccountPool) getBestCooldownAccount() *config.Account {
	var best *config.Account
	var earliest time.Time

	for id, acc := range p.accountMap {
		// 排除额度用尽的账号
		if acc.UsageLimit > 0 && acc.UsageCurrent >= acc.UsageLimit {
			continue
		}

		if cooldown, ok := p.cooldowns[id]; ok {
			if best == nil || cooldown.Before(earliest) {
				best = acc
				earliest = cooldown
			}
		} else {
			return acc
		}
	}

	return best
}

// GetNext 获取下一个可用账号（加权轮询）
func (p *AccountPool) GetNext() *config.Account {
	p.mu.RLock()
	defer p.mu.RUnlock()

	if len(p.accounts) == 0 {
		return nil
	}

	now := time.Now()
	n := len(p.accounts)
	seen := make(map[string]bool)

	// 加权轮询查找可用账号
	for i := 0; i < n; i++ {
		idx := atomic.AddUint64(&p.currentIndex, 1) % uint64(n)
		acc := &p.accounts[idx]

		if seen[acc.ID] {
			continue
		}

		// 跳过冷却中的账号
		if cooldown, ok := p.cooldowns[acc.ID]; ok && now.Before(cooldown) {
			seen[acc.ID] = true
			continue
		}

		// 跳过即将过期的 Token
		if acc.ExpiresAt > 0 && time.Now().Unix() > acc.ExpiresAt-300 {
			seen[acc.ID] = true
			continue
		}

		// 跳过额度已用尽的账号（适用于所有订阅类型）
		if acc.UsageLimit > 0 && acc.UsageCurrent >= acc.UsageLimit {
			seen[acc.ID] = true
			continue
		}

		return acc
	}

	// 无可用账号，返回冷却时间最短的（排除额度用尽的）
	var best *config.Account
	var earliest time.Time
	for i := range p.accounts {
		acc := &p.accounts[i]
		// 额度用尽的账号不作为 fallback
		if acc.UsageLimit > 0 && acc.UsageCurrent >= acc.UsageLimit {
			continue
		}
		if cooldown, ok := p.cooldowns[acc.ID]; ok {
			if best == nil || cooldown.Before(earliest) {
				best = acc
				earliest = cooldown
			}
		} else {
			return acc
		}
	}
	return best
}

// GetByID 根据 ID 获取账号
func (p *AccountPool) GetByID(id string) *config.Account {
	p.mu.RLock()
	defer p.mu.RUnlock()
	for i := range p.accounts {
		if p.accounts[i].ID == id {
			return &p.accounts[i]
		}
	}
	return nil
}

// RecordSuccess 记录请求成功，清除冷却
func (p *AccountPool) RecordSuccess(id string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.cooldowns, id)
	p.errorCounts[id] = 0
}

// RecordError 记录请求错误，设置冷却
func (p *AccountPool) RecordError(id string, isQuotaError bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.errorCounts[id]++

	if isQuotaError {
		// 配额错误，冷却 1 小时
		p.cooldowns[id] = time.Now().Add(time.Hour)
	} else if p.errorCounts[id] >= 3 {
		// 连续 3 次错误，冷却 1 分钟
		p.cooldowns[id] = time.Now().Add(time.Minute)
	}
}

// UpdateToken 更新账号 Token
func (p *AccountPool) UpdateToken(id, accessToken, refreshToken string, expiresAt int64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.accounts {
		if p.accounts[i].ID == id {
			p.accounts[i].AccessToken = accessToken
			if refreshToken != "" {
				p.accounts[i].RefreshToken = refreshToken
			}
			p.accounts[i].ExpiresAt = expiresAt
			break
		}
	}
}

// Count 返回账号总数
func (p *AccountPool) Count() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.accounts)
}

// AvailableCount 返回可用账号数
func (p *AccountPool) AvailableCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	now := time.Now()
	count := 0
	for _, acc := range p.accounts {
		if cooldown, ok := p.cooldowns[acc.ID]; ok && now.Before(cooldown) {
			continue
		}
		count++
	}
	return count
}

// UpdateStats 更新账号统计
func (p *AccountPool) UpdateStats(id string, tokens int, credits float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for i := range p.accounts {
		if p.accounts[i].ID == id {
			p.accounts[i].RequestCount++
			p.accounts[i].TotalTokens += tokens
			p.accounts[i].TotalCredits += credits
			p.accounts[i].LastUsed = time.Now().Unix()
			go config.UpdateAccountStats(id, p.accounts[i].RequestCount, p.accounts[i].ErrorCount, p.accounts[i].TotalTokens, p.accounts[i].TotalCredits, p.accounts[i].LastUsed)
			break
		}
	}
}

// GetAllAccounts 获取所有账号副本
func (p *AccountPool) GetAllAccounts() []config.Account {
	p.mu.RLock()
	defer p.mu.RUnlock()
	result := make([]config.Account, len(p.accounts))
	copy(result, p.accounts)
	return result
}

// DisableAccount 禁用账号并重新加载账号池
func (p *AccountPool) DisableAccount(id string, reason string) error {
	err := config.DisableAccount(id, reason)
	if err != nil {
		return err
	}
	p.Reload()
	return nil
}
