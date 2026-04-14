package proxy

import (
	"fmt"
	"kiro-api-proxy/config"
	"strings"
	"time"
)

// LogRequestInfo 记录请求关键信息（不包含完整 prompt）
func LogRequestInfo(conversationID, accountEmail, model string, messageCount, estimatedTokens int, stream bool, hasCacheControl bool, selectionReason string) {
	if !config.IsRequestLogEnabled() {
		return
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	streamMode := "non-stream"
	if stream {
		streamMode = "stream"
	}

	cacheStatus := "no-cache"
	if hasCacheControl {
		cacheStatus = "cached"
	}

	// 截取 conversationID 前8位作为用户标识
	userID := conversationID
	if len(conversationID) > 8 {
		userID = conversationID[:8]
	}

	// 格式化账号选择原因
	accountSelection := ""
	if selectionReason != "primary" {
		accountSelection = fmt.Sprintf(" | Selection: %s", selectionReason)
	}

	fmt.Printf("[%s] Request | User: %s | ConvID: %s | Account: %s%s | Model: %s | Messages: %d | Est.Tokens: %d | Mode: %s | Cache: %s\n",
		timestamp, userID, conversationID, accountEmail, accountSelection, model, messageCount, estimatedTokens, streamMode, cacheStatus)
}

// LogResponseInfo 记录响应关键信息
func LogResponseInfo(conversationID, accountEmail string, inputTokens, outputTokens int, credits float64, duration time.Duration, success bool) {
	if !config.IsRequestLogEnabled() {
		return
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	status := "SUCCESS"
	if !success {
		status = "FAILED"
	}

	fmt.Printf("[%s] Response | ConvID: %s | Account: %s | Status: %s | In: %d | Out: %d | Credits: %.2f | Duration: %dms\n",
		timestamp, conversationID, accountEmail, status, inputTokens, outputTokens, credits, duration.Milliseconds())
}

// LogErrorInfo 记录错误信息
func LogErrorInfo(conversationID, accountEmail, errorType, errorMsg string) {
	if !config.IsRequestLogEnabled() {
		return
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05")
	// 截断错误消息，避免过长
	if len(errorMsg) > 200 {
		errorMsg = errorMsg[:200] + "..."
	}

	fmt.Printf("[%s] Error | ConvID: %s | Account: %s | Type: %s | Message: %s\n",
		timestamp, conversationID, accountEmail, errorType, errorMsg)
}

// HasCacheControl 检测请求中是否使用了 cache_control
func HasCacheControl(req *ClaudeRequest) bool {
	// 检查 system prompt 中的 cache_control
	if req.System != nil {
		if blocks, ok := req.System.([]interface{}); ok {
			for _, b := range blocks {
				if block, ok := b.(map[string]interface{}); ok {
					if cacheControl, ok := block["cache_control"]; ok && cacheControl != nil {
						return true
					}
				}
			}
		}
	}

	// 检查 messages 中的 cache_control
	for _, msg := range req.Messages {
		switch content := msg.Content.(type) {
		case []interface{}:
			for _, item := range content {
				if block, ok := item.(map[string]interface{}); ok {
					if cacheControl, ok := block["cache_control"]; ok && cacheControl != nil {
						return true
					}
				}
			}
		}
	}

	return false
}

// GetLastUserMessage 获取最后一条用户消息的摘要（用于日志）
func GetLastUserMessage(messages []ClaudeMessage, maxLength int) string {
	if len(messages) == 0 {
		return ""
	}

	// 从后往前找第一条用户消息
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].Role == "user" {
			// 提取文本内容
			var textContent strings.Builder

			// Content 可能是 string 或 []ClaudeContentBlock
			switch content := messages[i].Content.(type) {
			case string:
				textContent.WriteString(content)
			case []interface{}:
				for _, item := range content {
					if block, ok := item.(map[string]interface{}); ok {
						if blockType, ok := block["type"].(string); ok && blockType == "text" {
							if text, ok := block["text"].(string); ok {
								textContent.WriteString(text)
								textContent.WriteString(" ")
							}
						}
					}
				}
			}

			text := strings.TrimSpace(textContent.String())
			if len(text) > maxLength {
				return text[:maxLength] + "..."
			}
			return text
		}
	}

	return ""
}

// LogRequestDetail 记录请求详细信息（可选，包含消息摘要）
func LogRequestDetail(conversationID, accountEmail, model string, messages []ClaudeMessage, systemPrompt string) {
	if !config.IsRequestLogEnabled() {
		return
	}

	timestamp := time.Now().Format("2006-01-02 15:04:05")

	// 获取最后一条用户消息的摘要
	lastMsg := GetLastUserMessage(messages, 100)
	if lastMsg == "" {
		lastMsg = "(no user message)"
	}

	// 系统提示摘要
	sysPromptSummary := ""
	if systemPrompt != "" {
		if len(systemPrompt) > 50 {
			sysPromptSummary = systemPrompt[:50] + "..."
		} else {
			sysPromptSummary = systemPrompt
		}
	}

	fmt.Printf("[%s] Detail | ConvID: %s | Account: %s | Model: %s\n", timestamp, conversationID, accountEmail, model)
	if sysPromptSummary != "" {
		fmt.Printf("  System: %s\n", sysPromptSummary)
	}
	fmt.Printf("  LastMsg: %s\n", lastMsg)
}
