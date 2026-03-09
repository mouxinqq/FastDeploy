package handler

import (
	"context"
	"math"

	"github.com/PaddlePaddle/FastDeploy/router/pkg/logger"
)

// CachedProcessTokenSelectWorker selects the worker with the lowest effective token load,
// where effective_load = max(0, (raw_tokens + request_tokens) - matched_tokens_from_prefix_cache).
func CachedProcessTokenSelectWorker(ctx context.Context, workers []string, message string) (string, error) {
	if len(workers) == 0 {
		return "", nil
	}

	// Fallback: if prefillCache is unavailable, use process_tokens
	if DefaultScheduler == nil || DefaultScheduler.prefillCache == nil || DefaultScheduler.prefillCache.cache == nil {
		logger.Warn(ctx, "cached_process_token: prefillCache unavailable, fallback to process_tokens")
		return ProcessTokensSelectWorker(ctx, workers, message)
	}

	strategy := DefaultScheduler.prefillCache

	// Use charsToTokens (not remote tokenizer) to ensure scale consistency with estimateTokens
	tokens := charsToTokens(message)

	// Query prefix tree for matched token counts
	matchedTokens := strategy.cache.MatchTokenCount(tokens, toWorkerSet(workers))

	// Estimate tokens for the current request (same scale as tokenCounter)
	reqTokens := estimateTokens(message)

	// Select worker with minimum effective load
	var (
		selected     string
		minEffective uint64 = math.MaxUint64
		minRaw       uint64 = math.MaxUint64
	)
	for _, w := range workers {
		raw := GetOrCreateTokenCounter(ctx, w).Get()
		matched := matchedTokens[w] // 0 for workers with no cache hit

		// token_sum = existing in-flight tokens + this request's tokens
		tokenSum := raw + reqTokens
		var effective uint64
		if tokenSum > matched {
			effective = tokenSum - matched
		}

		logger.Debug(ctx, "cached_process_token: worker=%s raw=%d req=%d matched=%d effective=%d", w, raw, reqTokens, matched, effective)

		if effective < minEffective || (effective == minEffective && raw < minRaw) {
			minEffective = effective
			minRaw = raw
			selected = w
		}
	}

	// Record prefix into cache tree to maintain cache learning
	if selected != "" && len(tokens) > 0 {
		strategy.cache.Record(tokens, selected)
	}

	return selected, nil
}
