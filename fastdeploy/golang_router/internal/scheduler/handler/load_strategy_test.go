package handler

import (
	"context"
	"testing"

	"github.com/PaddlePaddle/FastDeploy/router/internal/config"
	"github.com/stretchr/testify/assert"
)

func TestProcessTokensSelectWorker(t *testing.T) {
	ctx := context.Background()

	// Setup test data
	workers := []string{"worker1", "worker2", "worker3"}

	// Initialize scheduler and token counters
	Init(&config.Config{
		Scheduler: config.SchedulerConfig{
			Policy: "process_tokens",
		},
	}, nil)

	t.Run("select worker with least tokens", func(t *testing.T) {
		// Set up token counts
		tc1 := GetOrCreateTokenCounter(ctx, "worker1")
		tc1.Add(100)
		tc2 := GetOrCreateTokenCounter(ctx, "worker2")
		tc2.Add(50) // Should be selected
		tc3 := GetOrCreateTokenCounter(ctx, "worker3")
		tc3.Add(200)

		selected, err := ProcessTokensSelectWorker(ctx, workers, "test message")
		assert.NoError(t, err)
		assert.Equal(t, "worker2", selected)
	})

	t.Run("empty workers list", func(t *testing.T) {
		selected, err := ProcessTokensSelectWorker(ctx, []string{}, "test")
		assert.NoError(t, err)
		assert.Equal(t, "", selected)
	})
}

func TestRequestNumSelectWorker(t *testing.T) {
	ctx := context.Background()
	workers := []string{"worker1", "worker2", "worker3"}

	Init(&config.Config{
		Scheduler: config.SchedulerConfig{
			Policy: "request_num",
		},
	}, nil)

	t.Run("select worker with least requests", func(t *testing.T) {
		// Set up request counts
		c1 := GetOrCreateCounter(ctx, "worker1")
		c1.Inc()
		c1.Inc()                                 // count = 2
		c2 := GetOrCreateCounter(ctx, "worker2") // count = 0 (should be selected)
		c3 := GetOrCreateCounter(ctx, "worker3")
		c3.Inc() // count = 1

		// Verify counts (use variables to avoid "declared and not used" error)
		assert.Equal(t, uint64(2), c1.Get())
		assert.Equal(t, uint64(0), c2.Get())
		assert.Equal(t, uint64(1), c3.Get())

		selected, err := RequestNumSelectWorker(ctx, workers, "test")
		assert.NoError(t, err)
		assert.Equal(t, "worker2", selected)
	})

	t.Run("empty workers list", func(t *testing.T) {
		selected, err := RequestNumSelectWorker(ctx, []string{}, "test")
		assert.NoError(t, err)
		assert.Equal(t, "", selected)
	})
}

func TestCachedProcessTokenSelectWorker(t *testing.T) {
	ctx := context.Background()

	t.Run("empty workers", func(t *testing.T) {
		Init(&config.Config{
			Scheduler: config.SchedulerConfig{
				Policy:         "cached_process_token",
				CacheBlockSize: 4,
			},
		}, nil)

		selected, err := CachedProcessTokenSelectWorker(ctx, []string{}, "test")
		assert.NoError(t, err)
		assert.Equal(t, "", selected)
	})

	t.Run("fallback when prefillCache nil", func(t *testing.T) {
		Init(&config.Config{
			Scheduler: config.SchedulerConfig{
				Policy: "cached_process_token",
			},
		}, nil)
		// Force prefillCache to nil
		DefaultScheduler.prefillCache = nil

		workers := []string{"worker1", "worker2"}
		tc1 := GetOrCreateTokenCounter(ctx, "worker1")
		tc1.Add(100)
		tc2 := GetOrCreateTokenCounter(ctx, "worker2")
		tc2.Add(50)

		selected, err := CachedProcessTokenSelectWorker(ctx, workers, "test")
		assert.NoError(t, err)
		assert.Equal(t, "worker2", selected) // Falls back to ProcessTokensSelectWorker
	})

	t.Run("no cache hit degrades to process_tokens behavior", func(t *testing.T) {
		Init(&config.Config{
			Scheduler: config.SchedulerConfig{
				Policy:         "cached_process_token",
				CacheBlockSize: 4,
			},
		}, nil)

		workers := []string{"worker1", "worker2", "worker3"}
		tc1 := GetOrCreateTokenCounter(ctx, "worker1")
		tc1.Add(200)
		tc2 := GetOrCreateTokenCounter(ctx, "worker2")
		tc2.Add(50)
		tc3 := GetOrCreateTokenCounter(ctx, "worker3")
		tc3.Add(100)

		// Message "abcd" = 4 runes, reqTokens = estimateTokens("abcd") = 8
		// No prior Record => no match, effective = raw + 8 for all
		selected, err := CachedProcessTokenSelectWorker(ctx, workers, "abcd")
		assert.NoError(t, err)
		assert.Equal(t, "worker2", selected) // Lowest raw tokens wins
	})

	t.Run("cache hit reduces effective load", func(t *testing.T) {
		Init(&config.Config{
			Scheduler: config.SchedulerConfig{
				Policy:         "cached_process_token",
				CacheBlockSize: 4,
			},
		}, nil)

		workers := []string{"worker1", "worker2"}

		// Use a message with enough runes to form blocks (blockSize=4)
		// "abcdefgh" = 8 runes => charsToTokens produces 8 ints => 2 block hashes
		// reqTokens = estimateTokens("abcdefgh") = 16, matched = 2 blocks * 4 * 2 = 16
		message := "abcdefgh"

		// Record this message prefix for worker1 (simulate prior request)
		tokens := charsToTokens(message)
		DefaultScheduler.prefillCache.cache.Record(tokens, "worker1")

		// worker1: (raw=200 + req=16) - matched=16 = effective=200
		// worker2: (raw=190 + req=16) - matched=0  = effective=206
		tc1 := GetOrCreateTokenCounter(ctx, "worker1")
		tc1.Add(200)
		tc2 := GetOrCreateTokenCounter(ctx, "worker2")
		tc2.Add(190)

		selected, err := CachedProcessTokenSelectWorker(ctx, workers, message)
		assert.NoError(t, err)
		assert.Equal(t, "worker1", selected) // worker1 has lower effective load
	})

	t.Run("tie-break by raw tokens", func(t *testing.T) {
		Init(&config.Config{
			Scheduler: config.SchedulerConfig{
				Policy:         "cached_process_token",
				CacheBlockSize: 4,
			},
		}, nil)

		workers := []string{"worker1", "worker2"}

		// Both workers have 0 effective load (no raw tokens, no cache)
		// worker2 should win if raw is also 0 for both (first encountered),
		// but let's make worker2 have lower raw
		tc1 := GetOrCreateTokenCounter(ctx, "worker1")
		tc1.Add(10)
		tc2 := GetOrCreateTokenCounter(ctx, "worker2")
		tc2.Add(5)

		// Short message "ab" = 2 runes => reqTokens = 4, no cache hit
		// worker1: effective = 10+4 = 14, worker2: effective = 5+4 = 9 => worker2 selected
		selected, err := CachedProcessTokenSelectWorker(ctx, workers, "ab")
		assert.NoError(t, err)
		assert.Equal(t, "worker2", selected)
	})

	t.Run("tree grows with usage and affects subsequent decisions", func(t *testing.T) {
		// Verify that calling CachedProcessTokenSelectWorker auto-Records into the tree,
		// and subsequent calls with the same prefix see cache hits.
		Init(&config.Config{
			Scheduler: config.SchedulerConfig{
				Policy:         "cached_process_token",
				CacheBlockSize: 4,
			},
		}, nil)

		workers := []string{"worker1", "worker2"}
		// message "abcdefgh" = 8 runes => 2 blocks => Record writes 2 block hashes
		message := "abcdefgh"

		// Round 1: no cache, both raw=0, reqTokens=16 => effective both=16, tie => first worker wins
		selected1, err := CachedProcessTokenSelectWorker(ctx, workers, message)
		assert.NoError(t, err)
		assert.Equal(t, "worker1", selected1)

		// After round 1, tree should have recorded "abcdefgh" -> worker1.
		// Verify tree is non-empty by checking MatchTokenCount directly.
		matchBefore := DefaultScheduler.prefillCache.cache.MatchTokenCount(
			charsToTokens(message), toWorkerSet(workers))
		assert.True(t, matchBefore["worker1"] > 0, "tree should have recorded worker1 prefix")
		assert.Equal(t, uint64(0), matchBefore["worker2"], "worker2 should have no cache hit")

		// Round 2: same prefix, give worker1 higher raw load.
		// worker1: raw=100, matched=16 (2 blocks * 4 * 2) => effective=84
		// worker2: raw=90,  matched=0                      => effective=90
		// worker1 should still win due to cache hit reducing effective load.
		tc1 := GetOrCreateTokenCounter(ctx, "worker1")
		tc1.Add(100)
		tc2 := GetOrCreateTokenCounter(ctx, "worker2")
		tc2.Add(90)

		selected2, err := CachedProcessTokenSelectWorker(ctx, workers, message)
		assert.NoError(t, err)
		assert.Equal(t, "worker1", selected2)

		// Round 3: increase worker1 raw load so much that cache hit can't save it.
		// worker1: raw=200, matched=16 => effective=184
		// worker2: raw=90,  matched=0  => effective=90
		// Now worker2 should win.
		tc1.Add(100) // raw now 200
		selected3, err := CachedProcessTokenSelectWorker(ctx, workers, message)
		assert.NoError(t, err)
		assert.Equal(t, "worker2", selected3)

		// After round 3, tree should also have recorded prefix -> worker2.
		matchAfter := DefaultScheduler.prefillCache.cache.MatchTokenCount(
			charsToTokens(message), toWorkerSet(workers))
		assert.True(t, matchAfter["worker2"] > 0, "tree should have recorded worker2 prefix after round 3")
	})
}
