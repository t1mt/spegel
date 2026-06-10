package routing

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"math/rand/v2"
	"net/netip"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/spegel-org/spegel/internal/option"
	"github.com/spegel-org/spegel/pkg/metrics"
)

type RedisRouterConfig struct {
	AdvertiseBatchSize     int
	AdvertiseTTL           time.Duration
	ExpiredCleanupInterval uint64
	KeyPrefix              string
}

type RedisRouterOption = option.Option[RedisRouterConfig]

func WithRedisAdvertiseTTL(ttl time.Duration) RedisRouterOption {
	return func(cfg *RedisRouterConfig) error {
		cfg.AdvertiseTTL = ttl
		return nil
	}
}

func WithRedisAdvertiseBatchSize(size int) RedisRouterOption {
	return func(cfg *RedisRouterConfig) error {
		if size <= 0 {
			return errors.New("redis advertise batch size must be greater than 0")
		}
		cfg.AdvertiseBatchSize = size
		return nil
	}
}

func WithKeyPrefix(prefix string) RedisRouterOption {
	return func(cfg *RedisRouterConfig) error {
		cfg.KeyPrefix = prefix
		return nil
	}
}

func WithRedisExpiredCleanupInterval(interval uint64) RedisRouterOption {
	return func(cfg *RedisRouterConfig) error {
		cfg.ExpiredCleanupInterval = interval
		return nil
	}
}

var _ Router = &RedisRouter{}

const (
	redisDefaultAdvertiseBatchSize = 1000
	redisLookupMinBatchSize        = 8
	redisLookupMaxBatchSize        = 128
	redisLookupCandidateMultiple   = 4
	redisLookupMinCandidates       = 64
	redisLookupMaxCandidates       = 1024
)

type RedisRouter struct {
	clients                []redis.Cmdable
	advertiseBatchSize     int
	advertiseIP            string
	registryPort           uint16
	keyPrefix              string
	ttl                    time.Duration
	expiredCleanupInterval uint64
	cleanupCounter         atomic.Uint64
}

func NewRedisRouter(client redis.Cmdable, self Peer, opts ...RedisRouterOption) (*RedisRouter, error) {
	return NewRedisShardedRouter([]redis.Cmdable{client}, self, opts...)
}

func NewRedisShardedRouter(clients []redis.Cmdable, self Peer, opts ...RedisRouterOption) (*RedisRouter, error) {
	if len(clients) == 0 {
		return nil, errors.New("redis router requires at least one client")
	}
	cfg := RedisRouterConfig{
		AdvertiseBatchSize:     redisDefaultAdvertiseBatchSize,
		AdvertiseTTL:           15 * time.Minute,
		ExpiredCleanupInterval: 100,
		KeyPrefix:              "spegel",
	}
	if err := option.Apply(&cfg, opts...); err != nil {
		return nil, err
	}

	var advertiseIP string
	if len(self.Addresses) > 0 {
		advertiseIP = self.Addresses[0].String()
	} else {
		advertiseIP = self.Host
	}

	return &RedisRouter{
		clients:                clients,
		advertiseBatchSize:     cfg.AdvertiseBatchSize,
		advertiseIP:            advertiseIP,
		registryPort:           self.Metadata.RegistryPort,
		keyPrefix:              cfg.KeyPrefix,
		ttl:                    cfg.AdvertiseTTL,
		expiredCleanupInterval: cfg.ExpiredCleanupInterval,
	}, nil
}

func (r *RedisRouter) leaseKey(contentKey string) string {
	return fmt.Sprintf("%s:%s", r.keyPrefix, escapeRedisContentKey(contentKey))
}

func escapeRedisContentKey(contentKey string) string {
	contentKey = strings.ReplaceAll(contentKey, "%", "%25")
	return strings.ReplaceAll(contentKey, ":", "%3A")
}

func (r *RedisRouter) peerMember() string {
	return fmt.Sprintf("%s|%d", r.advertiseIP, r.registryPort)
}

func parsePeerMember(member string) (string, uint16, error) {
	ip, portStr, ok := strings.Cut(member, "|")
	if !ok {
		return "", 0, fmt.Errorf("invalid peer member %q", member)
	}
	port, err := strconv.ParseUint(portStr, 10, 16)
	if err != nil {
		return "", 0, fmt.Errorf("could not parse registry port %q: %w", portStr, err)
	}
	if _, err := netip.ParseAddr(ip); err != nil {
		return "", 0, fmt.Errorf("could not parse advertise ip %q: %w", ip, err)
	}
	return ip, uint16(port), nil
}

func (r *RedisRouter) Ready(ctx context.Context) (bool, error) {
	defer r.updateRedisPoolStats()
	for _, client := range r.clients {
		if client == nil {
			recordRedisCommand("ping", 1, errors.New("redis client is nil"))
			return false, nil
		}
		err := client.Ping(ctx).Err()
		recordRedisCommand("ping", 1, err)
		if err != nil {
			return false, nil
		}
	}
	return true, nil
}

func (r *RedisRouter) Lookup(ctx context.Context, key string, count int) (Balancer, error) {
	defer r.updateRedisPoolStats()
	log := logr.FromContextOrDiscard(ctx)

	lookupTimer := prometheus.NewTimer(metrics.ResolveDurHistogram.WithLabelValues("redis"))
	defer lookupTimer.ObserveDuration()

	leaseKey := r.leaseKey(key)
	client := r.clientForLeaseKey(leaseKey)
	if client == nil {
		return nil, errors.New("redis client is nil")
	}
	now := float64(time.Now().UnixMilli())
	nowStr := fmt.Sprintf("%f", now)

	peers := make([]Peer, 0)
	seen := map[string]struct{}{}
	fetchedMembers := 0

	if count > 0 {
		batchSize := lookupBatchSize(count)
		maxCandidates := lookupMaxCandidates(count)
		for offset := int64(0); int64(len(peers)) < maxCandidates && offset < maxCandidates; {
			limit := batchSize
			if remaining := maxCandidates - offset; limit > remaining {
				limit = remaining
			}
			members, err := client.ZRangeByScore(ctx, leaseKey, &redis.ZRangeBy{
				Min:    nowStr,
				Max:    "+inf",
				Offset: offset,
				Count:  limit,
			}).Result()
			recordRedisCommand("zrangebyscore", 1, err)
			if err != nil {
				return nil, fmt.Errorf("could not lookup key %s: %w", key, err)
			}
			fetchedMembers += len(members)
			if len(members) == 0 {
				break
			}
			peers = r.appendPeers(log, peers, seen, members, int(maxCandidates))
			offset += int64(len(members))
			if int64(len(members)) < limit {
				break
			}
		}
	} else {
		members, err := client.ZRangeByScore(ctx, leaseKey, &redis.ZRangeBy{
			Min: nowStr,
			Max: "+inf",
		}).Result()
		recordRedisCommand("zrangebyscore", 1, err)
		if err != nil {
			return nil, fmt.Errorf("could not lookup key %s: %w", key, err)
		}
		fetchedMembers = len(members)
		peers = r.appendPeers(log, make([]Peer, 0, len(members)), seen, members, 0)
	}

	r.cleanupExpired(ctx, client, leaseKey, nowStr, log)
	// Redis returns sorted-set members ordered by their TTL score. Shuffle the
	// bounded candidate set before applying count so repeated lookups do not
	// always select the same earliest-expiring peers and create pull hotspots.
	// shufflePeers(peers)
	if count > 0 && len(peers) > count {
		peers = peers[:count]
	}
	metrics.RedisRouterLookupCandidates.Observe(float64(fetchedMembers))
	metrics.RedisRouterLookupPeers.Observe(float64(len(peers)))

	rr := NewRoundRobin()
	for _, peer := range peers {
		rr.Add(peer)
	}
	return rr, nil
}

func (r *RedisRouter) appendPeers(log logr.Logger, peers []Peer, seen map[string]struct{}, members []string, limit int) []Peer {
	for _, member := range members {
		ip, port, err := parsePeerMember(member)
		if err != nil {
			log.Error(err, "could not parse peer member", "member", member)
			continue
		}
		if ip == r.advertiseIP {
			continue
		}
		if _, ok := seen[ip]; ok {
			continue
		}
		seen[ip] = struct{}{}
		addr, err := netip.ParseAddr(ip)
		if err != nil {
			log.Error(err, "could not parse peer member address", "member", member)
			continue
		}
		peers = append(peers, Peer{
			Host:      ip,
			Addresses: []netip.Addr{addr},
			Metadata:  PeerMetadata{RegistryPort: port},
		})
		if limit > 0 && len(peers) >= limit {
			break
		}
	}
	return peers
}

func (r *RedisRouter) cleanupExpired(ctx context.Context, client redis.Cmdable, leaseKey, now string, log logr.Logger) {
	if !r.shouldCleanupExpired() {
		return
	}
	removed, err := client.ZRemRangeByScore(ctx, leaseKey, "-inf", now).Result()
	recordRedisCommand("zremrangebyscore", 1, err)
	if err != nil {
		log.Error(err, "could not cleanup expired redis peers", "key", leaseKey)
		return
	}
	if removed > 0 {
		metrics.RedisRouterCleanupRemovedTotal.Add(float64(removed))
	}
}

func (r *RedisRouter) shouldCleanupExpired() bool {
	if r.expiredCleanupInterval == 0 {
		return false
	}
	return r.cleanupCounter.Add(1)%r.expiredCleanupInterval == 0
}

func lookupBatchSize(count int) int64 {
	batchSize := count * 2
	if batchSize < redisLookupMinBatchSize {
		batchSize = redisLookupMinBatchSize
	}
	if batchSize > redisLookupMaxBatchSize {
		batchSize = redisLookupMaxBatchSize
	}
	return int64(batchSize)
}

func lookupMaxCandidates(count int) int64 {
	maxCandidates := count * redisLookupCandidateMultiple
	if maxCandidates < redisLookupMinCandidates {
		maxCandidates = redisLookupMinCandidates
	}
	if maxCandidates > redisLookupMaxCandidates {
		maxCandidates = redisLookupMaxCandidates
	}
	if maxCandidates < count {
		maxCandidates = count
	}
	return int64(maxCandidates)
}

func (r *RedisRouter) Advertise(ctx context.Context, keys []string) error {
	defer r.updateRedisPoolStats()
	if len(keys) == 0 {
		return nil
	}
	log := logr.FromContextOrDiscard(ctx)

	expireAt := float64(time.Now().Add(r.ttl).UnixMilli())
	member := r.peerMember()
	now := fmt.Sprintf("%f", float64(time.Now().UnixMilli()))
	keyGroups := r.groupKeysByClient(keys)

	for _, group := range keyGroups {
		if len(group.keys) == 0 {
			continue
		}
		if group.client == nil {
			return errors.New("redis client is nil")
		}
		for start := 0; start < len(group.keys); start += r.advertiseBatchSize {
			end := start + r.advertiseBatchSize
			if end > len(group.keys) {
				end = len(group.keys)
			}
			pipe := group.client.Pipeline()
			zaddCount := 0
			cleanupCommands := []*redis.IntCmd{}
			for _, key := range group.keys[start:end] {
				leaseKey := r.leaseKey(key)
				pipe.ZAdd(ctx, leaseKey, redis.Z{Score: expireAt, Member: member})
				zaddCount++
				if r.shouldCleanupExpired() {
					cleanupCommands = append(cleanupCommands, pipe.ZRemRangeByScore(ctx, leaseKey, "-inf", now))
				}
			}
			_, err := pipe.Exec(ctx)
			recordRedisCommand("zadd", zaddCount, err)
			recordRedisCommand("zremrangebyscore", len(cleanupCommands), err)
			if err != nil {
				return fmt.Errorf("could not advertise keys: %w", err)
			}
			for _, cmd := range cleanupCommands {
				removed, err := cmd.Result()
				if err == nil && removed > 0 {
					metrics.RedisRouterCleanupRemovedTotal.Add(float64(removed))
				}
			}
		}
	}

	log.V(1).Info("advertised keys", "count", len(keys), "advertiseIP", r.advertiseIP)
	return nil
}

func (r *RedisRouter) Withdraw(ctx context.Context, keys []string) error {
	defer r.updateRedisPoolStats()
	if len(keys) == 0 {
		return nil
	}
	log := logr.FromContextOrDiscard(ctx)

	member := r.peerMember()
	keyGroups := r.groupKeysByClient(keys)
	for _, group := range keyGroups {
		if len(group.keys) == 0 {
			continue
		}
		if group.client == nil {
			return errors.New("redis client is nil")
		}
		for start := 0; start < len(group.keys); start += r.advertiseBatchSize {
			end := start + r.advertiseBatchSize
			if end > len(group.keys) {
				end = len(group.keys)
			}
			pipe := group.client.Pipeline()
			zremCount := 0
			for _, key := range group.keys[start:end] {
				leaseKey := r.leaseKey(key)
				pipe.ZRem(ctx, leaseKey, member)
				zremCount++
			}
			_, err := pipe.Exec(ctx)
			recordRedisCommand("zrem", zremCount, err)
			if err != nil {
				return fmt.Errorf("could not withdraw keys: %w", err)
			}
		}
	}

	log.V(1).Info("withdrew keys", "count", len(keys))
	return nil
}

type redisKeyGroup struct {
	client redis.Cmdable
	keys   []string
}

func (r *RedisRouter) groupKeysByClient(keys []string) []redisKeyGroup {
	groups := make([]redisKeyGroup, len(r.clients))
	for idx, client := range r.clients {
		groups[idx].client = client
	}
	for _, key := range keys {
		leaseKey := r.leaseKey(key)
		idx := redisShardIndex(leaseKey, len(r.clients))
		groups[idx].keys = append(groups[idx].keys, key)
	}
	return groups
}

func (r *RedisRouter) clientForLeaseKey(leaseKey string) redis.Cmdable {
	if len(r.clients) == 0 {
		return nil
	}
	return r.clients[redisShardIndex(leaseKey, len(r.clients))]
}

func redisShardIndex(key string, shardCount int) int {
	if shardCount <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))
	return int(h.Sum32() % uint32(shardCount))
}

func shufflePeers(peers []Peer) {
	// Fisher-Yates shuffle in-place. The caller already bounded the candidate
	// slice, so this stays O(candidate count) and avoids another allocation.
	for i := len(peers) - 1; i > 0; i-- {
		j := rand.IntN(i + 1)
		peers[i], peers[j] = peers[j], peers[i]
	}
}

func recordRedisCommand(command string, count int, err error) {
	if count == 0 {
		return
	}
	result := "success"
	if err != nil {
		result = "error"
	}
	metrics.RedisRouterCommandsTotal.WithLabelValues(command, result).Add(float64(count))
}

type redisPoolStatsProvider interface {
	PoolStats() *redis.PoolStats
}

func (r *RedisRouter) updateRedisPoolStats() {
	for idx, client := range r.clients {
		provider, ok := client.(redisPoolStatsProvider)
		if !ok {
			continue
		}
		stats := provider.PoolStats()
		if stats == nil {
			continue
		}
		clientLabel := strconv.Itoa(idx)
		metrics.RedisRouterPoolStats.WithLabelValues(clientLabel, "hits").Set(float64(stats.Hits))
		metrics.RedisRouterPoolStats.WithLabelValues(clientLabel, "misses").Set(float64(stats.Misses))
		metrics.RedisRouterPoolStats.WithLabelValues(clientLabel, "timeouts").Set(float64(stats.Timeouts))
		metrics.RedisRouterPoolStats.WithLabelValues(clientLabel, "total_conns").Set(float64(stats.TotalConns))
		metrics.RedisRouterPoolStats.WithLabelValues(clientLabel, "idle_conns").Set(float64(stats.IdleConns))
		metrics.RedisRouterPoolStats.WithLabelValues(clientLabel, "stale_conns").Set(float64(stats.StaleConns))
	}
}

func (r *RedisRouter) LocalAddresses() ([]netip.Addr, error) {
	addr, err := netip.ParseAddr(r.advertiseIP)
	if err != nil {
		return nil, err
	}
	return []netip.Addr{addr}, nil
}

func (r *RedisRouter) ListPeers() ([]Peer, error) {
	return []Peer{}, nil
}

func (r *RedisRouter) HostID() string {
	return r.advertiseIP
}
