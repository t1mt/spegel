package routing

import (
	"context"
	"fmt"
	"net/netip"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/redis/go-redis/v9"

	"github.com/spegel-org/spegel/internal/option"
	"github.com/spegel-org/spegel/pkg/metrics"
)

type RedisRouterConfig struct {
	AdvertiseTTL time.Duration
	KeyPrefix    string
}

type RedisRouterOption = option.Option[RedisRouterConfig]

func WithRedisAdvertiseTTL(ttl time.Duration) RedisRouterOption {
	return func(cfg *RedisRouterConfig) error {
		cfg.AdvertiseTTL = ttl
		return nil
	}
}

func WithKeyPrefix(prefix string) RedisRouterOption {
	return func(cfg *RedisRouterConfig) error {
		cfg.KeyPrefix = prefix
		return nil
	}
}

var _ Router = &RedisRouter{}

type RedisRouter struct {
	client       redis.Cmdable
	advertiseIP  string
	registryPort uint16
	keyPrefix    string
	ttl          time.Duration
}

func NewRedisRouter(client redis.Cmdable, self Peer, opts ...RedisRouterOption) (*RedisRouter, error) {
	cfg := RedisRouterConfig{
		AdvertiseTTL: 15 * time.Minute,
		KeyPrefix:    "spegel",
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
		client:       client,
		advertiseIP:  advertiseIP,
		registryPort: self.Metadata.RegistryPort,
		keyPrefix:    cfg.KeyPrefix,
		ttl:          cfg.AdvertiseTTL,
	}, nil
}

// routeKey returns the Redis key for a content key.
// Format: {prefix}:{contentKey}:[{advertiseIP}]
// IPv6 addresses are bracketed to avoid ambiguity with the `:` separator.
func (r *RedisRouter) routeKey(contentKey string) string {
	return fmt.Sprintf("%s:%s:[%s]", r.keyPrefix, contentKey, r.advertiseIP)
}

// peerKeyPattern returns the pattern to match all peer keys for a content key.
// Format: {prefix}:{contentKey}:*
func (r *RedisRouter) peerKeyPattern(contentKey string) string {
	return fmt.Sprintf("%s:%s:*", r.keyPrefix, contentKey)
}

// parsePeerFromKey parses peer information from Redis key.
// Key format: {prefix}:{contentKey}:[{advertiseIP}]
func (r *RedisRouter) parsePeerFromKey(key string) (Peer, error) {
	if !strings.HasSuffix(key, "]") {
		return Peer{}, fmt.Errorf("key %q does not end with ]", key)
	}
	open := strings.LastIndex(key, ":[")
	if open == -1 {
		return Peer{}, fmt.Errorf("key %q does not contain :[ip]", key)
	}
	advertiseIP := key[open+2 : len(key)-1]
	addr, err := netip.ParseAddr(advertiseIP)
	if err != nil {
		return Peer{}, fmt.Errorf("could not parse advertiseIP %q: %w", advertiseIP, err)
	}
	return Peer{
		Host:      advertiseIP,
		Addresses: []netip.Addr{addr},
		Metadata:  PeerMetadata{RegistryPort: r.registryPort},
	}, nil
}

func (r *RedisRouter) Ready(ctx context.Context) (bool, error) {
	err := r.client.Ping(ctx).Err()
	if err != nil {
		return false, nil
	}
	return true, nil
}

func (r *RedisRouter) Lookup(ctx context.Context, key string, count int) (Balancer, error) {
	log := logr.FromContextOrDiscard(ctx)

	lookupTimer := prometheus.NewTimer(metrics.ResolveDurHistogram.WithLabelValues("redis"))
	defer lookupTimer.ObserveDuration()

	pattern := r.peerKeyPattern(key)

	// Use SCAN to find all keys matching the pattern
	var cursor uint64
	peers := []Peer{}
	seen := make(map[string]bool)

	for {
		var keys []string
		var err error
		keys, cursor, err = r.client.Scan(ctx, cursor, pattern, 100).Result()
		if err != nil {
			return nil, fmt.Errorf("could not scan keys for pattern %s: %w", pattern, err)
		}

		for _, k := range keys {
			// Skip self
			if strings.HasSuffix(k, ":[" + r.advertiseIP + "]") {
				continue
			}

			// Check if key is expired (value is timestamp, check TTL)
			ttl, err := r.client.TTL(ctx, k).Result()
			if err != nil {
				log.Error(err, "could not get TTL for key", "key", k)
				continue
			}
			if ttl <= 0 {
				// Key expired or doesn't exist
				continue
			}

			peer, err := r.parsePeerFromKey(k)
			if err != nil {
				log.Error(err, "could not parse peer from key", "key", k)
				continue
			}

			if seen[peer.Host] {
				continue
			}
			seen[peer.Host] = true
			peers = append(peers, peer)

			if count > 0 && len(peers) >= count {
				break
			}
		}

		if count > 0 && len(peers) >= count {
			break
		}
		if cursor == 0 {
			break
		}
	}

	rr := NewRoundRobin()
	for _, peer := range peers {
		rr.Add(peer)
	}
	return rr, nil
}

func (r *RedisRouter) Advertise(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	log := logr.FromContextOrDiscard(ctx)

	now := time.Now().Unix()
	nowStr := strconv.FormatInt(now, 10)

	pipe := r.client.Pipeline()
	for _, key := range keys {
		rKey := r.routeKey(key)
		pipe.Set(ctx, rKey, nowStr, r.ttl)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("could not advertise keys: %w", err)
	}

	log.V(1).Info("advertised keys", "count", len(keys), "advertiseIP", r.advertiseIP)
	return nil
}

func (r *RedisRouter) Withdraw(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	log := logr.FromContextOrDiscard(ctx)

	pipe := r.client.Pipeline()
	for _, key := range keys {
		rKey := r.routeKey(key)
		pipe.Del(ctx, rKey)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("could not withdraw keys: %w", err)
	}

	log.V(1).Info("withdrew keys", "count", len(keys))
	return nil
}

func (r *RedisRouter) LocalAddresses() ([]netip.Addr, error) {
	addr, err := netip.ParseAddr(r.advertiseIP)
	if err != nil {
		return nil, err
	}
	return []netip.Addr{addr}, nil
}

func (r *RedisRouter) ListPeers() ([]Peer, error) {
	// Redis router doesn't maintain a peer list like P2P
	// Return empty list as peers are discovered on-demand via Lookup
	return []Peer{}, nil
}

func (r *RedisRouter) HostID() string {
	return r.advertiseIP
}
