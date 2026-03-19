package routing

import (
	"context"
	"encoding/json"
	"fmt"
	"net/netip"
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
	client    redis.Cmdable
	self      Peer
	keyPrefix string
	ttl       time.Duration
}

func NewRedisRouter(client redis.Cmdable, self Peer, opts ...RedisRouterOption) (*RedisRouter, error) {
	cfg := RedisRouterConfig{
		AdvertiseTTL: 15 * time.Minute,
		KeyPrefix:    "spegel",
	}
	if err := option.Apply(&cfg, opts...); err != nil {
		return nil, err
	}
	return &RedisRouter{
		client:    client,
		self:      self,
		keyPrefix: cfg.KeyPrefix,
		ttl:       cfg.AdvertiseTTL,
	}, nil
}

// routeKey returns the Redis key for a content key's peer set.
// Uses Sorted Set: member = JSON peer data, score = expireAt timestamp.
func (r *RedisRouter) routeKey(contentKey string) string {
	return fmt.Sprintf("%s:route:%s", r.keyPrefix, contentKey)
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

	rKey := r.routeKey(key)
	now := float64(time.Now().Unix())

	// Query only non-expired entries (score > now)
	vals, err := r.client.ZRangeByScore(ctx, rKey, &redis.ZRangeBy{
		Min: fmt.Sprintf("%f", now),
		Max: "+inf",
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("could not lookup key %s: %w", key, err)
	}

	rr := NewRoundRobin()
	added := 0
	for _, val := range vals {
		peer, err := unmarshalPeer(val)
		if err != nil {
			log.Error(err, "could not unmarshal peer data")
			continue
		}
		if peer.Host == r.self.Host {
			continue
		}
		rr.Add(peer)
		added++
		if count > 0 && added >= count {
			break
		}
	}
	return rr, nil
}

func (r *RedisRouter) Advertise(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	log := logr.FromContextOrDiscard(ctx)

	peerData, err := marshalPeer(r.self)
	if err != nil {
		return err
	}

	expireAt := float64(time.Now().Add(r.ttl).Unix())

	pipe := r.client.Pipeline()
	for _, key := range keys {
		rKey := r.routeKey(key)
		// ZADD with score = expireAt, member = peerData
		// If member exists, score is updated (refreshes TTL)
		pipe.ZAdd(ctx, rKey, redis.Z{Score: expireAt, Member: peerData})
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("could not advertise keys: %w", err)
	}
	log.V(1).Info("advertised keys", "count", len(keys))
	return nil
}

func (r *RedisRouter) Withdraw(ctx context.Context, keys []string) error {
	if len(keys) == 0 {
		return nil
	}
	log := logr.FromContextOrDiscard(ctx)

	peerData, err := marshalPeer(r.self)
	if err != nil {
		return err
	}

	pipe := r.client.Pipeline()
	for _, key := range keys {
		rKey := r.routeKey(key)
		pipe.ZRem(ctx, rKey, peerData)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("could not withdraw keys: %w", err)
	}
	log.V(1).Info("withdrew keys", "count", len(keys))
	return nil
}

type redisPeer struct {
	Host         string       `json:"host"`
	Addresses    []netip.Addr `json:"addresses"`
	RegistryPort uint16       `json:"registryPort"`
}

func marshalPeer(p Peer) (string, error) {
	rp := redisPeer{
		Host:         p.Host,
		Addresses:    p.Addresses,
		RegistryPort: p.Metadata.RegistryPort,
	}
	b, err := json.Marshal(rp)
	if err != nil {
		return "", fmt.Errorf("could not marshal peer: %w", err)
	}
	return string(b), nil
}

func unmarshalPeer(data string) (Peer, error) {
	var rp redisPeer
	if err := json.Unmarshal([]byte(data), &rp); err != nil {
		return Peer{}, fmt.Errorf("could not unmarshal peer: %w", err)
	}
	return Peer{
		Host:      rp.Host,
		Addresses: rp.Addresses,
		Metadata:  PeerMetadata{RegistryPort: rp.RegistryPort},
	}, nil
}

func (r *RedisRouter) LocalAddresses() ([]netip.Addr, error) {
	return r.self.Addresses, nil
}

func (r *RedisRouter) ListPeers() ([]Peer, error) {
	// Redis router doesn't maintain a peer list like P2P
	// Return empty list as peers are discovered on-demand via Lookup
	return []Peer{}, nil
}

func (r *RedisRouter) HostID() string {
	return r.self.Host
}
