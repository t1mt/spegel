package routing

import (
	"context"
	"fmt"
	"net"
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

func (r *RedisRouter) leaseKey(contentKey string) string {
	return fmt.Sprintf("%s:%s", r.keyPrefix, contentKey)
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

	leaseKey := r.leaseKey(key)
	now := float64(time.Now().UnixMilli())

	members, err := r.client.ZRangeByScore(ctx, leaseKey, &redis.ZRangeBy{
		Min: fmt.Sprintf("%f", now),
		Max: "+inf",
	}).Result()
	if err != nil {
		return nil, fmt.Errorf("could not lookup key %s: %w", key, err)
	}

	peers := make([]Peer, 0, len(members))
	seen := map[string]struct{}{}
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
		addr, ok := netip.AddrFromSlice(net.ParseIP(ip))
		if !ok {
			log.Error(fmt.Errorf("could not convert %q to netip.Addr", ip), "could not parse peer member", "member", member)
			continue
		}
		peers = append(peers, Peer{
			Host:      ip,
			Addresses: []netip.Addr{addr},
			Metadata:  PeerMetadata{RegistryPort: port},
		})
		if count > 0 && len(peers) >= count {
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

	expireAt := float64(time.Now().Add(r.ttl).UnixMilli())
	member := r.peerMember()

	pipe := r.client.Pipeline()
	for _, key := range keys {
		leaseKey := r.leaseKey(key)
		pipe.ZAdd(ctx, leaseKey, redis.Z{Score: expireAt, Member: member})
		pipe.ZRemRangeByScore(ctx, leaseKey, "-inf", fmt.Sprintf("%f", float64(time.Now().UnixMilli())))
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

	member := r.peerMember()
	pipe := r.client.Pipeline()
	for _, key := range keys {
		leaseKey := r.leaseKey(key)
		pipe.ZRem(ctx, leaseKey, member)
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
	return []Peer{}, nil
}

func (r *RedisRouter) HostID() string {
	return r.advertiseIP
}
