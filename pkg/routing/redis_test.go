package routing

import (
	"fmt"
	"net/netip"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/require"
)

func newTestRedisClient(t *testing.T) *redis.Client {
	t.Helper()
	addr := os.Getenv("REDIS_ADDR")
	if addr == "" {
		addr = "localhost:6379"
	}
	password := os.Getenv("REDIS_PASSWORD")
	client := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
	})
	err := client.Ping(t.Context()).Err()
	if err != nil {
		t.Skipf("skipping Redis test: could not connect to Redis at %s: %v", addr, err)
	}
	t.Cleanup(func() {
		client.Close()
	})
	return client
}

func testKeyPrefix(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("test:%s", t.Name())
}

func TestRedisRouterReady(t *testing.T) {
	t.Parallel()
	client := newTestRedisClient(t)
	prefix := testKeyPrefix(t)

	r, err := NewRedisRouter(client, Peer{Host: "self"}, WithKeyPrefix(prefix))
	require.NoError(t, err)

	ok, err := r.Ready(t.Context())
	require.NoError(t, err)
	require.True(t, ok)
}

func TestRedisRouterAdvertiseAndLookup(t *testing.T) {
	t.Parallel()
	client := newTestRedisClient(t)
	prefix := testKeyPrefix(t)

	peerA := Peer{
		Host:      "peer-a",
		Addresses: []netip.Addr{netip.MustParseAddr("10.0.0.1")},
		Metadata:  PeerMetadata{RegistryPort: 5000},
	}
	peerB := Peer{
		Host:      "peer-b",
		Addresses: []netip.Addr{netip.MustParseAddr("10.0.0.2")},
		Metadata:  PeerMetadata{RegistryPort: 5000},
	}

	routerA, err := NewRedisRouter(client, peerA, WithKeyPrefix(prefix))
	require.NoError(t, err)
	routerB, err := NewRedisRouter(client, peerB, WithKeyPrefix(prefix))
	require.NoError(t, err)

	err = routerA.Advertise(t.Context(), []string{"image:latest"})
	require.NoError(t, err)

	balancer, err := routerB.Lookup(t.Context(), "image:latest", 1)
	require.NoError(t, err)
	require.Equal(t, 1, balancer.Size())

	peer, err := balancer.Next()
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1", peer.Host)
	require.Equal(t, netip.MustParseAddr("10.0.0.1"), peer.Addresses[0])
	require.Equal(t, uint16(5000), peer.Metadata.RegistryPort)

	t.Cleanup(func() {
		client.Del(t.Context(), routerA.leaseKey("image:latest"))
	})
}

func TestRedisRouterAdvertiseWithBatchSize(t *testing.T) {
	t.Parallel()
	client := newTestRedisClient(t)
	prefix := testKeyPrefix(t)

	self := Peer{
		Host:      "10.0.0.1",
		Addresses: []netip.Addr{netip.MustParseAddr("10.0.0.1")},
		Metadata:  PeerMetadata{RegistryPort: 5000},
	}
	other := Peer{
		Host:      "10.0.0.2",
		Addresses: []netip.Addr{netip.MustParseAddr("10.0.0.2")},
		Metadata:  PeerMetadata{RegistryPort: 5000},
	}

	routerSelf, err := NewRedisRouter(client, self, WithKeyPrefix(prefix), WithRedisAdvertiseBatchSize(2), WithRedisExpiredCleanupInterval(0))
	require.NoError(t, err)
	routerOther, err := NewRedisRouter(client, other, WithKeyPrefix(prefix))
	require.NoError(t, err)

	keys := []string{"batch-1", "batch-2", "batch-3", "batch-4", "batch-5"}
	err = routerSelf.Advertise(t.Context(), keys)
	require.NoError(t, err)

	for _, key := range keys {
		balancer, err := routerOther.Lookup(t.Context(), key, 1)
		require.NoError(t, err)
		require.Equal(t, 1, balancer.Size())
	}

	t.Cleanup(func() {
		for _, key := range keys {
			client.Del(t.Context(), routerSelf.leaseKey(key))
		}
	})
}

func TestRedisRouterWithdraw(t *testing.T) {
	t.Parallel()
	client := newTestRedisClient(t)
	prefix := testKeyPrefix(t)

	self := Peer{
		Host:      "self",
		Addresses: []netip.Addr{netip.MustParseAddr("10.0.0.1")},
		Metadata:  PeerMetadata{RegistryPort: 5000},
	}
	other := Peer{
		Host:      "other",
		Addresses: []netip.Addr{netip.MustParseAddr("10.0.0.2")},
		Metadata:  PeerMetadata{RegistryPort: 5000},
	}

	routerSelf, err := NewRedisRouter(client, self, WithKeyPrefix(prefix))
	require.NoError(t, err)
	routerOther, err := NewRedisRouter(client, other, WithKeyPrefix(prefix))
	require.NoError(t, err)

	err = routerSelf.Advertise(t.Context(), []string{"key1"})
	require.NoError(t, err)

	balancer, err := routerOther.Lookup(t.Context(), "key1", 1)
	require.NoError(t, err)
	require.Equal(t, 1, balancer.Size())

	err = routerSelf.Withdraw(t.Context(), []string{"key1"})
	require.NoError(t, err)

	balancer, err = routerOther.Lookup(t.Context(), "key1", 1)
	require.NoError(t, err)
	require.Equal(t, 0, balancer.Size())
	_, err = balancer.Next()
	require.ErrorIs(t, err, ErrNoNext)

	t.Cleanup(func() {
		client.Del(t.Context(), routerSelf.leaseKey("key1"))
	})
}

func TestRedisRouterLookupSkipsSelf(t *testing.T) {
	t.Parallel()
	client := newTestRedisClient(t)
	prefix := testKeyPrefix(t)

	self := Peer{
		Host:      "10.0.0.1",
		Addresses: []netip.Addr{netip.MustParseAddr("10.0.0.1")},
		Metadata:  PeerMetadata{RegistryPort: 5000},
	}

	r, err := NewRedisRouter(client, self, WithKeyPrefix(prefix))
	require.NoError(t, err)

	err = r.Advertise(t.Context(), []string{"mykey"})
	require.NoError(t, err)

	balancer, err := r.Lookup(t.Context(), "mykey", 1)
	require.NoError(t, err)
	require.Equal(t, 0, balancer.Size())

	t.Cleanup(func() {
		client.Del(t.Context(), r.leaseKey("mykey"))
	})
}

func TestRedisRouterLookupCount(t *testing.T) {
	t.Parallel()
	client := newTestRedisClient(t)
	prefix := testKeyPrefix(t)

	observer := Peer{
		Host:      "10.0.0.99",
		Addresses: []netip.Addr{netip.MustParseAddr("10.0.0.99")},
		Metadata:  PeerMetadata{RegistryPort: 5000},
	}
	r, err := NewRedisRouter(client, observer, WithKeyPrefix(prefix))
	require.NoError(t, err)

	peerRouters := []*RedisRouter{}
	// Advertise from 3 different peers.
	for i := range 3 {
		peer := Peer{
			Host:      fmt.Sprintf("10.0.0.%d", i+1),
			Addresses: []netip.Addr{netip.MustParseAddr(fmt.Sprintf("10.0.0.%d", i+1))},
			Metadata:  PeerMetadata{RegistryPort: 5000},
		}
		peerRouter, err := NewRedisRouter(client, peer, WithKeyPrefix(prefix))
		require.NoError(t, err)
		peerRouters = append(peerRouters, peerRouter)
		err = peerRouter.Advertise(t.Context(), []string{"shared-key"})
		require.NoError(t, err)
	}

	balancer, err := r.Lookup(t.Context(), "shared-key", 2)
	require.NoError(t, err)
	require.Equal(t, 2, balancer.Size())

	t.Cleanup(func() {
		for _, pr := range peerRouters {
			client.Del(t.Context(), pr.leaseKey("shared-key"))
		}
	})
}

func TestRedisRouterLimitedLookupFetchesAdditionalBatches(t *testing.T) {
	t.Parallel()
	client := newTestRedisClient(t)
	prefix := testKeyPrefix(t)

	observer := Peer{
		Host:      "10.0.0.99",
		Addresses: []netip.Addr{netip.MustParseAddr("10.0.0.99")},
		Metadata:  PeerMetadata{RegistryPort: 5000},
	}
	r, err := NewRedisRouter(client, observer, WithKeyPrefix(prefix))
	require.NoError(t, err)

	leaseKey := r.leaseKey("limited")
	score := float64(time.Now().Add(1 * time.Minute).UnixMilli())
	members := []redis.Z{
		{Score: score + 1, Member: "10.0.0.99|5000"},
		{Score: score + 2, Member: "bad-member-1"},
		{Score: score + 3, Member: "bad-member-2"},
		{Score: score + 4, Member: "bad-member-3"},
		{Score: score + 5, Member: "bad-member-4"},
		{Score: score + 6, Member: "bad-member-5"},
		{Score: score + 7, Member: "bad-member-6"},
		{Score: score + 8, Member: "bad-member-7"},
		{Score: score + 9, Member: "10.0.0.1|5000"},
		{Score: score + 10, Member: "10.0.0.2|5000"},
		{Score: score + 11, Member: "10.0.0.3|5000"},
	}
	err = client.ZAdd(t.Context(), leaseKey, members...).Err()
	require.NoError(t, err)

	balancer, err := r.Lookup(t.Context(), "limited", 3)
	require.NoError(t, err)
	require.Equal(t, 3, balancer.Size())

	t.Cleanup(func() {
		client.Del(t.Context(), leaseKey)
	})
}

func TestRedisRouterLookupCleansExpiredPeers(t *testing.T) {
	t.Parallel()
	client := newTestRedisClient(t)
	prefix := testKeyPrefix(t)

	observer := Peer{
		Host:      "10.0.0.99",
		Addresses: []netip.Addr{netip.MustParseAddr("10.0.0.99")},
		Metadata:  PeerMetadata{RegistryPort: 5000},
	}
	r, err := NewRedisRouter(client, observer, WithKeyPrefix(prefix), WithRedisExpiredCleanupInterval(1))
	require.NoError(t, err)

	leaseKey := r.leaseKey("cleanup")
	err = client.ZAdd(t.Context(), leaseKey,
		redis.Z{Score: float64(time.Now().Add(-1 * time.Minute).UnixMilli()), Member: "10.0.0.1|5000"},
		redis.Z{Score: float64(time.Now().Add(1 * time.Minute).UnixMilli()), Member: "10.0.0.2|5000"},
	).Err()
	require.NoError(t, err)

	balancer, err := r.Lookup(t.Context(), "cleanup", 1)
	require.NoError(t, err)
	require.Equal(t, 1, balancer.Size())

	_, err = client.ZScore(t.Context(), leaseKey, "10.0.0.1|5000").Result()
	require.ErrorIs(t, err, redis.Nil)
	card, err := client.ZCard(t.Context(), leaseKey).Result()
	require.NoError(t, err)
	require.Equal(t, int64(1), card)

	t.Cleanup(func() {
		client.Del(t.Context(), leaseKey)
	})
}

func TestRedisRouterMultiplePeers(t *testing.T) {
	t.Parallel()
	client := newTestRedisClient(t)
	prefix := testKeyPrefix(t)

	observer := Peer{
		Host:      "10.0.0.99",
		Addresses: []netip.Addr{netip.MustParseAddr("10.0.0.99")},
		Metadata:  PeerMetadata{RegistryPort: 5000},
	}
	r, err := NewRedisRouter(client, observer, WithKeyPrefix(prefix))
	require.NoError(t, err)

	peerRouters := []*RedisRouter{}
	peers := []Peer{
		{Host: "10.0.0.1", Addresses: []netip.Addr{netip.MustParseAddr("10.0.0.1")}, Metadata: PeerMetadata{RegistryPort: 5000}},
		{Host: "10.0.0.2", Addresses: []netip.Addr{netip.MustParseAddr("10.0.0.2")}, Metadata: PeerMetadata{RegistryPort: 5000}},
		{Host: "10.0.0.3", Addresses: []netip.Addr{netip.MustParseAddr("10.0.0.3")}, Metadata: PeerMetadata{RegistryPort: 5000}},
	}
	for _, p := range peers {
		pr, err := NewRedisRouter(client, p, WithKeyPrefix(prefix))
		require.NoError(t, err)
		peerRouters = append(peerRouters, pr)
		err = pr.Advertise(t.Context(), []string{"multi"})
		require.NoError(t, err)
	}

	balancer, err := r.Lookup(t.Context(), "multi", 0)
	require.NoError(t, err)
	require.Equal(t, 3, balancer.Size())

	t.Cleanup(func() {
		for _, pr := range peerRouters {
			client.Del(t.Context(), pr.leaseKey("multi"))
		}
	})
}

func TestRedisRouterIndependentTTL(t *testing.T) {
	t.Parallel()
	client := newTestRedisClient(t)
	prefix := testKeyPrefix(t)

	peerA := Peer{
		Host:      "10.0.0.1",
		Addresses: []netip.Addr{netip.MustParseAddr("10.0.0.1")},
		Metadata:  PeerMetadata{RegistryPort: 5000},
	}
	peerB := Peer{
		Host:      "10.0.0.2",
		Addresses: []netip.Addr{netip.MustParseAddr("10.0.0.2")},
		Metadata:  PeerMetadata{RegistryPort: 5000},
	}
	observer := Peer{
		Host:      "10.0.0.99",
		Addresses: []netip.Addr{netip.MustParseAddr("10.0.0.99")},
		Metadata:  PeerMetadata{RegistryPort: 5000},
	}

	// Peer A has short TTL (1 second)
	routerA, err := NewRedisRouter(client, peerA, WithKeyPrefix(prefix), WithRedisAdvertiseTTL(1*time.Second))
	require.NoError(t, err)

	// Peer B has long TTL (1 minute)
	routerB, err := NewRedisRouter(client, peerB, WithKeyPrefix(prefix), WithRedisAdvertiseTTL(1*time.Minute))
	require.NoError(t, err)

	routerObserver, err := NewRedisRouter(client, observer, WithKeyPrefix(prefix))
	require.NoError(t, err)

	// Both peers advertise the same key
	err = routerA.Advertise(t.Context(), []string{"shared"})
	require.NoError(t, err)
	err = routerB.Advertise(t.Context(), []string{"shared"})
	require.NoError(t, err)

	// Initially both peers are visible
	balancer, err := routerObserver.Lookup(t.Context(), "shared", 0)
	require.NoError(t, err)
	require.Equal(t, 2, balancer.Size())

	// Wait for peer A's TTL to expire
	time.Sleep(2 * time.Second)

	// Now only peer B should be visible (peer A expired independently)
	balancer, err = routerObserver.Lookup(t.Context(), "shared", 0)
	require.NoError(t, err)
	require.Equal(t, 1, balancer.Size())

	peer, err := balancer.Next()
	require.NoError(t, err)
	require.Equal(t, "10.0.0.2", peer.Host)

	t.Cleanup(func() {
		client.Del(t.Context(), routerA.leaseKey("shared"))
		client.Del(t.Context(), routerB.leaseKey("shared"))
	})
}

func TestRedisRouterTTLExpiry(t *testing.T) {
	t.Parallel()
	client := newTestRedisClient(t)
	prefix := testKeyPrefix(t)

	self := Peer{
		Host:      "10.0.0.1",
		Addresses: []netip.Addr{netip.MustParseAddr("10.0.0.1")},
		Metadata:  PeerMetadata{RegistryPort: 5000},
	}
	other := Peer{
		Host:      "10.0.0.2",
		Addresses: []netip.Addr{netip.MustParseAddr("10.0.0.2")},
		Metadata:  PeerMetadata{RegistryPort: 5000},
	}

	routerSelf, err := NewRedisRouter(client, self, WithKeyPrefix(prefix), WithRedisAdvertiseTTL(1*time.Second))
	require.NoError(t, err)
	routerOther, err := NewRedisRouter(client, other, WithKeyPrefix(prefix))
	require.NoError(t, err)

	err = routerSelf.Advertise(t.Context(), []string{"expiring"})
	require.NoError(t, err)

	balancer, err := routerOther.Lookup(t.Context(), "expiring", 1)
	require.NoError(t, err)
	require.Equal(t, 1, balancer.Size())

	time.Sleep(2 * time.Second)

	balancer, err = routerOther.Lookup(t.Context(), "expiring", 1)
	require.NoError(t, err)
	require.Equal(t, 0, balancer.Size())

	t.Cleanup(func() {
		client.Del(t.Context(), routerSelf.leaseKey("expiring"))
	})
}

func TestRedisRouterParsePeerMember(t *testing.T) {
	t.Parallel()

	ip, port, err := parsePeerMember("10.0.0.1|5000")
	require.NoError(t, err)
	require.Equal(t, "10.0.0.1", ip)
	require.Equal(t, uint16(5000), port)

	_, _, err = parsePeerMember("bad-member")
	require.Error(t, err)

	_, _, err = parsePeerMember("fd00::1|5000")
	require.NoError(t, err)
}

func TestRedisRouterAdvertiseBatchSizeValidation(t *testing.T) {
	t.Parallel()

	_, err := NewRedisRouter(nil, Peer{}, WithRedisAdvertiseBatchSize(0))
	require.Error(t, err)
}

func TestRedisRouterCleanupInterval(t *testing.T) {
	t.Parallel()

	r, err := NewRedisRouter(nil, Peer{}, WithRedisExpiredCleanupInterval(3))
	require.NoError(t, err)
	require.False(t, r.shouldCleanupExpired())
	require.False(t, r.shouldCleanupExpired())
	require.True(t, r.shouldCleanupExpired())
}
