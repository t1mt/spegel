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
	require.Equal(t, "peer-a", peer.Host)
	require.Equal(t, netip.MustParseAddr("10.0.0.1"), peer.Addresses[0])
	require.Equal(t, uint16(5000), peer.Metadata.RegistryPort)

	t.Cleanup(func() {
		client.Del(t.Context(), routerA.routeKey("image:latest"))
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
	other := Peer{Host: "other"}

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
		client.Del(t.Context(), routerSelf.routeKey("key1"))
	})
}

func TestRedisRouterLookupSkipsSelf(t *testing.T) {
	t.Parallel()
	client := newTestRedisClient(t)
	prefix := testKeyPrefix(t)

	self := Peer{
		Host:      "self-node",
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
		client.Del(t.Context(), r.routeKey("mykey"))
	})
}

func TestRedisRouterLookupCount(t *testing.T) {
	t.Parallel()
	client := newTestRedisClient(t)
	prefix := testKeyPrefix(t)

	observer := Peer{Host: "observer"}
	r, err := NewRedisRouter(client, observer, WithKeyPrefix(prefix))
	require.NoError(t, err)

	// Advertise from 3 different peers.
	for i := range 3 {
		peer := Peer{
			Host:      fmt.Sprintf("peer-%d", i),
			Addresses: []netip.Addr{netip.MustParseAddr(fmt.Sprintf("10.0.0.%d", i+1))},
			Metadata:  PeerMetadata{RegistryPort: 5000},
		}
		peerRouter, err := NewRedisRouter(client, peer, WithKeyPrefix(prefix))
		require.NoError(t, err)
		err = peerRouter.Advertise(t.Context(), []string{"shared-key"})
		require.NoError(t, err)
	}

	balancer, err := r.Lookup(t.Context(), "shared-key", 2)
	require.NoError(t, err)
	require.Equal(t, 2, balancer.Size())

	t.Cleanup(func() {
		client.Del(t.Context(), r.routeKey("shared-key"))
	})
}

func TestRedisRouterMultiplePeers(t *testing.T) {
	t.Parallel()
	client := newTestRedisClient(t)
	prefix := testKeyPrefix(t)

	observer := Peer{Host: "observer"}
	r, err := NewRedisRouter(client, observer, WithKeyPrefix(prefix))
	require.NoError(t, err)

	peers := []Peer{
		{Host: "a", Addresses: []netip.Addr{netip.MustParseAddr("10.0.0.1")}, Metadata: PeerMetadata{RegistryPort: 5000}},
		{Host: "b", Addresses: []netip.Addr{netip.MustParseAddr("10.0.0.2")}, Metadata: PeerMetadata{RegistryPort: 5000}},
		{Host: "c", Addresses: []netip.Addr{netip.MustParseAddr("10.0.0.3")}, Metadata: PeerMetadata{RegistryPort: 5000}},
	}
	for _, p := range peers {
		pr, err := NewRedisRouter(client, p, WithKeyPrefix(prefix))
		require.NoError(t, err)
		err = pr.Advertise(t.Context(), []string{"multi"})
		require.NoError(t, err)
	}

	balancer, err := r.Lookup(t.Context(), "multi", 0)
	require.NoError(t, err)
	require.Equal(t, 3, balancer.Size())

	t.Cleanup(func() {
		client.Del(t.Context(), r.routeKey("multi"))
	})
}

func TestRedisRouterIndependentTTL(t *testing.T) {
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
	observer := Peer{Host: "observer"}

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
	require.Equal(t, "peer-b", peer.Host)

	t.Cleanup(func() {
		client.Del(t.Context(), routerObserver.routeKey("shared"))
	})
}

func TestRedisRouterTTLExpiry(t *testing.T) {
	t.Parallel()
	client := newTestRedisClient(t)
	prefix := testKeyPrefix(t)

	self := Peer{
		Host:      "ttl-peer",
		Addresses: []netip.Addr{netip.MustParseAddr("10.0.0.1")},
		Metadata:  PeerMetadata{RegistryPort: 5000},
	}
	other := Peer{Host: "other"}

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
		client.Del(t.Context(), routerSelf.routeKey("expiring"))
	})
}

func TestMarshalUnmarshalPeer(t *testing.T) {
	t.Parallel()

	original := Peer{
		Host:      "test-host",
		Addresses: []netip.Addr{netip.MustParseAddr("192.168.1.1"), netip.MustParseAddr("fd00::1")},
		Metadata:  PeerMetadata{RegistryPort: 8080},
	}

	data, err := marshalPeer(original)
	require.NoError(t, err)

	restored, err := unmarshalPeer(data)
	require.NoError(t, err)

	require.Equal(t, original.Host, restored.Host)
	require.Equal(t, original.Addresses, restored.Addresses)
	require.Equal(t, original.Metadata, restored.Metadata)
}
