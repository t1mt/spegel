package routing

import (
	"context"
	"net/netip"
)

// Router implements the discovery of content.
type Router interface {
	// Ready returns true when the router is ready.
	Ready(ctx context.Context) (bool, error)
	// Lookup discovers peers with the given key and returns a balancer with the peers.
	Lookup(ctx context.Context, key string, count int) (Balancer, error)
	// Advertise broadcasts the availability of the given keys.
	Advertise(ctx context.Context, keys []string) error
	// Withdraw stops the broadcasting the availability of the given keys to the network.
	Withdraw(ctx context.Context, keys []string) error
	// LocalAddresses returns the local addresses of this router.
	LocalAddresses() ([]netip.Addr, error)
	// ListPeers returns all known peers.
	ListPeers() ([]Peer, error)
	// HostID returns the unique identifier for the host.
	HostID() string
}
