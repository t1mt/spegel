package routing

import (
	"context"
	"net/netip"
	"slices"
	"sync"
	"sync/atomic"
)

var _ Router = &MemoryRouter{}

type MemoryRouter struct {
	resolver map[string][]Peer
	self     Peer
	ready    atomic.Bool
	mx       sync.RWMutex
}

func NewMemoryRouter(resolver map[string][]Peer, self Peer) *MemoryRouter {
	r := &MemoryRouter{
		resolver: resolver,
		self:     self,
	}
	r.ready.Store(true)
	return r
}

func (m *MemoryRouter) SetReadiness(ready bool) {
	m.ready.Store(ready)
}

func (m *MemoryRouter) Ready(ctx context.Context) (bool, error) {
	return m.ready.Load(), nil
}

func (m *MemoryRouter) Lookup(ctx context.Context, key string, count int) (Balancer, error) {
	m.mx.RLock()
	defer m.mx.RUnlock()

	peers, ok := m.resolver[key]
	if !ok {
		return &RoundRobin{}, nil
	}

	rr := NewRoundRobin()
	for _, peer := range peers {
		rr.Add(peer)
	}
	return rr, nil
}

func (m *MemoryRouter) Advertise(ctx context.Context, keys []string) error {
	for _, key := range keys {
		m.Add(key, m.self)
	}
	return nil
}

func (m *MemoryRouter) Withdraw(ctx context.Context, keys []string) error {
	for _, key := range keys {
		m.Delete(key, m.self)
	}
	return nil
}

func (m *MemoryRouter) LocalAddresses() ([]netip.Addr, error) {
	return m.self.Addresses, nil
}

func (m *MemoryRouter) ListPeers() ([]Peer, error) {
	m.mx.RLock()
	defer m.mx.RUnlock()

	peerMap := make(map[string]Peer)
	for _, peers := range m.resolver {
		for _, peer := range peers {
			if peer.Host != m.self.Host {
				peerMap[peer.Host] = peer
			}
		}
	}

	result := make([]Peer, 0, len(peerMap))
	for _, peer := range peerMap {
		result = append(result, peer)
	}
	return result, nil
}

func (m *MemoryRouter) HostID() string {
	return m.self.Host
}

func (m *MemoryRouter) Add(key string, peer Peer) {
	m.mx.Lock()
	defer m.mx.Unlock()

	peers, ok := m.resolver[key]
	if !ok {
		m.resolver[key] = []Peer{peer}
		return
	}
	for _, p := range peers {
		if p.Host == peer.Host {
			return
		}
	}
	m.resolver[key] = append(peers, peer)
}

func (m *MemoryRouter) Delete(key string, peer Peer) {
	m.mx.Lock()
	defer m.mx.Unlock()

	peers, ok := m.resolver[key]
	if !ok {
		return
	}
	peers = slices.DeleteFunc(peers, func(v Peer) bool {
		return v.Host == peer.Host
	})
	m.resolver[key] = peers
}

func (m *MemoryRouter) Get(key string) ([]Peer, bool) {
	m.mx.RLock()
	defer m.mx.RUnlock()

	peers, ok := m.resolver[key]
	return peers, ok
}
