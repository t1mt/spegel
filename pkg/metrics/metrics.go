package metrics

import (
	"github.com/prometheus/client_golang/prometheus"

	"github.com/spegel-org/spegel/pkg/httpx"
)

var (
	// DefaultRegisterer and DefaultGatherer are the implementations of the
	// prometheus Registerer and Gatherer interfaces that all metrics operations
	// will use. They are variables so that packages that embed this library can
	// replace them at runtime, instead of having to pass around specific
	// registries.
	DefaultRegisterer = prometheus.DefaultRegisterer
	DefaultGatherer   = prometheus.DefaultGatherer
)

var (
	MirrorRequestsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "spegel_mirror_requests_total",
		Help: "Total number of mirror requests.",
	}, []string{"registry", "cache"})
	MirrorLastSuccessTimestamp = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "spegel_mirror_last_success_timestamp_seconds",
		Help: "The timestamp of the last successful mirror request.",
	})
	ResolveDurHistogram = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "spegel_resolve_duration_seconds",
		Help: "The duration for router to resolve a peer.",
	}, []string{"router"})
	AdvertisedImageTags = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "spegel_advertised_image_tags",
		Help: "Number of image tags advertised to be available.",
	}, []string{"registry"})
	AdvertisedImageDigests = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "spegel_advertised_image_digests",
		Help: "Number of image digests advertised to be available.",
	}, []string{"registry"})
	AdvertisedContentDigests = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "spegel_advertised_content_digests",
		Help: "Number of content digests advertised to be available.",
	}, []string{"registry"})
	RedisRouterCommandsTotal = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "spegel_redis_router_commands_total",
		Help: "Total number of Redis commands issued by the Redis router.",
	}, []string{"command", "result"})
	RedisRouterCleanupRemovedTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "spegel_redis_router_cleanup_removed_total",
		Help: "Total number of expired Redis router members removed.",
	})
	RedisRouterLookupCandidates = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "spegel_redis_router_lookup_candidates",
		Help:    "Number of Redis sorted-set members fetched per Redis router lookup.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 11),
	})
	RedisRouterLookupPeers = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "spegel_redis_router_lookup_peers",
		Help:    "Number of usable peers returned per Redis router lookup.",
		Buckets: prometheus.ExponentialBuckets(1, 2, 11),
	})
	RedisRouterPoolStats = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "spegel_redis_router_pool_stats",
		Help: "Redis router client connection pool statistics.",
	}, []string{"client", "stat"})
)

func Register() {
	DefaultRegisterer.MustRegister(MirrorRequestsTotal)
	DefaultRegisterer.MustRegister(MirrorLastSuccessTimestamp)
	DefaultRegisterer.MustRegister(ResolveDurHistogram)
	DefaultRegisterer.MustRegister(AdvertisedImageTags)
	DefaultRegisterer.MustRegister(AdvertisedImageDigests)
	DefaultRegisterer.MustRegister(AdvertisedContentDigests)
	DefaultRegisterer.MustRegister(RedisRouterCommandsTotal)
	DefaultRegisterer.MustRegister(RedisRouterCleanupRemovedTotal)
	DefaultRegisterer.MustRegister(RedisRouterLookupCandidates)
	DefaultRegisterer.MustRegister(RedisRouterLookupPeers)
	DefaultRegisterer.MustRegister(RedisRouterPoolStats)
	httpx.RegisterMetrics(DefaultRegisterer)
}
