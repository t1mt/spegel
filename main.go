package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"net/netip"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/alexflint/go-arg"
	"github.com/go-logr/logr"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/redis/go-redis/v9"
	"golang.org/x/sync/errgroup"

	"github.com/spegel-org/spegel/internal/cleanup"
	"github.com/spegel-org/spegel/internal/version"
	"github.com/spegel-org/spegel/pkg/metrics"
	"github.com/spegel-org/spegel/pkg/oci"
	"github.com/spegel-org/spegel/pkg/registry"
	"github.com/spegel-org/spegel/pkg/routing"
	"github.com/spegel-org/spegel/pkg/state"
	"github.com/spegel-org/spegel/pkg/web"
)

type VersionCmd struct {
	Format string `arg:"--format" default:"text" help:"Format to output version information in."`
}

type ConfigurationCmd struct {
	ContainerdRegistryConfigPath string   `arg:"--containerd-registry-config-path,env:CONTAINERD_REGISTRY_CONFIG_PATH" default:"/etc/containerd/certs.d" help:"Directory where mirror configuration is written."`
	MirroredRegistries           []string `arg:"--mirrored-registries,env:MIRRORED_REGISTRIES" help:"Registries that are configured to be mirrored, if slice is empty all registires are mirrored."`
	MirrorTargets                []string `arg:"--mirror-targets,env:MIRROR_TARGETS,required" help:"registries that are configured to act as mirrors."`
	ResolveTags                  bool     `arg:"--resolve-tags,env:RESOLVE_TAGS" default:"true" help:"When true Spegel will resolve tags to digests."`
	PrependExisting              bool     `arg:"--prepend-existing,env:PREPEND_EXISTING" default:"false" help:"When true existing mirror configuration will be kept and Spegel will prepend it's configuration."`
}

type BootstrapConfig struct {
	BootstrapKind        string   `arg:"--bootstrap-kind,env:BOOTSTRAP_KIND" help:"Kind of bootsrapper to use."`
	DNSBootstrapDomain   string   `arg:"--dns-bootstrap-domain,env:DNS_BOOTSTRAP_DOMAIN" help:"Domain to use when bootstrapping using DNS."`
	HTTPBootstrapAddr    string   `arg:"--http-bootstrap-addr,env:HTTP_BOOTSTRAP_ADDR" help:"Address to serve for HTTP bootstrap."`
	HTTPBootstrapPeer    string   `arg:"--http-bootstrap-peer,env:HTTP_BOOTSTRAP_PEER" help:"Peer to HTTP bootstrap with."`
	StaticBootstrapPeers []string `arg:"--static-bootstrap-peers,env:STATIC_BOOTSTRAP_PEERS" help:"Static list of peers to bootstrap with."`
}

type RegistryCmd struct {
	BootstrapConfig
	MetricsAddr           string           `arg:"--metrics-addr,env:METRICS_ADDR" default:":9090" help:"address to serve metrics."`
	ContainerdSock        string           `arg:"--containerd-sock,env:CONTAINERD_SOCK" default:"/run/containerd/containerd.sock" help:"Endpoint of containerd service."`
	ContainerdNamespace   string           `arg:"--containerd-namespace,env:CONTAINERD_NAMESPACE" default:"k8s.io" help:"Containerd namespace to fetch images from."`
	ContainerdContentPath string           `arg:"--containerd-content-path,env:CONTAINERD_CONTENT_PATH" default:"/var/lib/containerd/io.containerd.content.v1.content" help:"Path to Containerd content store"`
	DataDir               string           `arg:"--data-dir,env:DATA_DIR" default:"/var/lib/spegel" help:"Directory where Spegel persists data."`
	RouterAddr            string           `arg:"--router-addr,env:ROUTER_ADDR" default:":5001" help:"address to serve router."`
	RouterKind            string           `arg:"--router-kind,env:ROUTER_KIND" default:"p2p" help:"Kind of router to use (p2p or redis)."`
	RegistryAddr          string           `arg:"--registry-addr,env:REGISTRY_ADDR" default:":5000" help:"address to server image registry."`
	MirroredRegistries    []string         `arg:"--mirrored-registries,env:MIRRORED_REGISTRIES" help:"Registries that are configured to be mirrored, if slice is empty all registries are mirrored."`
	RegistryFilters       []*regexp.Regexp `arg:"--registry-filters,env:REGISTRY_FILTERS" help:"Regular expressions to filter out tags/registries, if slice is empty all registries/tags are resolved."`
	MirrorResolveTimeout  time.Duration    `arg:"--mirror-resolve-timeout,env:MIRROR_RESOLVE_TIMEOUT" default:"20ms" help:"Max duration spent finding a mirror."`
	MirrorResolveRetries  int              `arg:"--mirror-resolve-retries,env:MIRROR_RESOLVE_RETRIES" default:"3" help:"Max amount of mirrors to attempt."`
	DebugWebEnabled       bool             `arg:"--debug-web-enabled,env:DEBUG_WEB_ENABLED" default:"true" help:"When true enables debug web page."`

	RedisRouter
}

type RedisRouter struct {
	RedisAddr                   string        `arg:"--redis-addr,env:REDIS_ADDR" help:"Redis server address (required when router-kind is redis)."`
	RedisAddrs                  []string      `arg:"--redis-addrs,env:REDIS_ADDRS" help:"Redis server addresses. Overrides redis-addr; multiple standalone addresses are sharded by content key unless redis-cluster-enabled is true."`
	RedisUsername               string        `arg:"--redis-username,env:REDIS_USERNAME" help:"Redis username for ACL authentication."`
	RedisPassword               string        `arg:"--redis-password,env:REDIS_PASSWORD" help:"Redis password for authentication."`
	RedisKeyPrefix              string        `arg:"--redis-key-prefix,env:REDIS_KEY_PREFIX" default:"spegel" help:"Redis key prefix for namespacing."`
	RedisAdvertiseTTL           time.Duration `arg:"--redis-advertise-ttl,env:REDIS_ADVERTISE_TTL" default:"15m" help:"TTL for Redis advertised keys."`
	RedisAdvertiseIP            string        `arg:"--redis-advertise-ip,env:REDIS_ADVERTISE_IP" help:"Advertise router ip to the redis"`
	RedisAdvertiseBatchSize     int           `arg:"--redis-advertise-batch-size,env:REDIS_ADVERTISE_BATCH_SIZE" default:"1000" help:"Maximum Redis route keys per advertise pipeline batch."`
	RedisExpiredCleanupInterval uint64        `arg:"--redis-expired-cleanup-interval,env:REDIS_EXPIRED_CLEANUP_INTERVAL" default:"100" help:"Cleanup expired Redis sorted-set members every N cleanup opportunities, 0 disables opportunistic cleanup."`
	RedisReadvertiseJitter      time.Duration `arg:"--redis-readvertise-jitter,env:REDIS_READVERTISE_JITTER" default:"0" help:"Random jitter for Redis re-advertise interval, 0 uses 10% of redis-advertise-ttl/2."`
	RedisClusterEnabled         bool          `arg:"--redis-cluster-enabled,env:REDIS_CLUSTER_ENABLED" default:"false" help:"Use go-redis cluster mode for redis-addrs instead of standalone content-key sharding."`
	RedisSentinelAddrs          []string      `arg:"--redis-sentinel-addrs,env:REDIS_SENTINEL_ADDRS" help:"Redis Sentinel addresses. When set, redis-sentinel-master-name is required."`
	RedisSentinelMasterName     string        `arg:"--redis-sentinel-master-name,env:REDIS_SENTINEL_MASTER_NAME" help:"Redis Sentinel master name."`
	RedisSentinelUsername       string        `arg:"--redis-sentinel-username,env:REDIS_SENTINEL_USERNAME" help:"Redis Sentinel username for ACL authentication."`
	RedisSentinelPassword       string        `arg:"--redis-sentinel-password,env:REDIS_SENTINEL_PASSWORD" help:"Redis Sentinel password for authentication."`
	RedisTLSEnabled             bool          `arg:"--redis-tls-enabled,env:REDIS_TLS_ENABLED" default:"false" help:"Enable TLS for Redis connections."`
	RedisTLSCAFile              string        `arg:"--redis-tls-ca-file,env:REDIS_TLS_CA_FILE" help:"CA bundle file for Redis TLS verification."`
	RedisTLSCertFile            string        `arg:"--redis-tls-cert-file,env:REDIS_TLS_CERT_FILE" help:"Client certificate file for Redis mTLS."`
	RedisTLSKeyFile             string        `arg:"--redis-tls-key-file,env:REDIS_TLS_KEY_FILE" help:"Client private key file for Redis mTLS."`
	RedisTLSInsecureSkipVerify  bool          `arg:"--redis-tls-insecure-skip-verify,env:REDIS_TLS_INSECURE_SKIP_VERIFY" default:"false" help:"Skip Redis TLS certificate verification."`
	RedisPoolSize               int           `arg:"--redis-pool-size,env:REDIS_POOL_SIZE" default:"0" help:"Maximum Redis connections per Spegel process, 0 uses the go-redis default."`
	RedisMinIdleConns           int           `arg:"--redis-min-idle-conns,env:REDIS_MIN_IDLE_CONNS" default:"0" help:"Minimum idle Redis connections per Spegel process."`
	RedisDialTimeout            time.Duration `arg:"--redis-dial-timeout,env:REDIS_DIAL_TIMEOUT" default:"0" help:"Redis dial timeout, 0 uses the go-redis default."`
	RedisReadTimeout            time.Duration `arg:"--redis-read-timeout,env:REDIS_READ_TIMEOUT" default:"0" help:"Redis read timeout, 0 uses the go-redis default."`
	RedisWriteTimeout           time.Duration `arg:"--redis-write-timeout,env:REDIS_WRITE_TIMEOUT" default:"0" help:"Redis write timeout, 0 uses the go-redis default."`
}

type CleanupCmd struct {
	Addr                         string `arg:"--addr,required,env:ADDR" help:"address to run readiness probe on."`
	ContainerdRegistryConfigPath string `arg:"--containerd-registry-config-path,env:CONTAINERD_REGISTRY_CONFIG_PATH" default:"/etc/containerd/certs.d" help:"Directory where mirror configuration is written."`
}

type CleanupWaitCmd struct {
	ProbeEndpoint string        `arg:"--probe-endpoint,required,env:PROBE_ENDPOINT" help:"endpoint to probe cleanup jobs from."`
	Threshold     int           `arg:"--threshold,env:THRESHOLD" default:"3" help:"amount of consecutive successful probes to consider cleanup done."`
	Period        time.Duration `arg:"--period,env:PERIOD" default:"2s" help:"address to run readiness probe on."`
}

type Arguments struct {
	Version       *VersionCmd       `arg:"subcommand:version"`
	Configuration *ConfigurationCmd `arg:"subcommand:configuration"`
	Registry      *RegistryCmd      `arg:"subcommand:registry"`
	Cleanup       *CleanupCmd       `arg:"subcommand:cleanup"`
	CleanupWait   *CleanupWaitCmd   `arg:"subcommand:cleanup-wait"`
	LogLevel      slog.Level        `arg:"--log-level,env:LOG_LEVEL" default:"INFO" help:"Minimum log level to output. Value should be DEBUG, INFO, WARN, or ERROR."`
}

func main() {
	args := &Arguments{}
	arg.MustParse(args)

	err := run(context.Background(), args)
	if err != nil {
		os.Exit(1)
	}
}

func run(ctx context.Context, args *Arguments) error {
	ctx, cancel := signal.NotifyContext(ctx, syscall.SIGTERM)
	defer cancel()

	opts := slog.HandlerOptions{
		AddSource: true,
		Level:     args.LogLevel,
	}
	handler := slog.NewJSONHandler(os.Stderr, &opts)
	log := logr.FromSlogHandler(handler)
	ctx = logr.NewContext(ctx, log)

	err := func() error {
		switch {
		case args.Version != nil:
			return versionCommand(ctx, args.Version)
		case args.Configuration != nil:
			return configurationCommand(ctx, args.Configuration)
		case args.Registry != nil:
			return registryCommand(ctx, args.Registry)
		case args.Cleanup != nil:
			return cleanupCommand(ctx, args.Cleanup)
		case args.CleanupWait != nil:
			return cleanupWaitCommand(ctx, args.CleanupWait)
		default:
			return errors.New("unknown subcommand")
		}
	}()
	if err != nil {
		log.Error(err, "exit with error")
		return err
	}
	if args.Version != nil {
		log.Info("exit gracefully")
	}
	return nil
}

func versionCommand(_ context.Context, args *VersionCmd) error {
	versionInfo, err := version.Load()
	if err != nil {
		return err
	}
	switch args.Format {
	case "text":
		//nolint: forbidigo // Output already formatted so no need to log formatting.
		fmt.Printf("spegel version %s %s\n", versionInfo.Build.Version, versionInfo.Build.Commit)
		return nil
	case "json":
		b, err := json.Marshal(&versionInfo)
		if err != nil {
			return err
		}
		//nolint: forbidigo // Output already formatted so no need to log formatting.
		fmt.Print(string(b))
		return nil
	default:
		return fmt.Errorf("unknown output format %s", args.Format)
	}
}

func configurationCommand(ctx context.Context, args *ConfigurationCmd) error {
	username, password, err := loadBasicAuth()
	if err != nil {
		return err
	}
	err = oci.AddMirrorConfiguration(ctx, args.ContainerdRegistryConfigPath, args.MirroredRegistries, args.MirrorTargets, args.ResolveTags, args.PrependExisting, username, password)
	if err != nil {
		return err
	}
	return nil
}

func registryCommand(ctx context.Context, args *RegistryCmd) error {
	log := logr.FromContextOrDiscard(ctx)
	g, ctx := errgroup.WithContext(ctx)

	versionInfo, err := version.Load()
	if err != nil {
		return err
	}
	err = versionInfo.Preflight()
	if err != nil {
		return err
	}

	username, password, err := loadBasicAuth()
	if err != nil {
		return err
	}
	ociClient, err := oci.NewClient()
	if err != nil {
		return err
	}

	filters := []oci.Filter{}
	regFilter, err := oci.FilterForMirroredRegistries(args.MirroredRegistries)
	if err != nil {
		return err
	}
	if regFilter != nil {
		filters = append(filters, *regFilter)
	}
	for _, r := range args.RegistryFilters {
		filters = append(filters, oci.RegexFilter{Regex: r})
	}

	// OCI Store
	ociStore, err := oci.NewContainerd(ctx, args.ContainerdSock, args.ContainerdNamespace, oci.WithContentPath(args.ContainerdContentPath))
	if err != nil {
		return err
	}
	defer ociStore.Close()

	// Router
	_, registryPort, err := net.SplitHostPort(args.RegistryAddr)
	if err != nil {
		return err
	}

	stateOpts := []state.TrackerOption{
		state.WithRegistryFilters(filters),
	}
	var router routing.Router
	switch args.RouterKind {
	case "redis":
		redisRouter, err := createRedisRouter(ctx, args.RedisRouter, registryPort)
		if err != nil {
			return err
		}
		router = redisRouter
		readvertiseJitter := args.RedisReadvertiseJitter
		if readvertiseJitter < 0 {
			return errors.New("redis-readvertise-jitter must be greater than or equal to 0")
		}
		if readvertiseJitter == 0 {
			readvertiseJitter = args.RedisAdvertiseTTL / 20
		}
		stateOpts = append(stateOpts, state.WithReadvertiseInterval(args.RedisAdvertiseTTL/2), state.WithReadvertiseJitter(readvertiseJitter))
	case "p2p":
		bootstrapper, err := getBootstrapper(args.BootstrapConfig)
		if err != nil {
			return err
		}
		routerOpts := []routing.P2PRouterOption{
			routing.WithDataDir(args.DataDir),
		}
		router, err = routing.NewP2PRouter(ctx, args.RouterAddr, bootstrapper, registryPort, routerOpts...)
		if err != nil {
			return err
		}
		p2pRouter := router.(*routing.P2PRouter)
		g.Go(func() error {
			err := p2pRouter.Run(ctx)
			if err != nil {
				return err
			}
			return nil
		})
	default:
		return fmt.Errorf("unknown router kind %s", args.RouterKind)
	}

	// State tracking
	g.Go(func() error {
		err := state.Track(ctx, ociStore, router, stateOpts...)
		if err != nil && !errors.Is(err, context.Canceled) {
			return err
		}
		return nil
	})

	// Registry
	registryOpts := []registry.RegistryOption{
		registry.WithRegistryFilters(filters),
		registry.WithResolveTimeout(args.MirrorResolveTimeout),
		registry.WithBasicAuth(username, password),
		registry.WithOCIClient(ociClient),
	}
	reg, err := registry.NewRegistry(ociStore, router, registryOpts...)
	if err != nil {
		return err
	}
	regSrv := &http.Server{
		Addr:    args.RegistryAddr,
		Handler: reg.Handler(log),
	}
	g.Go(func() error {
		if err := regSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return regSrv.Shutdown(shutdownCtx)
	})

	// Metrics, pprof, and debug web
	metrics.Register()
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(metrics.DefaultGatherer, promhttp.HandlerOpts{}))
	mux.Handle("/debug/pprof/", http.HandlerFunc(pprof.Index))
	mux.Handle("/debug/pprof/profile", http.HandlerFunc(pprof.Profile))
	mux.Handle("/debug/pprof/trace", http.HandlerFunc(pprof.Trace))
	mux.Handle("/debug/pprof/symbol", http.HandlerFunc(pprof.Symbol))
	mux.Handle("/debug/pprof/heap", pprof.Handler("heap"))
	mux.Handle("/debug/pprof/allocs", pprof.Handler("allocs"))
	mux.Handle("/debug/pprof/goroutine", pprof.Handler("goroutine"))
	mux.Handle("/debug/pprof/threadcreate", pprof.Handler("threadcreate"))
	mux.Handle("/debug/pprof/block", pprof.Handler("block"))
	mux.Handle("/debug/pprof/mutex", pprof.Handler("mutex"))
	if args.DebugWebEnabled {
		webOpts := []web.WebOption{
			web.WithOCIClient(ociClient),
			web.WithRegistryFilters(filters),
		}
		mirror := &url.URL{
			Scheme: "http",
			Host:   args.RegistryAddr,
		}
		web, err := web.NewWeb(router, ociStore, reg, mirror, webOpts...)
		if err != nil {
			return err
		}
		mux.Handle("/debug/web/", web.Handler(log))
	}
	metricsSrv := &http.Server{
		Addr:    args.MetricsAddr,
		Handler: mux,
	}
	g.Go(func() error {
		if err := metricsSrv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
		return nil
	})
	g.Go(func() error {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		return metricsSrv.Shutdown(shutdownCtx)
	})

	log.Info("running Spegel", "registry", args.RegistryAddr, "router", args.RouterAddr)
	err = g.Wait()
	if err != nil {
		return err
	}
	return nil
}

func cleanupCommand(ctx context.Context, args *CleanupCmd) error {
	err := cleanup.Run(ctx, args.Addr, args.ContainerdRegistryConfigPath)
	if err != nil {
		return err
	}
	return nil
}

func cleanupWaitCommand(ctx context.Context, args *CleanupWaitCmd) error {
	err := cleanup.Wait(ctx, args.ProbeEndpoint, args.Period, args.Threshold)
	if err != nil {
		return err
	}
	return nil
}

func getBootstrapper(cfg BootstrapConfig) (routing.Bootstrapper, error) { //nolint: ireturn // Return type can be different structs.
	switch cfg.BootstrapKind {
	case "dns":
		return routing.NewDNSBootstrapper(cfg.DNSBootstrapDomain), nil
	case "http":
		return routing.NewHTTPBootstrapper(cfg.HTTPBootstrapAddr, cfg.HTTPBootstrapPeer), nil
	case "static":
		return routing.NewStaticBootstrapperFromStrings(cfg.StaticBootstrapPeers)
	default:
		return nil, fmt.Errorf("unknown bootstrap kind %s", cfg.BootstrapKind)
	}
}

func createRedisRouter(ctx context.Context, cfg RedisRouter, registryPort string) (*routing.RedisRouter, error) {
	if cfg.RedisAdvertiseBatchSize <= 0 {
		return nil, errors.New("redis-advertise-batch-size must be greater than 0")
	}
	if cfg.RedisPoolSize < 0 {
		return nil, errors.New("redis-pool-size must be greater than or equal to 0")
	}
	if cfg.RedisMinIdleConns < 0 {
		return nil, errors.New("redis-min-idle-conns must be greater than or equal to 0")
	}
	if cfg.RedisDialTimeout < 0 {
		return nil, errors.New("redis-dial-timeout must be greater than or equal to 0")
	}
	if cfg.RedisReadTimeout < 0 {
		return nil, errors.New("redis-read-timeout must be greater than or equal to 0")
	}
	if cfg.RedisWriteTimeout < 0 {
		return nil, errors.New("redis-write-timeout must be greater than or equal to 0")
	}
	routerIP := strings.TrimSpace(cfg.RedisAdvertiseIP)
	if routerIP == "" {
		return nil, errors.New("redis-advertise-ip is required when router-kind is redis")
	}

	tlsConfig, err := newRedisTLSConfig(cfg)
	if err != nil {
		return nil, err
	}
	clients, err := newRedisClients(cfg, tlsConfig)
	if err != nil {
		return nil, err
	}
	for idx, client := range clients {
		if err := client.Ping(ctx).Err(); err != nil {
			closeRedisClients(clients)
			return nil, fmt.Errorf("could not connect to Redis client %d: %w", idx, err)
		}
	}

	port, err := strconv.ParseUint(registryPort, 10, 16)
	if err != nil {
		closeRedisClients(clients)
		return nil, err
	}

	var addrs []netip.Addr
	raddr, err := netip.ParseAddr(routerIP)
	if err != nil {
		closeRedisClients(clients)
		return nil, err
	}
	addrs = append(addrs, raddr)

	self := routing.Peer{
		Host:      routerIP,
		Addresses: addrs,
		Metadata: routing.PeerMetadata{
			RegistryPort: uint16(port),
		},
	}

	router, err := routing.NewRedisShardedRouter(
		clients,
		self,
		routing.WithKeyPrefix(cfg.RedisKeyPrefix),
		routing.WithRedisAdvertiseTTL(cfg.RedisAdvertiseTTL),
		routing.WithRedisAdvertiseBatchSize(cfg.RedisAdvertiseBatchSize),
		routing.WithRedisExpiredCleanupInterval(cfg.RedisExpiredCleanupInterval),
	)
	if err != nil {
		closeRedisClients(clients)
		return nil, err
	}

	return router, nil
}

func newRedisClients(cfg RedisRouter, tlsConfig *tls.Config) ([]redis.Cmdable, error) {
	addrs := redisEndpointAddrs(cfg)
	sentinelAddrs := cleanRedisAddrs(cfg.RedisSentinelAddrs)

	if len(sentinelAddrs) > 0 || cfg.RedisSentinelMasterName != "" {
		if len(sentinelAddrs) == 0 {
			return nil, errors.New("redis-sentinel-addrs is required when redis-sentinel-master-name is set")
		}
		if cfg.RedisSentinelMasterName == "" {
			return nil, errors.New("redis-sentinel-master-name is required when redis-sentinel-addrs is set")
		}
		opts := redisUniversalOptions(cfg, tlsConfig, 1)
		opts.Addrs = sentinelAddrs
		opts.MasterName = cfg.RedisSentinelMasterName
		return []redis.Cmdable{redis.NewUniversalClient(opts)}, nil
	}

	if len(addrs) == 0 {
		return nil, errors.New("redis-addr or redis-addrs is required when router-kind is redis")
	}
	if cfg.RedisClusterEnabled {
		opts := redisUniversalOptions(cfg, tlsConfig, 1)
		opts.Addrs = addrs
		opts.IsClusterMode = true
		return []redis.Cmdable{redis.NewUniversalClient(opts)}, nil
	}
	if len(addrs) == 1 {
		opts := redisUniversalOptions(cfg, tlsConfig, 1)
		opts.Addrs = addrs
		return []redis.Cmdable{redis.NewUniversalClient(opts)}, nil
	}

	clients := make([]redis.Cmdable, 0, len(addrs))
	for _, addr := range addrs {
		opts := redisUniversalOptions(cfg, tlsConfig, len(addrs))
		opts.Addrs = []string{addr}
		clients = append(clients, redis.NewUniversalClient(opts))
	}
	return clients, nil
}

func redisEndpointAddrs(cfg RedisRouter) []string {
	addrs := cleanRedisAddrs(cfg.RedisAddrs)
	if len(addrs) > 0 {
		return addrs
	}
	addr := strings.TrimSpace(cfg.RedisAddr)
	if addr == "" {
		return nil
	}
	return []string{addr}
}

func cleanRedisAddrs(values []string) []string {
	addrs := []string{}
	for _, value := range values {
		for _, addr := range strings.Split(value, ",") {
			addr = strings.TrimSpace(addr)
			if addr != "" {
				addrs = append(addrs, addr)
			}
		}
	}
	return addrs
}

func redisUniversalOptions(cfg RedisRouter, tlsConfig *tls.Config, poolDivisor int) *redis.UniversalOptions {
	return &redis.UniversalOptions{
		Username:         cfg.RedisUsername,
		Password:         cfg.RedisPassword,
		SentinelUsername: cfg.RedisSentinelUsername,
		SentinelPassword: cfg.RedisSentinelPassword,
		PoolSize:         dividePositive(cfg.RedisPoolSize, poolDivisor),
		MinIdleConns:     dividePositive(cfg.RedisMinIdleConns, poolDivisor),
		DialTimeout:      cfg.RedisDialTimeout,
		ReadTimeout:      cfg.RedisReadTimeout,
		WriteTimeout:     cfg.RedisWriteTimeout,
		TLSConfig:        tlsConfig,
	}
}

func dividePositive(value, divisor int) int {
	if value <= 0 || divisor <= 1 {
		return value
	}
	result := value / divisor
	if value%divisor != 0 {
		result++
	}
	if result == 0 {
		return 1
	}
	return result
}

func newRedisTLSConfig(cfg RedisRouter) (*tls.Config, error) {
	if !cfg.RedisTLSEnabled &&
		cfg.RedisTLSCAFile == "" &&
		cfg.RedisTLSCertFile == "" &&
		cfg.RedisTLSKeyFile == "" &&
		!cfg.RedisTLSInsecureSkipVerify {
		return nil, nil
	}

	tlsConfig := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.RedisTLSInsecureSkipVerify, //nolint:gosec // Explicit user-controlled option for private Redis deployments.
	}
	if cfg.RedisTLSCAFile != "" {
		caPEM, err := os.ReadFile(cfg.RedisTLSCAFile)
		if err != nil {
			return nil, fmt.Errorf("could not read redis tls ca file: %w", err)
		}
		rootCAs, err := x509.SystemCertPool()
		if err != nil {
			rootCAs = x509.NewCertPool()
		}
		if ok := rootCAs.AppendCertsFromPEM(caPEM); !ok {
			return nil, errors.New("could not parse redis tls ca file")
		}
		tlsConfig.RootCAs = rootCAs
	}
	if cfg.RedisTLSCertFile != "" || cfg.RedisTLSKeyFile != "" {
		if cfg.RedisTLSCertFile == "" || cfg.RedisTLSKeyFile == "" {
			return nil, errors.New("redis-tls-cert-file and redis-tls-key-file must be set together")
		}
		cert, err := tls.LoadX509KeyPair(cfg.RedisTLSCertFile, cfg.RedisTLSKeyFile)
		if err != nil {
			return nil, fmt.Errorf("could not load redis tls client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}
	return tlsConfig, nil
}

func closeRedisClients(clients []redis.Cmdable) {
	for _, client := range clients {
		closer, ok := client.(interface{ Close() error })
		if !ok {
			continue
		}
		_ = closer.Close()
	}
}

func loadBasicAuth() (string, string, error) {
	dirPath := "/etc/secrets/basic-auth"
	username, err := os.ReadFile(filepath.Join(dirPath, "username"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", "", err
	}
	password, err := os.ReadFile(filepath.Join(dirPath, "password"))
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return "", "", err
	}
	return string(username), string(password), nil
}
