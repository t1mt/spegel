package registry

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"

	"github.com/spegel-org/spegel/internal/buffer"
	"github.com/spegel-org/spegel/internal/mux"
	"github.com/spegel-org/spegel/pkg/metrics"
	"github.com/spegel-org/spegel/pkg/oci"
	"github.com/spegel-org/spegel/pkg/routing"
	"github.com/spegel-org/spegel/pkg/throttle"
)

const (
	MirroredHeaderKey = "X-Spegel-Mirrored"
)

type Registry struct {
	bufferPool       *buffer.BufferPool
	log              logr.Logger
	throttler        *throttle.Throttler
	ociClient        oci.Client
	router           routing.Router
	transport        http.RoundTripper
	localAddr        string
	resolveRetries   int
	resolveTimeout   time.Duration
	resolveLatestTag bool
}

type Option func(*Registry)

func WithResolveRetries(resolveRetries int) Option {
	return func(r *Registry) {
		r.resolveRetries = resolveRetries
	}
}

func WithResolveLatestTag(resolveLatestTag bool) Option {
	return func(r *Registry) {
		r.resolveLatestTag = resolveLatestTag
	}
}

func WithResolveTimeout(resolveTimeout time.Duration) Option {
	return func(r *Registry) {
		r.resolveTimeout = resolveTimeout
	}
}

func WithTransport(transport http.RoundTripper) Option {
	return func(r *Registry) {
		r.transport = transport
	}
}

func WithLocalAddress(localAddr string) Option {
	return func(r *Registry) {
		r.localAddr = localAddr
	}
}

func WithBlobSpeed(blobSpeed throttle.Byterate) Option {
	return func(r *Registry) {
		r.throttler = throttle.NewThrottler(blobSpeed)
	}
}

func WithLogger(log logr.Logger) Option {
	return func(r *Registry) {
		r.log = log
	}
}

func NewRegistry(ociClient oci.Client, router routing.Router, opts ...Option) *Registry {
	r := &Registry{
		ociClient:        ociClient,
		router:           router,
		resolveRetries:   3,
		resolveTimeout:   20 * time.Millisecond,
		resolveLatestTag: true,
		bufferPool:       buffer.NewBufferPool(),
	}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

func (r *Registry) Server(addr string) (*http.Server, error) {
	m, err := mux.NewServeMux(r.handle)
	if err != nil {
		return nil, err
	}
	srv := &http.Server{
		Addr:    addr,
		Handler: m,
	}
	return srv, nil
}

func (r *Registry) handle(rw mux.ResponseWriter, req *http.Request) {
	start := time.Now()
	handler := ""
	path := req.URL.Path
	if strings.HasPrefix(path, "/v2") {
		path = "/v2/*"
	}
	defer func() {
		latency := time.Since(start)
		statusCode := strconv.FormatInt(int64(rw.Status()), 10)

		metrics.HttpRequestsInflight.WithLabelValues(path).Add(-1)
		metrics.HttpRequestDurHistogram.WithLabelValues(path, req.Method, statusCode).Observe(latency.Seconds())
		metrics.HttpResponseSizeHistogram.WithLabelValues(path, req.Method, statusCode).Observe(float64(rw.Size()))

		// Ignore logging requests to healthz to reduce log noise
		if req.URL.Path == "/healthz" {
			return
		}

		kvs := []interface{}{
			"path", req.URL.Path,
			"status", rw.Status(),
			"method", req.Method,
			"latency", latency.String(),
			"length", rw.Header().Get("Content-Length"),
			"ip", getClientIP(req),
			"handler", handler,
		}
		if rw.Status() >= 200 && rw.Status() < 300 {
			r.log.Info("", kvs...)
			return
		}
		r.log.Error(rw.Error(), "", kvs...)
	}()
	metrics.HttpRequestsInflight.WithLabelValues(path).Add(1)

	if req.URL.Path == "/healthz" && req.Method == http.MethodGet {
		r.readyHandler(rw, req)
		handler = "ready"
		return
	}
	if strings.HasPrefix(req.URL.Path, "/v2") && (req.Method == http.MethodGet || req.Method == http.MethodHead) {
		handler = r.registryHandler(rw, req)
		return
	}

	proxyUpstream(rw, req, r.log)
	r.log.V(4).Info("proxy request", "url", req.URL.String())
}

func (r *Registry) readyHandler(rw mux.ResponseWriter, req *http.Request) {
	ok, err := r.router.Ready(req.Context())
	if err != nil {
		rw.WriteError(http.StatusInternalServerError, fmt.Errorf("could not determine router readiness: %w", err))
		return
	}
	if !ok {
		rw.WriteHeader(http.StatusInternalServerError)
		return
	}
}

func (r *Registry) registryHandler(rw mux.ResponseWriter, req *http.Request) string {
	// Quickly return 200 for /v2 to indicate that registry supports v2.
	if path.Clean(req.URL.Path) == "/v2" {
		rw.WriteHeader(http.StatusOK)
		return "v2"
	}

	// Parse out path components from request.
	originalRegistry := req.URL.Query().Get("ns")
	ref, err := parsePathComponents(originalRegistry, req.URL.Path)
	if err != nil {
		rw.WriteError(http.StatusNotFound, fmt.Errorf("could not parse path according to OCI distribution spec: %w", err))
		return "registry"
	}

	// Request with mirror header are proxied.
	if req.Header.Get(MirroredHeaderKey) != "true" {
		// Set mirrored header in request to stop infinite loops
		req.Header.Set(MirroredHeaderKey, "true")
		r.handleMirror(rw, req, ref)
		return "mirror"
	}

	// Serve registry endpoints.
	switch ref.kind {
	case referenceKindManifest:
		r.handleManifest(rw, req, ref)
		return "manifest"
	case referenceKindBlob:
		r.handleBlob(rw, req, ref)
		return "blob"
	default:
		rw.WriteError(http.StatusNotFound, fmt.Errorf("unknown reference kind %s", ref.kind))
		return "registry"
	}
}

func (r *Registry) handleMirror(rw mux.ResponseWriter, req *http.Request, ref reference) {
	key := ref.dgst.String()
	if key == "" {
		key = ref.name
	}

	log := r.log.WithValues("key", key, "path", req.URL.Path, "ip", getClientIP(req))

	isExternal := r.isExternalRequest(req)
	if isExternal {
		log.Info("handling mirror request from external node")
	}

	defer func() {
		sourceType := "internal"
		if isExternal {
			sourceType = "external"
		}
		cacheType := "hit"
		if rw.Status() != http.StatusOK {
			cacheType = "miss"
		}
		metrics.MirrorRequestsTotal.WithLabelValues(ref.originalRegistry, cacheType, sourceType).Inc()
	}()

	if !r.resolveLatestTag && ref.hasLatestTag() {
		r.log.V(4).Info("skipping mirror request for image with latest tag", "image", ref.name)
		rw.WriteHeader(http.StatusNotFound)
		return
	}

	// Resolve mirror with the requested key
	resolveCtx, cancel := context.WithTimeout(req.Context(), r.resolveTimeout)
	defer cancel()
	resolveCtx = logr.NewContext(resolveCtx, log)
	peerCh, err := r.router.Resolve(resolveCtx, key, isExternal, r.resolveRetries)
	if err != nil {
		rw.WriteError(http.StatusInternalServerError, fmt.Errorf("error occurred when attempting to resolve mirrors: %w", err))
		return
	}

	mirrorAttempts := 0
	for {
		select {
		case <-req.Context().Done():
			// Request has been closed by server or client. No use continuing.
			rw.WriteError(http.StatusNotFound, fmt.Errorf("mirroring for image component %s has been cancelled: %w", key, resolveCtx.Err()))
			return
		case ipAddr, ok := <-peerCh:
			// Channel closed means no more mirrors will be received and max retries has been reached.
			if !ok {
				err = fmt.Errorf("mirror with image component %s could not be found", key)
				if mirrorAttempts > 0 {
					err = errors.Join(err, fmt.Errorf("requests to %d mirrors failed, all attempts have been exhausted or timeout has been reached", mirrorAttempts))
				}
				rw.WriteError(http.StatusNotFound, err)
				return
			}

			mirrorAttempts++

			// Modify response returns and error on non 200 status code and NOP error handler skips response writing.
			// If proxy fails no response is written and it is tried again against a different mirror.
			// If the response writer has been written to it means that the request was properly proxied.
			succeeded := false
			scheme := "http"
			if req.TLS != nil {
				scheme = "https"
			}
			u := &url.URL{
				Scheme: scheme,
				Host:   ipAddr.String(),
			}
			proxy := httputil.NewSingleHostReverseProxy(u)
			proxy.BufferPool = r.bufferPool
			proxy.Transport = r.transport
			proxy.ErrorHandler = func(_ http.ResponseWriter, _ *http.Request, err error) {
				log.Error(err, "request to mirror failed", "attempt", mirrorAttempts)
			}
			proxy.ModifyResponse = func(resp *http.Response) error {
				if resp.StatusCode != http.StatusOK {
					return fmt.Errorf("expected mirror to respond with 200 OK but received: %s", resp.Status)
				}
				succeeded = true
				return nil
			}
			proxy.ServeHTTP(rw, req)
			if !succeeded {
				break
			}
			log.V(4).Info("mirrored request", "url", u.String())
			return
		}
	}
}

func (r *Registry) handleManifest(rw mux.ResponseWriter, req *http.Request, ref reference) {
	if ref.dgst == "" {
		var err error
		ref.dgst, err = r.ociClient.Resolve(req.Context(), ref.name)
		if err != nil {
			rw.WriteError(http.StatusNotFound, fmt.Errorf("could not get digest for image tag %s: %w", ref.name, err))
			return
		}
	}
	b, mediaType, err := r.ociClient.GetManifest(req.Context(), ref.dgst)
	if err != nil {
		rw.WriteError(http.StatusNotFound, fmt.Errorf("could not get manifest content for digest %s: %w", ref.dgst.String(), err))
		return
	}
	rw.Header().Set("Content-Type", mediaType)
	rw.Header().Set("Content-Length", strconv.FormatInt(int64(len(b)), 10))
	rw.Header().Set("Docker-Content-Digest", ref.dgst.String())
	if req.Method == http.MethodHead {
		return
	}
	_, err = rw.Write(b)
	if err != nil {
		r.log.Error(err, "error occurred when writing manifest")
		return
	}
}

func (r *Registry) handleBlob(rw mux.ResponseWriter, req *http.Request, ref reference) {
	size, err := r.ociClient.Size(req.Context(), ref.dgst)
	if err != nil {
		rw.WriteError(http.StatusInternalServerError, fmt.Errorf("could not determine size of blob with digest %s: %w", ref.dgst.String(), err))
		return
	}
	rw.Header().Set("Content-Type", "application/octet-stream")
	rw.Header().Set("Content-Length", strconv.FormatInt(size, 10))
	rw.Header().Set("Docker-Content-Digest", ref.dgst.String())
	if req.Method == http.MethodHead {
		return
	}
	var w mux.ResponseWriter = rw
	if r.throttler != nil {
		w = r.throttler.Writer(rw)
	}
	rc, err := r.ociClient.GetBlob(req.Context(), ref.dgst)
	if err != nil {
		rw.WriteError(http.StatusInternalServerError, fmt.Errorf("could not get reader for blob with digest %s: %w", ref.dgst.String(), err))
		return
	}
	defer rc.Close()

	http.ServeContent(w, req, ref.dgst.String(), time.Time{}, rc)
}

func (r *Registry) isExternalRequest(req *http.Request) bool {
	return req.Host != r.localAddr
}

func getClientIP(req *http.Request) string {
	forwardedFor := req.Header.Get("X-Forwarded-For")
	if forwardedFor != "" {
		comps := strings.Split(forwardedFor, ",")
		if len(comps) > 1 {
			return comps[0]
		}
		return forwardedFor
	}
	h, _, err := net.SplitHostPort(req.RemoteAddr)
	if err != nil {
		return ""
	}
	return h
}

func proxyUpstream(rw http.ResponseWriter, req *http.Request, log logr.Logger) {
	connBackend, err := net.DialTimeout("tcp", req.Host, 10*time.Second)
	if err != nil {
		http.Error(rw, err.Error(), http.StatusServiceUnavailable)
		return
	}
	rw.WriteHeader(http.StatusOK)
	hijacker, ok := rw.(http.Hijacker)
	if !ok {
		http.Error(rw, "Hijacking not supported", http.StatusInternalServerError)
		return
	}
	conn, _, err := hijacker.Hijack()
	if err != nil {
		http.Error(rw, err.Error(), http.StatusServiceUnavailable)
		return
	}
	go connCopy(connBackend, conn, log)
	go connCopy(conn, connBackend, log)
}

func connCopy(destination io.WriteCloser, source io.ReadCloser, log logr.Logger) {
	defer destination.Close()
	defer source.Close()
	_, err := io.Copy(destination, source)
	if err != nil {
		log.Error(err, "error copying connection")
	}
}
