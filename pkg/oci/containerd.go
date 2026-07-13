package oci

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"text/template"
	"time"

	"github.com/avast/retry-go/v4"
	eventtypes "github.com/containerd/containerd/api/events"
	"github.com/containerd/containerd/v2/client"
	"github.com/containerd/containerd/v2/core/content"
	"github.com/containerd/containerd/v2/core/events"
	"github.com/containerd/containerd/v2/core/images"
	"github.com/containerd/containerd/v2/pkg/labels"
	"github.com/containerd/errdefs"
	"github.com/containerd/typeurl/v2"
	"github.com/go-logr/logr"
	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"github.com/pelletier/go-toml/v2"
	tomlu "github.com/pelletier/go-toml/v2/unstable"

	"github.com/spegel-org/spegel/internal/option"
	"github.com/spegel-org/spegel/pkg/httpx"
)

const (
	backupDir       = "_backup"
	listImageFilter = `name~="^.+/"`
)

type ContainerdConfig struct {
	ContentPath string
}

type ContainerdOption = option.Option[ContainerdConfig]

func WithContentPath(path string) ContainerdOption {
	return func(c *ContainerdConfig) error {
		c.ContentPath = path
		return nil
	}
}

var _ Store = &Containerd{}

type Containerd struct {
	client       *client.Client
	mediaTypeIdx *lru.Cache[digest.Digest, string]
	contentPath  string
}

func NewContainerd(ctx context.Context, sock, namespace string, opts ...ContainerdOption) (*Containerd, error) {
	cfg := ContainerdConfig{}
	err := option.Apply(&cfg, opts...)
	if err != nil {
		return nil, err
	}

	client, err := client.New(sock, client.WithDefaultNamespace(namespace))
	if err != nil {
		return nil, err
	}
	mediaTypeIdx, err := lru.New[digest.Digest, string](100)
	if err != nil {
		return nil, err
	}
	c := &Containerd{
		client:       client,
		mediaTypeIdx: mediaTypeIdx,
		contentPath:  cfg.ContentPath,
	}
	return c, nil
}

func (c *Containerd) Close() error {
	err := c.client.Close()
	if err != nil {
		return err
	}
	return nil
}

func (c *Containerd) Name() string {
	return "containerd"
}

func (c *Containerd) ListImages(ctx context.Context) ([]Image, error) {
	cImgs, err := c.client.ImageService().List(ctx, listImageFilter)
	if err != nil {
		return nil, err
	}
	tagDgsts := map[digest.Digest]string{}
	imgs := []Image{}
	for _, cImg := range cImgs {
		img, err := ParseImage(cImg.Name, WithDigest(cImg.Target.Digest))
		if err != nil {
			return nil, err
		}
		if img.Tag != "" {
			tagDgsts[img.Digest] = img.Tag
		}
		imgs = append(imgs, img)
	}
	// Remove duplicate digest images that already have tags.
	imgs = slices.DeleteFunc(imgs, func(img Image) bool {
		if img.Tag != "" {
			return false
		}
		if _, ok := tagDgsts[img.Digest]; ok {
			return true
		}
		return false
	})
	return imgs, nil
}

func (c *Containerd) ListContent(ctx context.Context) ([][]Reference, error) {
	contents := [][]Reference{}
	// Content without distribution source labels (e.g. images loaded via
	// "ctr image import") cannot be mapped to a reference through labels. Such
	// blobs are collected here and resolved through an image walk below.
	orphans := map[digest.Digest]struct{}{}
	err := c.client.ContentStore().Walk(ctx, func(i content.Info) error {
		refs, err := contentLabelsToReferences(i.Labels, i.Digest)
		if err != nil {
			orphans[i.Digest] = struct{}{}
			return nil
		}
		contents = append(contents, refs)
		return nil
	})
	if err != nil {
		return nil, err
	}
	// Fast path: every blob carries distribution source labels, which is the
	// case for content pulled from a registry. No image walk is needed.
	if len(orphans) == 0 {
		return contents, nil
	}
	// Fallback: resolve label-less content by walking the image manifest trees
	// and matching their digests against the orphan set.
	recovered, err := c.resolveOrphanContent(ctx, orphans)
	if err != nil {
		logr.FromContextOrDiscard(ctx).Error(err, "failed to resolve label-less content via image walk")
		return contents, nil
	}
	contents = append(contents, recovered...)
	return contents, nil
}

// resolveOrphanContent walks every local image and returns references for the
// content digests present in the orphans set. References are deduplicated per
// digest by registry and repository.
func (c *Containerd) resolveOrphanContent(ctx context.Context, orphans map[digest.Digest]struct{}) ([][]Reference, error) {
	log := logr.FromContextOrDiscard(ctx)
	cImgs, err := c.client.ImageService().List(ctx, listImageFilter)
	if err != nil {
		return nil, err
	}
	include := func(dgst digest.Digest) bool {
		_, ok := orphans[dgst]
		return ok
	}
	byDigest := map[digest.Digest]map[string]Reference{}
	for _, cImg := range cImgs {
		img, err := ParseImage(cImg.Name, WithDigest(cImg.Target.Digest))
		if err != nil {
			log.Error(err, "skipping image that cannot be parsed", "image", cImg.Name)
			continue
		}
		refs, err := collectImageReferences(ctx, c.client.ContentStore(), img, cImg.Target, include)
		if err != nil {
			log.Error(err, "skipping image that cannot be walked", "image", img.String())
			continue
		}
		for _, ref := range refs {
			key := ref.Registry + "/" + ref.Repository
			m, ok := byDigest[ref.Digest]
			if !ok {
				m = map[string]Reference{}
				byDigest[ref.Digest] = m
			}
			m[key] = ref
		}
	}
	out := make([][]Reference, 0, len(byDigest))
	for _, m := range byDigest {
		refs := make([]Reference, 0, len(m))
		for _, ref := range m {
			refs = append(refs, ref)
		}
		out = append(out, refs)
	}
	return out, nil
}

// collectImageReferences walks the manifest tree rooted at target and returns a
// reference for every visited descriptor whose digest passes the include
// predicate. A nil include predicate collects every descriptor.
func collectImageReferences(ctx context.Context, provider content.Provider, img Image, target ocispec.Descriptor, include func(digest.Digest) bool) ([]Reference, error) {
	refs := []Reference{}
	handler := images.HandlerFunc(func(ctx context.Context, desc ocispec.Descriptor) ([]ocispec.Descriptor, error) {
		children, err := images.ChildrenHandler(provider).Handle(ctx, desc)
		if errors.Is(err, errdefs.ErrNotFound) {
			return nil, nil
		}
		if err != nil {
			return nil, err
		}
		if include == nil || include(desc.Digest) {
			refs = append(refs, Reference{
				Registry:   img.Registry,
				Repository: img.Repository,
				Digest:     desc.Digest,
			})
		}
		return children, nil
	})
	err := images.Walk(ctx, handler, target)
	if err != nil {
		return nil, err
	}
	return refs, nil
}

func (c *Containerd) Resolve(ctx context.Context, ref string) (digest.Digest, error) {
	cImg, err := c.client.ImageService().Get(ctx, ref)
	if err != nil {
		return "", err
	}
	return cImg.Target.Digest, nil
}

func (c *Containerd) Descriptor(ctx context.Context, dgst digest.Digest) (ocispec.Descriptor, error) {
	info, err := c.client.ContentStore().Info(ctx, dgst)
	if errors.Is(err, errdefs.ErrNotFound) {
		return ocispec.Descriptor{}, errors.Join(ErrNotFound, err)
	}
	if err != nil {
		return ocispec.Descriptor{}, err
	}

	mt, ok := c.mediaTypeIdx.Get(dgst)
	if !ok {
		mt, err = func() (string, error) {
			if info.Size > ManifestMaxSize {
				return httpx.ContentTypeBinary, nil
			}
			rc, err := c.Open(ctx, dgst)
			if err != nil {
				return "", err
			}
			defer rc.Close()
			mt, err := FingerprintMediaType(rc)
			if err != nil {
				return "", err
			}
			return mt, nil
		}()
		if err != nil {
			return ocispec.Descriptor{}, err
		}
		c.mediaTypeIdx.Add(dgst, mt)
	}

	desc := ocispec.Descriptor{
		Size:      info.Size,
		Digest:    dgst,
		MediaType: mt,
	}
	return desc, nil
}

func (c *Containerd) Open(ctx context.Context, dgst digest.Digest) (io.ReadSeekCloser, error) {
	if c.contentPath != "" {
		path := filepath.Join(c.contentPath, "blobs", dgst.Algorithm().String(), dgst.Encoded())
		file, err := os.Open(path)
		if errors.Is(err, os.ErrNotExist) {
			return nil, errors.Join(ErrNotFound, err)
		}
		if err != nil {
			return nil, err
		}
		return file, nil
	}
	ra, err := c.client.ContentStore().ReaderAt(ctx, ocispec.Descriptor{Digest: dgst})
	if errors.Is(err, errdefs.ErrNotFound) {
		return nil, errors.Join(ErrNotFound, err)
	}
	if err != nil {
		return nil, err
	}
	return struct {
		io.ReadSeeker
		io.Closer
	}{
		ReadSeeker: io.NewSectionReader(ra, 0, ra.Size()),
		Closer:     ra,
	}, nil
}

func (c *Containerd) Subscribe(ctx context.Context) (<-chan OCIEvent, error) {
	log := logr.FromContextOrDiscard(ctx)

	eventCh := make(chan OCIEvent)
	subCtx, subCancel := context.WithCancel(ctx)
	eventFilters := []string{`topic~="/images/create|/images/delete",event.name~="^.+/"`, `topic~="/content/create"`}
	envelopeCh, cErrCh := c.client.EventService().Subscribe(subCtx, eventFilters...)

	// Populate the content index.
	contentIdx := map[digest.Digest][]Reference{}
	cImgs, err := c.client.ImageService().List(ctx, listImageFilter)
	if err != nil {
		subCancel()
		return nil, err
	}
	for _, cImg := range cImgs {
		img, err := ParseImage(cImg.Name, WithDigest(cImg.Target.Digest))
		if err != nil {
			log.Error(err, "skipping image that cannot be parsed", "image", img.String())
			continue
		}
		refs, err := collectImageReferences(ctx, c.client.ContentStore(), img, cImg.Target, nil)
		if err != nil {
			log.Error(err, "skipping image that cannot be walked", "image", img.String())
			continue
		}
		contentIdx[cImg.Target.Digest] = refs
	}

	go func() {
		defer close(eventCh)
		for {
			select {
			case <-subCtx.Done():
				return
			case envelope := <-envelopeCh:
				events, err := c.handleEvent(subCtx, *envelope, contentIdx)
				if err != nil {
					log.Error(err, "error when handling containerd event")
					continue
				}
				for _, event := range events {
					eventCh <- event
				}
			}
		}
	}()
	go func() {
		// Required so that the event channel closes in case containerd is restarted.
		defer subCancel()
		for err := range cErrCh {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Error(err, "received containerd event error")
		}
	}()
	return eventCh, nil
}

func (c *Containerd) handleEvent(ctx context.Context, envelope events.Envelope, contentIdx map[digest.Digest][]Reference) ([]OCIEvent, error) {
	if envelope.Event == nil {
		return nil, errors.New("envelope event cannot be nil")
	}
	evt, err := typeurl.UnmarshalAny(envelope.Event)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal envelope event: %w", err)
	}
	switch e := evt.(type) {
	case *eventtypes.ContentCreate:
		dgst := digest.Digest(e.GetDigest())
		retryOpts := []retry.Option{
			retry.Context(ctx),
			retry.Attempts(10),
			retry.MaxDelay(100 * time.Millisecond),
		}
		refs, err := retry.DoWithData(func() ([]Reference, error) {
			info, err := c.client.ContentStore().Info(ctx, dgst)
			if err != nil {
				return nil, retry.Unrecoverable(err)
			}
			refs, err := contentLabelsToReferences(info.Labels, dgst)
			if err != nil {
				return nil, err
			}
			return refs, nil
		}, retryOpts...)
		if err != nil {
			return nil, err
		}
		events := []OCIEvent{}
		for _, ref := range refs {
			events = append(events, OCIEvent{Type: CreateEvent, Reference: ref})
		}
		return events, nil
	case *eventtypes.ImageCreate:
		img, err := ParseImage(e.GetName(), AllowTagOnly())
		if err != nil {
			return nil, err
		}
		// Tag reference without an explicit digest. Advertise the tag itself and,
		// for images that lack distribution source labels (e.g. loaded through
		// "ctr image import"), advertise their content tree directly since the
		// ContentCreate path cannot map them through labels.
		if img.Digest == "" {
			events := []OCIEvent{{Type: CreateEvent, Reference: img.Reference}}
			contentEvents, err := c.advertiseLabellessContent(ctx, e.GetName())
			if err != nil {
				logr.FromContextOrDiscard(ctx).Error(err, "failed to advertise label-less content", "image", img.String())
				return events, nil
			}
			return append(events, contentEvents...), nil
		}
		// Walk the image to index its content.
		cImg, err := c.client.ImageService().Get(ctx, img.String())
		if err != nil {
			return nil, err
		}
		refs, err := collectImageReferences(ctx, c.client.ContentStore(), img, cImg.Target, nil)
		if err != nil {
			return nil, err
		}
		contentIdx[img.Digest] = refs
		return nil, nil
	case *eventtypes.ImageDelete:
		img, err := ParseImage(e.GetName(), AllowTagOnly())
		if err != nil {
			return nil, err
		}
		// Just advertise the image if it is a tag reference.
		if img.Digest == "" {
			return []OCIEvent{{Type: DeleteEvent, Reference: img.Reference}}, nil
		}
		// Advertise deletion of images content if it no longer exists.
		refs, ok := contentIdx[img.Digest]
		if !ok {
			logr.FromContextOrDiscard(ctx).Info("delete event with missing content index entry")
			return []OCIEvent{{Type: DeleteEvent, Reference: img.Reference}}, nil
		}
		delete(contentIdx, img.Digest)
		// Delete events are sent before garbage collection is run.
		retryOpts := []retry.Option{
			retry.Context(ctx),
			retry.Attempts(10),
			retry.MaxDelay(100 * time.Millisecond),
		}
		err = retry.Do(func() error {
			_, err := c.client.ContentStore().Info(ctx, img.Digest)
			if errors.Is(err, errdefs.ErrNotFound) {
				return nil
			}
			if err != nil {
				return retry.Unrecoverable(err)
			}
			return fmt.Errorf("manifest with digest %s still exists", img.Digest.String())
		}, retryOpts...)
		if err != nil {
			return nil, fmt.Errorf("image manifest has not been deleted: %w", err)
		}
		// Create delete events for contents that has been removed.
		events := []OCIEvent{}
		for _, ref := range refs {
			_, err := c.client.ContentStore().Info(ctx, ref.Digest)
			if err == nil {
				continue
			}
			if !errors.Is(err, errdefs.ErrNotFound) {
				return nil, err
			}
			events = append(events, OCIEvent{Type: DeleteEvent, Reference: ref})
		}
		return events, nil
	default:
		return nil, errors.New("unsupported event type")
	}
}

// advertiseLabellessContent walks the manifest tree of the named image and
// returns create events for its content. It is a fallback for images that lack
// the containerd.io/distribution.source.* labels, such as those loaded via
// "ctr image import". Images pulled from a registry carry these labels and are
// skipped here, leaving the ContentCreate path authoritative for them.
func (c *Containerd) advertiseLabellessContent(ctx context.Context, name string) ([]OCIEvent, error) {
	img, err := ParseImage(name, AllowTagOnly())
	if err != nil {
		return nil, err
	}
	cImg, err := c.client.ImageService().Get(ctx, name)
	if err != nil {
		return nil, err
	}
	// Use the manifest as a sentinel: pulled content has distribution source
	// labels while imported content does not. A single Info lookup gates the
	// whole walk so the normal pull path keeps its original behavior.
	info, err := c.client.ContentStore().Info(ctx, cImg.Target.Digest)
	if err != nil {
		return nil, err
	}
	if _, err := contentLabelsToReferences(info.Labels, cImg.Target.Digest); err == nil {
		return nil, nil
	}
	refs, err := collectImageReferences(ctx, c.client.ContentStore(), img, cImg.Target, nil)
	if err != nil {
		return nil, err
	}
	events := make([]OCIEvent, 0, len(refs))
	for _, ref := range refs {
		events = append(events, OCIEvent{Type: CreateEvent, Reference: ref})
	}
	return events, nil
}

func contentLabelsToReferences(l map[string]string, dgst digest.Digest) ([]Reference, error) {
	refs := []Reference{}
	for k, v := range l {
		if !strings.HasPrefix(k, labels.LabelDistributionSource) {
			continue
		}
		ref := Reference{
			Registry:   strings.TrimPrefix(k, labels.LabelDistributionSource+"."),
			Repository: v,
			Digest:     dgst,
		}
		refs = append(refs, ref)
	}
	if len(refs) == 0 {
		return nil, fmt.Errorf("no distribution source labels found for %s", dgst)
	}
	return refs, nil
}

// Refer to containerd registry configuration documentation for more information about required configuration.
// https://github.com/containerd/containerd/blob/main/docs/cri/config.md#registry-configuration
// https://github.com/containerd/containerd/blob/main/docs/hosts.md#registry-configuration---examples
func AddMirrorConfiguration(ctx context.Context, configPath string, mirroredRegistries, mirrorTargets []string, resolveTags, prependExisting bool, mirrorDialTimeout time.Duration, username, password string) error {
	log := logr.FromContextOrDiscard(ctx)
	if mirrorDialTimeout < 0 {
		return errors.New("mirror dial timeout must be greater than or equal to 0")
	}

	// Parse and verify mirror urls.
	parsedMirroredRegistries, err := parseRegistries(mirroredRegistries, true)
	if err != nil {
		return err
	}
	parsedMirrorTargets, err := parseRegistries(mirrorTargets, false)
	if err != nil {
		return err
	}

	// Backup and clear configgurrationn.
	err = os.MkdirAll(configPath, 0o755)
	if err != nil {
		return err
	}
	err = backupConfig(log, configPath)
	if err != nil {
		return err
	}
	err = clearConfig(configPath)
	if err != nil {
		return err
	}

	// Write mirror configuration
	capabilities := []string{"pull"}
	if resolveTags {
		capabilities = append(capabilities, "resolve")
	}
	for _, mr := range parsedMirroredRegistries {
		templatedHosts, err := templateHosts(mr, parsedMirrorTargets, capabilities, mirrorDialTimeout, username, password)
		if err != nil {
			return err
		}
		if prependExisting {
			existingHostConfig, err := existingHosts(configPath, mr)
			if err != nil {
				return err
			}
			templatedHosts, err = mergeHostSections(templatedHosts, existingHostConfig)
			if err != nil {
				return err
			}
			if existingHostConfig != "" {
				// If we are prepending we also want to keep files like certificates that may be referenced.
				backupRegDir := path.Join(configPath, backupDir, mr.Host)
				err = filepath.WalkDir(backupRegDir, func(path string, d fs.DirEntry, err error) error {
					if err != nil {
						return err
					}
					if d.IsDir() {
						return nil
					}
					if d.Name() == "hosts.toml" {
						return nil
					}
					src, err := os.Open(path)
					if err != nil {
						return err
					}
					defer src.Close()
					relPath, err := filepath.Rel(backupRegDir, path)
					if err != nil {
						return err
					}
					dstPath := filepath.Join(configPath, mr.Host, relPath)
					err = os.MkdirAll(filepath.Dir(dstPath), 0o755)
					if err != nil {
						return err
					}
					dst, err := os.OpenFile(dstPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
					if err != nil {
						return err
					}
					defer dst.Close()
					_, err = io.Copy(dst, src)
					if err != nil {
						return err
					}
					return nil
				})
				if err != nil {
					return err
				}

				log.Info("prepending to existing containerd mirror configuration", "registry", mr.String())
			}
		}
		fp := path.Join(configPath, mr.Host, "hosts.toml")
		err = os.MkdirAll(filepath.Dir(fp), 0o755)
		if err != nil {
			return err
		}
		err = os.WriteFile(fp, []byte(templatedHosts), 0o644)
		if err != nil {
			return err
		}
		log.Info("added containerd mirror configuration", "registry", mr.String(), "path", fp)
	}
	return nil
}

func CleanupMirrorConfiguration(ctx context.Context, configPath string) error {
	log := logr.FromContextOrDiscard(ctx)

	// If backup directory does not exist it means mirrors was never configured or cleanup has already run.
	backupDirPath := path.Join(configPath, backupDir)
	ok, err := dirExists(backupDirPath)
	if err != nil {
		return err
	}
	if !ok {
		log.Info("skipping cleanup because backup directory does not exist")
		return nil
	}

	// Remove everything except _backup
	err = clearConfig(configPath)
	if err != nil {
		return err
	}

	// Move content from backup directory
	files, err := os.ReadDir(backupDirPath)
	if err != nil {
		return err
	}
	for _, fi := range files {
		oldPath := path.Join(backupDirPath, fi.Name())
		newPath := path.Join(configPath, fi.Name())
		err := os.Rename(oldPath, newPath)
		if err != nil {
			return err
		}
		log.Info("recovering containerd host configuration", "path", oldPath)
	}

	// Remove backup directory to indicate that cleanup has been run.
	err = os.RemoveAll(backupDirPath)
	if err != nil {
		return err
	}

	return nil
}

func backupConfig(log logr.Logger, configPath string) error {
	backupDirPath := path.Join(configPath, backupDir)
	ok, err := dirExists(backupDirPath)
	if err != nil {
		return err
	}
	if ok {
		return nil
	}
	files, err := os.ReadDir(configPath)
	if err != nil {
		return err
	}
	err = os.MkdirAll(backupDirPath, 0o755)
	if err != nil {
		return err
	}
	for _, fi := range files {
		oldPath := path.Join(configPath, fi.Name())
		newPath := path.Join(backupDirPath, fi.Name())
		err := os.Rename(oldPath, newPath)
		if err != nil {
			return err
		}
		log.Info("backing up containerd host configuration", "path", oldPath)
	}
	return nil
}

func clearConfig(configPath string) error {
	files, err := os.ReadDir(configPath)
	if err != nil {
		return err
	}
	for _, fi := range files {
		if fi.Name() == backupDir {
			continue
		}
		filePath := path.Join(configPath, fi.Name())
		err := os.RemoveAll(filePath)
		if err != nil {
			return err
		}
	}
	return nil
}

func templateHosts(parsedMirrorRegistry url.URL, parsedMirrorTargets []url.URL, capabilities []string, mirrorDialTimeout time.Duration, username, password string) (string, error) {
	server := parsedMirrorRegistry.String()
	if parsedMirrorRegistry.String() == "https://docker.io" {
		server = "https://registry-1.docker.io"
	}
	if parsedMirrorRegistry == wildcardRegistryURL {
		server = ""
	}

	authorization := ""
	if username != "" || password != "" {
		authorization = username + ":" + password
		authorization = base64.StdEncoding.EncodeToString([]byte(authorization))
		authorization = "Basic " + authorization
	}

	dialTimeout := ""
	if mirrorDialTimeout > 0 {
		dialTimeout = mirrorDialTimeout.String()
	}

	hc := struct {
		Authorization string
		Server        string
		Capabilities  string
		DialTimeout   string
		MirrorTargets []url.URL
	}{
		Server:        server,
		Capabilities:  fmt.Sprintf("['%s']", strings.Join(capabilities, "', '")),
		DialTimeout:   dialTimeout,
		MirrorTargets: parsedMirrorTargets,
		Authorization: authorization,
	}
	tmpl, err := template.New("").Parse(`{{- with .Server }}server = '{{ . }}'{{ end }}
{{- $authorization := .Authorization }}
{{ range .MirrorTargets }}
[host.'{{ .String }}']
capabilities = {{ $.Capabilities }}
{{- if $.DialTimeout }}
dial_timeout = '{{ $.DialTimeout }}'
{{- end }}
{{- if $authorization }}
[host.'{{ .String }}'.header]
Authorization = '{{ $authorization }}'
{{- end }}
{{ end }}`)
	if err != nil {
		return "", err
	}
	buf := bytes.NewBuffer(nil)
	err = tmpl.Execute(buf, hc)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(buf.String()), nil
}

func existingHosts(configPath string, parsedMirrorRegistry url.URL) (string, error) {
	fp := path.Join(configPath, backupDir, parsedMirrorRegistry.Host, "hosts.toml")
	b, err := os.ReadFile(fp)
	if errors.Is(err, os.ErrNotExist) {
		return "", nil
	}
	if err != nil {
		return "", err
	}

	type hostFile struct {
		Hosts map[string]any `toml:"host"`
	}

	var hf hostFile
	err = toml.Unmarshal(b, &hf)
	if err != nil {
		return "", err
	}
	if len(hf.Hosts) == 0 {
		return "", nil
	}

	hosts := []string{}
	seen := map[string]struct{}{}
	parser := tomlu.Parser{}
	parser.Reset(b)
	for parser.NextExpression() {
		err := parser.Error()
		if err != nil {
			return "", err
		}
		e := parser.Expression()
		if e.Kind != tomlu.Table {
			continue
		}
		ki := e.Key()
		if ki.Next() && string(ki.Node().Data) == "host" && ki.Next() && ki.IsLast() {
			host := string(ki.Node().Data)
			if _, ok := seen[host]; ok {
				continue
			}
			seen[host] = struct{}{}
			hosts = append(hosts, host)
		}
	}

	ehs := []string{}
	for _, h := range hosts {
		host, ok := hf.Hosts[h]
		if !ok {
			continue
		}
		data := hostFile{
			Hosts: map[string]any{
				h: host,
			},
		}
		b, err := toml.Marshal(data)
		if err != nil {
			return "", err
		}
		eh := strings.TrimPrefix(string(b), "[host]\n")
		ehs = append(ehs, eh)
	}
	return strings.TrimSpace(strings.Join(ehs, "\n")), nil
}

func mergeHostSections(generated, existing string) (string, error) {
	genPrefix, genSections, err := splitHostSections(generated)
	if err != nil {
		return "", err
	}
	existingPrefix, existingSections, err := splitHostSections(existing)
	if err != nil {
		return "", err
	}

	sectionsByHost := map[string]map[string]any{}
	generatedOrder := []string{}
	existingOrder := []string{}
	generatedSeen := map[string]struct{}{}
	existingSeen := map[string]struct{}{}

	for _, section := range existingSections {
		host, value, err := parseHostSection(section)
		if err != nil {
			return "", err
		}
		if host == "" {
			continue
		}
		if _, ok := existingSeen[host]; !ok {
			existingSeen[host] = struct{}{}
			existingOrder = append(existingOrder, host)
		}
		if current, ok := sectionsByHost[host]; ok {
			sectionsByHost[host] = mergeTomlMaps(current, value)
		} else {
			sectionsByHost[host] = value
		}
	}

	for _, section := range genSections {
		host, value, err := parseHostSection(section)
		if err != nil {
			return "", err
		}
		if host == "" {
			continue
		}
		if _, ok := generatedSeen[host]; !ok {
			generatedSeen[host] = struct{}{}
			generatedOrder = append(generatedOrder, host)
		}
		if current, ok := sectionsByHost[host]; ok {
			sectionsByHost[host] = mergeTomlMaps(current, value)
		} else {
			sectionsByHost[host] = value
		}
	}

	sections := []string{}
	if genPrefix != "" {
		sections = append(sections, genPrefix)
	} else if existingPrefix != "" {
		sections = append(sections, existingPrefix)
	}
	for _, host := range generatedOrder {
		rendered, err := marshalHostSection(host, sectionsByHost[host])
		if err != nil {
			return "", err
		}
		sections = append(sections, rendered)
	}
	for _, host := range existingOrder {
		if _, ok := generatedSeen[host]; ok {
			continue
		}
		rendered, err := marshalHostSection(host, sectionsByHost[host])
		if err != nil {
			return "", err
		}
		sections = append(sections, rendered)
	}
	return strings.TrimSpace(strings.Join(sections, "\n\n")), nil
}

func parseHostSection(section string) (string, map[string]any, error) {
	var doc map[string]any
	if err := toml.Unmarshal([]byte(section), &doc); err != nil {
		return "", nil, err
	}
	hostTable, ok := doc["host"].(map[string]any)
	if !ok || len(hostTable) == 0 {
		return "", nil, nil
	}
	for host, value := range hostTable {
		hostValue, ok := value.(map[string]any)
		if !ok {
			return "", nil, fmt.Errorf("host section %q has unexpected value type %T", host, value)
		}
		return host, hostValue, nil
	}
	return "", nil, nil
}

func mergeTomlMaps(dst, src map[string]any) map[string]any {
	if dst == nil && src == nil {
		return nil
	}
	if dst == nil {
		dst = map[string]any{}
	}
	for k, srcVal := range src {
		if dstVal, ok := dst[k]; ok {
			dstMap, dstOK := dstVal.(map[string]any)
			srcMap, srcOK := srcVal.(map[string]any)
			if dstOK && srcOK {
				dst[k] = mergeTomlMaps(dstMap, srcMap)
				continue
			}
		}
		dst[k] = srcVal
	}
	return dst
}

func marshalHostSection(host string, value any) (string, error) {
	data := struct {
		Hosts map[string]any `toml:"host"`
	}{
		Hosts: map[string]any{host: value},
	}
	b, err := toml.Marshal(data)
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(strings.TrimPrefix(string(b), "[host]\n")), nil
}

func splitHostSections(s string) (string, []string, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", nil, nil
	}
	lines := strings.Split(s, "\n")
	prefix := []string{}
	sections := []string{}
	current := []string{}
	inHostSection := false
	for _, line := range lines {
		if isTopLevelHostTableLine(line) {
			if len(current) > 0 {
				section := strings.TrimSpace(strings.Join(current, "\n"))
				if inHostSection {
					sections = append(sections, section)
				} else {
					prefix = append(prefix, section)
				}
				current = nil
			}
			inHostSection = true
		}
		current = append(current, line)
	}
	if len(current) > 0 {
		section := strings.TrimSpace(strings.Join(current, "\n"))
		if inHostSection {
			sections = append(sections, section)
		} else {
			prefix = append(prefix, section)
		}
	}
	return strings.Join(prefix, "\n\n"), sections, nil
}

func isTopLevelHostTableLine(line string) bool {
	trimmed := strings.TrimSpace(line)
	if !strings.HasPrefix(trimmed, "[host.") || !strings.HasSuffix(trimmed, "]") {
		return false
	}
	body := strings.TrimSuffix(strings.TrimPrefix(trimmed, "[host."), "]")
	if len(body) < 2 {
		return false
	}
	return (body[0] == '\'' && body[len(body)-1] == '\'') || (body[0] == '"' && body[len(body)-1] == '"')
}

func dirExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if err != nil {
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, err
	}
	return info.IsDir(), nil
}
