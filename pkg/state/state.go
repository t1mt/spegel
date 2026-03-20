package state

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"

	"github.com/spegel-org/spegel/internal/option"
	"github.com/spegel-org/spegel/pkg/metrics"
	"github.com/spegel-org/spegel/pkg/oci"
	"github.com/spegel-org/spegel/pkg/routing"
)

type TrackerConfig struct {
	Filters     []oci.Filter
	ReadvertiseInterval time.Duration
}

type TrackerOption = option.Option[TrackerConfig]

func WithRegistryFilters(filters []oci.Filter) TrackerOption {
	return func(cfg *TrackerConfig) error {
		cfg.Filters = filters
		return nil
	}
}

func WithReadvertiseInterval(d time.Duration) TrackerOption {
	return func(cfg *TrackerConfig) error {
		cfg.ReadvertiseInterval = d
		return nil
	}
}

func Track(ctx context.Context, ociStore oci.Store, router routing.Router, opts ...TrackerOption) error {
	cfg := TrackerConfig{}
	err := option.Apply(&cfg, opts...)
	if err != nil {
		return err
	}

	// Start subscribing to not miss events.
	eventCh, err := ociStore.Subscribe(ctx)
	if err != nil {
		return err
	}

	// Initial advertisement of all content.
	keys, err := collectKeys(ctx, ociStore, cfg.Filters)
	if err != nil {
		return err
	}
	err = router.Advertise(ctx, keys)
	if err != nil {
		return err
	}

	var ticker *time.Ticker
	var tickerC <-chan time.Time
	if cfg.ReadvertiseInterval > 0 {
		ticker = time.NewTicker(cfg.ReadvertiseInterval)
		defer ticker.Stop()
		tickerC = ticker.C
	}

	// Watch for OCI events.
	log := logr.FromContextOrDiscard(ctx)
	log.Info("waiting for store events")
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tickerC:
			keys, err := collectKeys(ctx, ociStore, cfg.Filters)
			if err != nil {
				log.Error(err, "failed to collect keys for re-advertisement")
				continue
			}
			if err := router.Advertise(ctx, keys); err != nil {
				log.Error(err, "failed to re-advertise keys")
				continue
			}
			log.V(1).Info("re-advertised keys", "count", len(keys))
		case event, ok := <-eventCh:
			if !ok {
				return errors.New("event channel closed")
			}
			err := handleEvent(ctx, router, event, cfg.Filters)
			if err != nil {
				logr.FromContextOrDiscard(ctx).Error(err, "could not handle event")
				continue
			}
		}
	}
}

func handleEvent(ctx context.Context, router routing.Router, event oci.OCIEvent, filters []oci.Filter) error {
	if oci.MatchesFilter(event.Reference, filters) {
		return nil
	}
	logr.FromContextOrDiscard(ctx).Info("OCI event", "ref", event.Reference.String(), "type", event.Type)
	switch event.Type {
	case oci.CreateEvent:
		if event.Reference.Tag != "" {
			metrics.AdvertisedImageTags.WithLabelValues(event.Reference.Registry).Inc()
		} else {
			metrics.AdvertisedContentDigests.WithLabelValues(event.Reference.Registry).Inc()
		}
		err := router.Advertise(ctx, []string{event.Reference.Identifier()})
		if err != nil {
			return err
		}
		return nil
	case oci.DeleteEvent:
		if event.Reference.Tag != "" {
			metrics.AdvertisedImageTags.WithLabelValues(event.Reference.Registry).Dec()
		} else {
			metrics.AdvertisedContentDigests.WithLabelValues(event.Reference.Registry).Dec()
		}
		err := router.Withdraw(ctx, []string{event.Reference.Identifier()})
		if err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("unhandled event type %s", event.Type)
	}
}

func allReferencesMatchFilter(refs []oci.Reference, filters []oci.Filter) bool {
	for _, ref := range refs {
		if !oci.MatchesFilter(ref, filters) {
			return false
		}
	}
	return true
}

func collectKeys(ctx context.Context, ociStore oci.Store, filters []oci.Filter) ([]string, error) {
	keys := []string{}
	imgs, err := ociStore.ListImages(ctx)
	if err != nil {
		return nil, err
	}
	for _, img := range imgs {
		if oci.MatchesFilter(img.Reference, filters) {
			continue
		}
		tagName, ok := img.TagName()
		if ok {
			keys = append(keys, tagName)
			metrics.AdvertisedImageTags.WithLabelValues(img.Registry).Inc()
		}
		metrics.AdvertisedImageDigests.WithLabelValues(img.Registry).Inc()
	}
	contents, err := ociStore.ListContent(ctx)
	if err != nil {
		return nil, err
	}
	for _, refs := range contents {
		// TODO(phillebaba): Apply filtering on parent image tag.
		if allReferencesMatchFilter(refs, filters) {
			continue
		}
		for _, ref := range refs {
			metrics.AdvertisedContentDigests.WithLabelValues(ref.Registry).Inc()
		}
		keys = append(keys, refs[0].Digest.String())
	}
	return keys, nil
}
