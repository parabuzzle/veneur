package veneur

import (
	"context"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/Sirupsen/logrus"
	"github.com/stripe/veneur/samplers"
	"github.com/stripe/veneur/trace"
)

type metricSink interface {
	Name() string
	Flush(context.Context, []samplers.InterMetric) error
	// This one is temporary?
	FlushEventsChecks(ctx context.Context, events []samplers.UDPEvent, checks []samplers.UDPServiceCheck)
}

type datadogMetricSink struct {
	HTTPClient      *http.Client
	ddHostname      string
	hostname        string
	apiKey          string
	flushMaxPerBody int
	statsd          *statsd.Client
	tags            []string
	interval        float64
}

// NewDatadogMetricSink creates a new Datadog sink for trace spans.
func NewDatadogMetricSink(config *Config, interval float64, httpClient *http.Client, stats *statsd.Client) (*datadogMetricSink, error) {
	return &datadogMetricSink{
		HTTPClient:      httpClient,
		statsd:          stats,
		interval:        interval,
		flushMaxPerBody: config.FlushMaxPerBody,
		ddHostname:      config.DatadogAPIHostname,
		apiKey:          config.DatadogAPIKey,
	}, nil
}

// Name returns the name of this sink.
func (dd *datadogMetricSink) Name() string {
	return "datadog"
}

func (dd *datadogMetricSink) Flush(ctx context.Context, interMetrics []samplers.InterMetric) error {
	span, _ := trace.StartSpanFromContext(ctx, "")
	defer span.Finish()

	metrics := dd.finalizeMetrics(interMetrics)

	// break the metrics into chunks of approximately equal size, such that
	// each chunk is less than the limit
	// we compute the chunks using rounding-up integer division
	workers := ((len(metrics) - 1) / dd.flushMaxPerBody) + 1
	chunkSize := ((len(metrics) - 1) / workers) + 1
	log.WithField("workers", workers).Debug("Worker count chosen")
	log.WithField("chunkSize", chunkSize).Debug("Chunk size chosen")
	var wg sync.WaitGroup
	flushStart := time.Now()
	for i := 0; i < workers; i++ {
		chunk := metrics[i*chunkSize:]
		if i < workers-1 {
			// trim to chunk size unless this is the last one
			chunk = chunk[:chunkSize]
		}
		wg.Add(1)
		go dd.flushPart(span.Attach(ctx), chunk, &wg)
	}
	wg.Wait()
	dd.statsd.TimeInMilliseconds("flush.total_duration_ns", float64(time.Since(flushStart).Nanoseconds()), []string{"part:post"}, 1.0)

	log.WithField("metrics", len(metrics)).Info("Completed flush to Datadog")
	return nil
}

func (dd *datadogMetricSink) FlushEventsChecks(ctx context.Context, events []samplers.UDPEvent, checks []samplers.UDPServiceCheck) {
	span, _ := trace.StartSpanFromContext(ctx, "")
	defer span.Finish()

	// fill in the default hostname for packets that didn't set it
	for i := range events {
		if events[i].Hostname == "" {
			events[i].Hostname = dd.hostname
		}
		events[i].Tags = append(events[i].Tags, dd.tags...)
	}
	for i := range checks {
		if checks[i].Hostname == "" {
			checks[i].Hostname = dd.hostname
		}
		checks[i].Tags = append(checks[i].Tags, dd.tags...)
	}

	if len(events) != 0 {
		// this endpoint is not documented at all, its existence is only known from
		// the official dd-agent
		// we don't actually pass all the body keys that dd-agent passes here... but
		// it still works
		err := postHelper(context.TODO(), dd.HTTPClient, dd.statsd, fmt.Sprintf("%s/intake?api_key=%s"), map[string]map[string][]samplers.UDPEvent{
			"events": {
				"api": events,
			},
		}, "flush_events", true)
		if err == nil {
			log.WithField("events", len(events)).Info("Completed flushing events to Datadog")
		} else {
			log.WithFields(logrus.Fields{
				"events":        len(events),
				logrus.ErrorKey: err}).Warn("Error flushing events to Datadog")
		}
	}

	if len(checks) != 0 {
		// this endpoint is not documented to take an array... but it does
		// another curious constraint of this endpoint is that it does not
		// support "Content-Encoding: deflate"
		err := postHelper(context.TODO(), dd.HTTPClient, dd.statsd, fmt.Sprintf("%s/api/v1/check_run?api_key=%s"), checks, "flush_checks", false)
		if err == nil {
			log.WithField("checks", len(checks)).Info("Completed flushing service checks to Datadog")
		} else {
			log.WithFields(logrus.Fields{
				"checks":        len(checks),
				logrus.ErrorKey: err}).Warn("Error flushing checks to Datadog")
		}
	}
}

func (dd *datadogMetricSink) finalizeMetrics(metrics []samplers.InterMetric) []samplers.DDMetric {
	ddMetrics := make([]samplers.DDMetric, len(metrics))
	for i, m := range metrics {
		// Defensively copy tags since we're gonna mutate it
		tags := make([]string, len(dd.tags))
		copy(tags, dd.tags)
		metricType := m.Type.String()
		value := m.Value
		// We convert Datadog counters into rates
		if metricType == "counter" {
			metricType = "rate"
			value = m.Value / dd.interval
		}
		ddMetric := samplers.DDMetric{
			Name: m.Name,
			Value: [1][2]float64{
				[2]float64{
					float64(m.Timestamp), value,
				},
			},
			Tags:       tags,
			MetricType: metricType,
			Interval:   int32(dd.interval),
		}

		// Let's look for "magic tags" that override metric fields host and device.
		for _, tag := range m.Tags {
			// This overrides hostname
			if strings.HasPrefix(tag, "host:") {
				// Override the hostname with the tag, trimming off the prefix.
				ddMetric.Hostname = tag[5:]
			} else if strings.HasPrefix(tag, "device:") {
				// Same as above, but device this time
				ddMetric.DeviceName = tag[7:]
			} else {
				// Add it, no reason to exclude it.
				ddMetric.Tags = append(ddMetric.Tags, tag)
			}
		}
		if ddMetric.Hostname == "" {
			// No magic tag, set the hostname
			ddMetric.Hostname = dd.hostname
		}
		ddMetrics[i] = ddMetric
	}

	return ddMetrics
}

func (dd *datadogMetricSink) flushPart(ctx context.Context, metricSlice []samplers.DDMetric, wg *sync.WaitGroup) {
	defer wg.Done()
	postHelper(ctx, dd.HTTPClient, dd.statsd, fmt.Sprintf("%s/api/v1/series?api_key=%s", dd.ddHostname, dd.apiKey), map[string][]samplers.DDMetric{
		"series": metricSlice,
	}, "flush", true)
}
