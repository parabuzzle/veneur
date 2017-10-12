package main

import (
	"flag"
	"fmt"
	"io"
	"math"
	"net/http"
	"time"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/sirupsen/logrus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

var (
	debug       = flag.Bool("d", false, "Enable debug mode")
	metricsHost = flag.String("h", "http://localhost:9090/metrics", "The full URL — like 'http://localhost:9090/metrics' to query for Prometheus metrics.")
	interval    = flag.String("i", "10s", "The interval at which to query. Value must be parseable by time.ParseDuration (https://golang.org/pkg/time/#ParseDuration).")
	prefix      = flag.String("p", "", "A prefix to append to any metrics emitted. Do not include a trailing period.")
	statsHost   = flag.String("s", "127.0.0.1:8126", "The host and port — like '127.0.0.1:8126' — to send our metrics to.")
)

func main() {
	flag.Parse()

	c, _ := statsd.New(*statsHost)

	if *debug {
		logrus.SetLevel(logrus.DebugLevel)
	}

	i, err := time.ParseDuration(*interval)
	if err != nil {
		logrus.WithError(err).Fatalf("Failed to parse interval '%s'", *interval)
	}

	ticker := time.NewTicker(i)
	for _ = range ticker.C {
		collect(c)
	}

	if *prefix != "" {
		c.Namespace = *prefix
	}
}

func collect(c *statsd.Client) {
	logrus.WithFields(logrus.Fields{
		"stats_host":   *statsHost,
		"metrics_host": *metricsHost,
	}).Debug("Beginning collection")

	resp, _ := http.Get(*metricsHost)
	d := expfmt.NewDecoder(resp.Body, expfmt.FmtText)
	var mf dto.MetricFamily
	for {
		err := d.Decode(&mf)
		if err == io.EOF {
			// We've hit the end, break out!
			break
		} else if err != nil {
			c.Count("veneur.prometheus.decode_errors_total", 1, nil, 1.0)
			logrus.WithError(err).Warn("Failed to decode a metric")
			break
		}

		var metricCount int64
		switch mf.GetType() {
		case dto.MetricType_COUNTER:
			for _, counter := range mf.GetMetric() {
				var tags []string
				labels := counter.GetLabel()
				for _, pair := range labels {
					tags = append(tags, fmt.Sprintf("%s:%s", pair.GetName(), pair.GetValue()))
				}
				c.Count(mf.GetName(), int64(counter.GetCounter().GetValue()), tags, 1.0)
				metricCount++
			}
		case dto.MetricType_GAUGE:
			for _, gauge := range mf.GetMetric() {
				var tags []string
				labels := gauge.GetLabel()
				for _, pair := range labels {
					tags = append(tags, fmt.Sprintf("%s:%s", pair.GetName(), pair.GetValue()))
				}
				c.Gauge(mf.GetName(), float64(gauge.GetGauge().GetValue()), tags, 1.0)
				metricCount++
			}
		case dto.MetricType_HISTOGRAM, dto.MetricType_SUMMARY:
			for _, histo := range mf.GetMetric() {
				var tags []string
				labels := histo.GetLabel()
				for _, pair := range labels {
					tags = append(tags, fmt.Sprintf("%s:%s", pair.GetName(), pair.GetValue()))
				}
				hname := mf.GetName()
				summ := histo.GetSummary()
				c.Gauge(fmt.Sprintf("%s.sum", hname), summ.GetSampleSum(), tags, 1.0)
				c.Gauge(fmt.Sprintf("%s.count", hname), float64(summ.GetSampleCount()), tags, 1.0)
				for _, quantile := range summ.GetQuantile() {
					v := quantile.GetValue()
					if !math.IsNaN(v) {
						c.Gauge(fmt.Sprintf("%s.%dpercentile", hname, int(quantile.GetQuantile()*100)), v, tags, 1.0)
						metricCount++
					}
				}
			}
		default:
			c.Count("veneur.prometheus.unknown_metric_type_total", 1, nil, 1.0)
		}
		c.Count("veneur.prometheus.metrics_flushed_total", metricCount, nil, 1.0)
	}
}
