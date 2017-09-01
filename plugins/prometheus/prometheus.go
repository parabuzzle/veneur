package prometheus

import (
	"bytes"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	"github.com/DataDog/datadog-go/statsd"
	"github.com/Sirupsen/logrus"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stripe/veneur/plugins"
	"github.com/stripe/veneur/samplers"
)

var _ plugins.Plugin = &PrometheusPlugin{}

// A helper type that we use to allow a `Len()` call
// on an io.Reader
type lengther interface {
	Len() int
}

// PrometheusPlugin is a plugin for emitting metrics to InfluxDB.
type PrometheusPlugin struct {
	Logger     *logrus.Logger
	InfluxURL  string
	HTTPClient *http.Client
	Statsd     *statsd.Client
}

// NewPrometheusPlugin creates a new Influx Plugin.
func NewPrometheusPlugin(logger *logrus.Logger, addr string, consistency string, db string, client *http.Client, stats *statsd.Client) *PrometheusPlugin {
	plugin := &PrometheusPlugin{
		Logger:     logger,
		HTTPClient: client,
		Statsd:     stats,
	}

	logger.Warn("HELLO WORLD PROMETHEUS")
	// inurl, err := url.Parse(addr)
	// if err != nil {
	// 	logger.Fatalf("Error parsing URL for InfluxDB: %q", err)
	// }
	//
	// // Construct a path we will be using later.
	// inurl.Path = "/write"
	// q := inurl.Query()
	// q.Set("db", db)
	// q.Set("precision", "s")
	// inurl.RawQuery = q.Encode()
	// plugin.InfluxURL = inurl.String()

	return plugin
}

// Flush sends a slice of metrics to InfluxDB
func (p *PrometheusPlugin) Flush(metrics []samplers.DDMetric, hostname string) error {
	p.Logger.Warn("FLUSHIN!")
	p.Statsd.Gauge("flush.post_metrics_total", float64(len(metrics)), nil, 1.0)
	// Check to see if we have anything to do
	if len(metrics) == 0 {
		p.Logger.Info("Nothing to flush, skipping.")
		return nil
	}

	buff := bytes.Buffer{}
	colons := regexp.MustCompile(":")
	for _, metric := range metrics {
		tags := strings.Join(metric.Tags, ",")
		// This is messy and we shouldn't have to do it this way, but since Veneur treats tags as arbitrary strings
		// rather than name value pairs, we have to do this ugly conversion
		cleanTags := colons.ReplaceAllLiteralString(tags, "=")
		buff.WriteString(
			fmt.Sprintf("%s,%s value=%f %d\n", metric.Name, cleanTags, metric.Value[0][1], int64(metric.Value[0][0])),
		)
	}

	g := prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "foo",
		Subsystem: "bar",
		Name:      "baz",
	}, []string{"fart"})
	g.With(prometheus.Labels{"fart": "bar"}).Set(1.0)
	if err := prometheus.Register(g); err != nil {
		p.Logger.WithError(err).Warn("DAMN")
	}

	return nil
}

// Name returns the name of the plugin.
func (p *PrometheusPlugin) Name() string {
	return "prometheus"
}
