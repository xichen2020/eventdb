package config

import (
	"strings"
	"time"

	"github.com/xichen2020/eventdb/parser/json"
	"github.com/xichen2020/eventdb/server/http"
	"github.com/xichen2020/eventdb/server/http/handlers"

	"github.com/m3db/m3/src/x/instrument"
)

// HTTPServerConfiguration contains http server configuration.
type HTTPServerConfiguration struct {
	// HTTP server listening address.
	ListenAddress string `yaml:"listenAddress" validate:"nonzero"`

	// Service configuration.
	Service httpServiceConfiguration `yaml:"service"`
}

// NewServerOptions create a new set of http server options.
func (c *HTTPServerConfiguration) NewServerOptions(instrumentOpts instrument.Options) *http.Options {
	opts := http.NewOptions().
		SetInstrumentOptions(instrumentOpts)
	return opts
}

type httpServiceConfiguration struct {
	ReadTimeout  *time.Duration                `yaml:"readTimeout"`
	WriteTimeout *time.Duration                `yaml:"writeTimeout"`
	ParserPool   *json.ParserPoolConfiguration `yaml:"parserPool"`
	Parser       *parserConfiguration          `yaml:"parser"`
}

func (c *httpServiceConfiguration) NewOptions(
	instrumentOpts instrument.Options,
) *handlers.Options {
	opts := handlers.NewOptions().
		SetInstrumentOptions(instrumentOpts)

	if c.ReadTimeout != nil {
		opts = opts.SetReadTimeout(*c.ReadTimeout)
	}
	if c.WriteTimeout != nil {
		opts = opts.SetWriteTimeout(*c.WriteTimeout)
	}
	scope := instrumentOpts.MetricsScope()
	// Initialize parser pool.
	var poolOpts *json.ParserPoolOptions
	if c.ParserPool != nil {
		iOpts := instrumentOpts.SetMetricsScope(scope.SubScope("parser-pool"))
		poolOpts = c.ParserPool.NewPoolOptions(iOpts)
	}
	parserPool := json.NewParserPool(poolOpts)
	var parserOpts *json.Options
	if c.Parser != nil {
		parserOpts = c.Parser.NewOptions()
	}
	parserPool.Init(func() json.Parser { return json.NewParser(parserOpts) })
	opts = opts.SetParserPool(parserPool)

	return opts
}

type parserConfiguration struct {
	MaxDepth         *int    `yaml:"maxDepth"`
	ExcludeKeySuffix *string `yaml:"excludeKeySuffix"`
}

func (c *parserConfiguration) NewOptions() *json.Options {
	opts := json.NewOptions()
	if c.MaxDepth != nil {
		opts = opts.SetMaxDepth(*c.MaxDepth)
	}
	if c.ExcludeKeySuffix != nil {
		opts = opts.SetObjectKeyFilterFn(func(key string) bool {
			return strings.HasSuffix(key, *c.ExcludeKeySuffix)
		})
	}
	return opts
}
