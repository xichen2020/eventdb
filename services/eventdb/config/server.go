package config

import (
	"strings"
	"time"

	"github.com/xichen2020/eventdb/parser/json"
	"github.com/xichen2020/eventdb/server/http"
	"github.com/xichen2020/eventdb/server/http/handlers"

	"github.com/m3db/m3x/instrument"
)

// HTTPServerConfiguration contains http server configuration.
type HTTPServerConfiguration struct {
	// HTTP server listening address.
	ListenAddress string `yaml:"listenAddress" validate:"nonzero"`

	// HTTP server read timeout.
	ReadTimeout time.Duration `yaml:"readTimeout"`

	// HTTP server write timeout.
	WriteTimeout time.Duration `yaml:"writeTimeout"`

	// Handler configuration
	Handler handlerConfiguration `yaml:"handler"`
}

// NewServerOptions create a new set of http server options.
func (c *HTTPServerConfiguration) NewServerOptions() *http.Options {
	opts := http.NewOptions()
	if c.ReadTimeout != 0 {
		opts = opts.SetReadTimeout(c.ReadTimeout)
	}
	if c.WriteTimeout != 0 {
		opts = opts.SetWriteTimeout(c.WriteTimeout)
	}
	return opts
}

type handlerConfiguration struct {
	ParserPool *json.ParserPoolConfiguration `yaml:"parserPool"`
	Parser     *parserConfiguration          `yaml:"parser"`
}

func (c *handlerConfiguration) NewOptions(
	instrumentOpts instrument.Options,
) *handlers.Options {
	opts := handlers.NewOptions().
		SetInstrumentOptions(instrumentOpts)

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
