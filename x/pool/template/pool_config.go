package template

import "github.com/m3db/m3/src/x/instrument"

// ValuePoolWatermarkConfiguration contains watermark configuration for pools.
type ValuePoolWatermarkConfiguration struct {
	// The low watermark to start refilling the pool, if zero none.
	RefillLowWatermark float64 `yaml:"low" validate:"min=0.0,max=1.0"`

	// The high watermark to stop refilling the pool, if zero none.
	RefillHighWatermark float64 `yaml:"high" validate:"min=0.0,max=1.0"`
}

// ValuePoolConfiguration contains pool configuration.
type ValuePoolConfiguration struct {
	// The size of the pool.
	Size *int `yaml:"size"`

	// The watermark configuration.
	Watermark ValuePoolWatermarkConfiguration `yaml:"watermark"`
}

// NewPoolOptions creates a new set of pool options.
func (c *ValuePoolConfiguration) NewPoolOptions(
	instrumentOpts instrument.Options,
) *ValuePoolOptions {
	opts := NewValuePoolOptions().
		SetInstrumentOptions(instrumentOpts).
		SetRefillLowWatermark(c.Watermark.RefillLowWatermark).
		SetRefillHighWatermark(c.Watermark.RefillHighWatermark)
	if c.Size != nil {
		opts = opts.SetSize(*c.Size)
	}
	return opts
}
