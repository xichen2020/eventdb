// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package pool

import (
	"github.com/m3db/m3/src/x/instrument"
)

// Float64ArrayPoolWatermarkConfiguration contains watermark configuration for pools.
type Float64ArrayPoolWatermarkConfiguration struct {
	// The low watermark to start refilling the pool, if zero none.
	RefillLowWatermark float64 `yaml:"low" validate:"min=0.0,max=1.0"`

	// The high watermark to stop refilling the pool, if zero none.
	RefillHighWatermark float64 `yaml:"high" validate:"min=0.0,max=1.0"`
}

// Float64ArrayPoolConfiguration contains pool configuration.
type Float64ArrayPoolConfiguration struct {
	// The size of the pool.
	Size *int `yaml:"size"`

	// The watermark configuration.
	Watermark Float64ArrayPoolWatermarkConfiguration `yaml:"watermark"`
}

// NewPoolOptions creates a new set of pool options.
func (c *Float64ArrayPoolConfiguration) NewPoolOptions(
	instrumentOpts instrument.Options,
) *Float64ArrayPoolOptions {
	opts := NewFloat64ArrayPoolOptions().
		SetInstrumentOptions(instrumentOpts).
		SetRefillLowWatermark(c.Watermark.RefillLowWatermark).
		SetRefillHighWatermark(c.Watermark.RefillHighWatermark)
	if c.Size != nil {
		opts = opts.SetSize(*c.Size)
	}
	return opts
}
