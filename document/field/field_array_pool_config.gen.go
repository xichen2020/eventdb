// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package field

import (
	"github.com/m3db/m3x/instrument"
)

// FieldArrayPoolWatermarkConfiguration contains watermark configuration for pools.
type FieldArrayPoolWatermarkConfiguration struct {
	// The low watermark to start refilling the pool, if zero none.
	RefillLowWatermark float64 `yaml:"low" validate:"min=0.0,max=1.0"`

	// The high watermark to stop refilling the pool, if zero none.
	RefillHighWatermark float64 `yaml:"high" validate:"min=0.0,max=1.0"`
}

// FieldArrayPoolConfiguration contains pool configuration.
type FieldArrayPoolConfiguration struct {
	// The size of the pool.
	Size *int `yaml:"size"`

	// The watermark configuration.
	Watermark FieldArrayPoolWatermarkConfiguration `yaml:"watermark"`
}

// NewPoolOptions creates a new set of pool options.
func (c *FieldArrayPoolConfiguration) NewPoolOptions(
	instrumentOpts instrument.Options,
) *FieldArrayPoolOptions {
	opts := NewFieldArrayPoolOptions().
		SetInstrumentOptions(instrumentOpts).
		SetRefillLowWatermark(c.Watermark.RefillLowWatermark).
		SetRefillHighWatermark(c.Watermark.RefillHighWatermark)
	if c.Size != nil {
		opts = opts.SetSize(*c.Size)
	}
	return opts
}
