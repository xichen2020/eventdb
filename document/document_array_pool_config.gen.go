// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package document

import (
	"github.com/m3db/m3/src/x/instrument"
)

// DocumentArrayPoolWatermarkConfiguration contains watermark configuration for pools.
type DocumentArrayPoolWatermarkConfiguration struct {
	// The low watermark to start refilling the pool, if zero none.
	RefillLowWatermark float64 `yaml:"low" validate:"min=0.0,max=1.0"`

	// The high watermark to stop refilling the pool, if zero none.
	RefillHighWatermark float64 `yaml:"high" validate:"min=0.0,max=1.0"`
}

// DocumentArrayPoolConfiguration contains pool configuration.
type DocumentArrayPoolConfiguration struct {
	// The size of the pool.
	Size *int `yaml:"size"`

	// The watermark configuration.
	Watermark DocumentArrayPoolWatermarkConfiguration `yaml:"watermark"`
}

// NewPoolOptions creates a new set of pool options.
func (c *DocumentArrayPoolConfiguration) NewPoolOptions(
	instrumentOpts instrument.Options,
) *DocumentArrayPoolOptions {
	opts := NewDocumentArrayPoolOptions().
		SetInstrumentOptions(instrumentOpts).
		SetRefillLowWatermark(c.Watermark.RefillLowWatermark).
		SetRefillHighWatermark(c.Watermark.RefillHighWatermark)
	if c.Size != nil {
		opts = opts.SetSize(*c.Size)
	}
	return opts
}
