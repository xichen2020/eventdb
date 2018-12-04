package config

import (
	"fmt"

	"github.com/xichen2020/eventdb/storage"
)

// DatabaseConfiguration encodes db config options.
type DatabaseConfiguration struct {
	Namespaces                   Namespaces `yaml:"namespaces"`
	NumShards                    int        `yaml:"numShards"`
	NamespaceFieldName           *string    `yaml:"namespaceFieldName"`
	FieldPathSeparator           *Separator `yaml:"fieldPathSeparator"`
	TimestampFieldName           *string    `yaml:"timestampFieldName"`
	MaxNumCachedSegmentsPerShard *int       `yaml:"maxNumCachedSegmentsPerShard"`
}

// NewDatabaseOptions creates a new set of database options from the supplied config.
func (d *DatabaseConfiguration) NewDatabaseOptions() *storage.Options {
	opts := storage.NewOptions()
	if d.FieldPathSeparator != nil {
		opts = opts.SetFieldPathSeparator(byte(*d.FieldPathSeparator))
	}
	if d.NamespaceFieldName != nil {
		opts = opts.SetNamespaceFieldName(*d.NamespaceFieldName)
	}
	if d.TimestampFieldName != nil {
		opts = opts.SetTimestampFieldName(*d.TimestampFieldName)
	}
	if d.MaxNumCachedSegmentsPerShard != nil {
		opts = opts.SetMaxNumCachedSegmentsPerShard(*d.MaxNumCachedSegmentsPerShard)
	}
	return opts
}

// Namespaces is a custom type for enforcing unmarshaling rules.
type Namespaces [][]byte

// UnmarshalYAML implements the Unmarshaler interface for the `Namespaces` type.
func (n *Namespaces) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var namespaces []string
	if err := unmarshal(&namespaces); err != nil {
		return err
	}

	nss := make([][]byte, len(namespaces))

	for idx, ns := range namespaces {
		nss[idx] = []byte(ns)
	}

	*n = nss

	return nil
}

// Separator is a custom type for enforcing unmarshaling rules.
type Separator byte

// UnmarshalYAML implements the Unmarshaler interface for the `Separator` type.
func (s *Separator) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var separatorString string
	if err := unmarshal(&separatorString); err != nil {
		return err
	}
	sepBytes := []byte(separatorString)
	if len(sepBytes) != 1 {
		return fmt.Errorf("separator (%s) must be a string consisting of a single byte", separatorString)
	}
	*s = Separator(sepBytes[0])
	return nil
}
