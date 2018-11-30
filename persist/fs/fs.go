package fs

import (
	"encoding/binary"
)

const (
	dataDirName         = "data"
	segmentDirPrefix    = "segment"
	infoFileName        = "info"
	checkpointFileName  = "checkpoint"
	fieldDataFileSuffix = ".fieldData"
	segmentFileSuffix   = ".db"
	separator           = "-"

	// maximum number of bytes to encode message size.
	maxMessageSizeInBytes = binary.MaxVarintLen32
)

var (
	magicHeader = []byte("0xdeadbeef")
)
