package fs

import (
	"encoding/binary"
	"unsafe"

	xhash "github.com/xichen2020/eventdb/x/hash"
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
	checkSumSize          = int(unsafe.Sizeof(xhash.Hash(0)))
)

var (
	endianness  = binary.LittleEndian
	magicHeader = []byte("0xdeadbeef")
)
