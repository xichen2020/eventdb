package fs

import (
	"bytes"
	"fmt"
	"os"
	"path"
	"strconv"

	"github.com/xichen2020/eventdb/persist"
	"github.com/xichen2020/eventdb/x/unsafe"
)

// namespaceDataDirPath returns the path to the directory for a given namespace.
func namespaceDataDirPath(prefix string, namespace []byte) string {
	return path.Join(prefix, dataDirName, string(namespace))
}

// shardDataDirPath returns the path to the data directory for a given shard.
func shardDataDirPath(prefix string, namespace []byte, shard uint32) string {
	namespacePath := namespaceDataDirPath(prefix, namespace)
	return path.Join(namespacePath, strconv.Itoa(int(shard)))
}

func segmentDirPath(
	dirPrefix string,
	segmentMeta persist.SegmentMetadata,
) string {
	dirName := fmt.Sprintf(
		"%s%s%d%s%d%s%s",
		segmentDirPrefix,
		separator,
		segmentMeta.MinTimeNanos,
		separator,
		segmentMeta.MaxTimeNanos,
		separator,
		segmentMeta.ID,
	)
	return path.Join(dirPrefix, dirName)
}

func segmentFilePath(segmentDirPath, fname string) string {
	fullName := fmt.Sprintf("%s%s", fname, segmentFileSuffix)
	return path.Join(segmentDirPath, fullName)
}

func infoFilePath(segmentDirPath string) string {
	return segmentFilePath(segmentDirPath, infoFileName)
}

func checkpointFilePath(segmentDirPath string) string {
	return segmentFilePath(segmentDirPath, checkpointFileName)
}

func fileExists(filePath string) (bool, error) {
	_, err := os.Stat(filePath)
	if err == nil {
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func fieldDataFilePath(
	segmentDirPath string,
	fieldPath []string,
	fieldSeparator string,
	buf *bytes.Buffer,
) string {
	buf.Reset()
	for i, p := range fieldPath {
		buf.WriteString(p)
		if i < len(fieldPath)-1 {
			buf.WriteString(fieldSeparator)
		}
	}
	buf.WriteString(fieldDataFileSuffix)
	b := buf.Bytes()
	return segmentFilePath(segmentDirPath, unsafe.ToString(b))
}

// openWritable opens a file for writing and truncating as necessary.
func openWritable(filePath string, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
}
