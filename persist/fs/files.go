package fs

import (
	"bytes"
	"fmt"
	"os"
	"path"

	"github.com/xichen2020/eventdb/document/field"
	"github.com/xichen2020/eventdb/index/segment"
)

// namespaceDataDirPath returns the path to the directory for a given namespace.
func namespaceDataDirPath(prefix string, namespace []byte) string {
	return path.Join(prefix, dataDirName, string(namespace))
}

func segmentDirPath(
	filePathPrefix string,
	namespace []byte,
	segmentMeta segment.Metadata,
) string {
	segmentDir := namespaceDataDirPath(filePathPrefix, namespace)
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
	return path.Join(segmentDir, dirName)
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
	fieldType field.ValueType,
	fieldSeparator string,
	buf *bytes.Buffer,
) string {
	buf.Reset()
	for _, p := range fieldPath {
		buf.WriteString(p)
		buf.WriteString(fieldSeparator)
	}
	buf.WriteString(fieldType.String())
	buf.WriteString(fieldDataFileSuffix)
	return segmentFilePath(segmentDirPath, buf.String())
}

// openWritable opens a file for writing and truncating as necessary.
func openWritable(filePath string, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, perm)
}
