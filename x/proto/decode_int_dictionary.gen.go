// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

// This file was automatically generated by genny.
// Any changes will be lost if this file is regenerated.
// see https://github.com/mauricelam/genny

package proto

import (
	"encoding/binary"

	"io"

	"github.com/xichen2020/eventdb/generated/proto/encodingpb"
	"github.com/xichen2020/eventdb/x/bytes"

	xio "github.com/xichen2020/eventdb/x/io"
)

// *encodingpb.IntDictionary is a generic gogo protobuf generated Message type.

// DecodeIntDictionary decodes a *encodingpb.IntDictionary message into a `io.Writer`.
func DecodeIntDictionary(
	msg *encodingpb.IntDictionary,
	extBuf *[]byte, // extBuf is an external byte buffer for memory re-use.
	reader xio.Reader,
) error {
	protoSizeBytes, err := binary.ReadVarint(reader)
	if err != nil {
		return err
	}
	*extBuf = bytes.EnsureBufferSize(*extBuf, int(protoSizeBytes), bytes.DontCopyData)
	if _, err := io.ReadFull(reader, (*extBuf)[:protoSizeBytes]); err != nil {
		return err
	}
	return msg.Unmarshal((*extBuf)[:protoSizeBytes])
}
