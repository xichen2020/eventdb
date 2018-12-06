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

// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: github.com/xichen2020/eventdb/generated/proto/encodingpb/int.proto

package encodingpb // import "github.com/xichen2020/eventdb/generated/proto/encodingpb"

import proto "github.com/gogo/protobuf/proto"
import fmt "fmt"
import math "math"

import io "io"

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto.Marshal
var _ = fmt.Errorf
var _ = math.Inf

// This is a compile-time assertion to ensure that this generated file
// is compatible with the proto package it is being compiled against.
// A compilation error at this line likely means your copy of the
// proto package needs to be updated.
const _ = proto.GoGoProtoPackageIsVersion2 // please upgrade the proto package

type IntMeta struct {
	Encoding EncodingType `protobuf:"varint,1,opt,name=encoding,proto3,enum=encodingpb.EncodingType" json:"encoding,omitempty"`
	// Bit packing.
	BitsPerRecord int64 `protobuf:"varint,2,opt,name=bits_per_record,json=bitsPerRecord,proto3" json:"bits_per_record,omitempty"`
	// First value for delta encoding.
	DeltaStart           int64    `protobuf:"varint,3,opt,name=delta_start,json=deltaStart,proto3" json:"delta_start,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *IntMeta) Reset()         { *m = IntMeta{} }
func (m *IntMeta) String() string { return proto.CompactTextString(m) }
func (*IntMeta) ProtoMessage()    {}
func (*IntMeta) Descriptor() ([]byte, []int) {
	return fileDescriptor_int_98eec01ef54fb760, []int{0}
}
func (m *IntMeta) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *IntMeta) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_IntMeta.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (dst *IntMeta) XXX_Merge(src proto.Message) {
	xxx_messageInfo_IntMeta.Merge(dst, src)
}
func (m *IntMeta) XXX_Size() int {
	return m.Size()
}
func (m *IntMeta) XXX_DiscardUnknown() {
	xxx_messageInfo_IntMeta.DiscardUnknown(m)
}

var xxx_messageInfo_IntMeta proto.InternalMessageInfo

func (m *IntMeta) GetEncoding() EncodingType {
	if m != nil {
		return m.Encoding
	}
	return EncodingType_UNKNOWN_ENCODING
}

func (m *IntMeta) GetBitsPerRecord() int64 {
	if m != nil {
		return m.BitsPerRecord
	}
	return 0
}

func (m *IntMeta) GetDeltaStart() int64 {
	if m != nil {
		return m.DeltaStart
	}
	return 0
}

// The string table is stored as an opaque
// byte slice due to bit packing. To get the
// value at table idx, you would seek to idx * bitsPerRecord
// and read bitsPerRecord # of bits into an int.
type IntTable struct {
	Data                 []byte   `protobuf:"bytes,1,opt,name=data,proto3" json:"data,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *IntTable) Reset()         { *m = IntTable{} }
func (m *IntTable) String() string { return proto.CompactTextString(m) }
func (*IntTable) ProtoMessage()    {}
func (*IntTable) Descriptor() ([]byte, []int) {
	return fileDescriptor_int_98eec01ef54fb760, []int{1}
}
func (m *IntTable) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *IntTable) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_IntTable.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (dst *IntTable) XXX_Merge(src proto.Message) {
	xxx_messageInfo_IntTable.Merge(dst, src)
}
func (m *IntTable) XXX_Size() int {
	return m.Size()
}
func (m *IntTable) XXX_DiscardUnknown() {
	xxx_messageInfo_IntTable.DiscardUnknown(m)
}

var xxx_messageInfo_IntTable proto.InternalMessageInfo

func (m *IntTable) GetData() []byte {
	if m != nil {
		return m.Data
	}
	return nil
}

func init() {
	proto.RegisterType((*IntMeta)(nil), "encodingpb.IntMeta")
	proto.RegisterType((*IntTable)(nil), "encodingpb.IntTable")
}
func (m *IntMeta) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *IntMeta) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Encoding != 0 {
		dAtA[i] = 0x8
		i++
		i = encodeVarintInt(dAtA, i, uint64(m.Encoding))
	}
	if m.BitsPerRecord != 0 {
		dAtA[i] = 0x10
		i++
		i = encodeVarintInt(dAtA, i, uint64(m.BitsPerRecord))
	}
	if m.DeltaStart != 0 {
		dAtA[i] = 0x18
		i++
		i = encodeVarintInt(dAtA, i, uint64(m.DeltaStart))
	}
	return i, nil
}

func (m *IntTable) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *IntTable) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if len(m.Data) > 0 {
		dAtA[i] = 0xa
		i++
		i = encodeVarintInt(dAtA, i, uint64(len(m.Data)))
		i += copy(dAtA[i:], m.Data)
	}
	return i, nil
}

func encodeVarintInt(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}
func (m *IntMeta) Size() (n int) {
	var l int
	_ = l
	if m.Encoding != 0 {
		n += 1 + sovInt(uint64(m.Encoding))
	}
	if m.BitsPerRecord != 0 {
		n += 1 + sovInt(uint64(m.BitsPerRecord))
	}
	if m.DeltaStart != 0 {
		n += 1 + sovInt(uint64(m.DeltaStart))
	}
	return n
}

func (m *IntTable) Size() (n int) {
	var l int
	_ = l
	l = len(m.Data)
	if l > 0 {
		n += 1 + l + sovInt(uint64(l))
	}
	return n
}

func sovInt(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozInt(x uint64) (n int) {
	return sovInt(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *IntMeta) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowInt
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: IntMeta: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: IntMeta: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Encoding", wireType)
			}
			m.Encoding = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowInt
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Encoding |= (EncodingType(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 2:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field BitsPerRecord", wireType)
			}
			m.BitsPerRecord = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowInt
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.BitsPerRecord |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field DeltaStart", wireType)
			}
			m.DeltaStart = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowInt
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.DeltaStart |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipInt(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthInt
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func (m *IntTable) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowInt
			}
			if iNdEx >= l {
				return io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		fieldNum := int32(wire >> 3)
		wireType := int(wire & 0x7)
		if wireType == 4 {
			return fmt.Errorf("proto: IntTable: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: IntTable: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 2 {
				return fmt.Errorf("proto: wrong wireType = %d for field Data", wireType)
			}
			var byteLen int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowInt
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				byteLen |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			if byteLen < 0 {
				return ErrInvalidLengthInt
			}
			postIndex := iNdEx + byteLen
			if postIndex > l {
				return io.ErrUnexpectedEOF
			}
			m.Data = append(m.Data[:0], dAtA[iNdEx:postIndex]...)
			if m.Data == nil {
				m.Data = []byte{}
			}
			iNdEx = postIndex
		default:
			iNdEx = preIndex
			skippy, err := skipInt(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthInt
			}
			if (iNdEx + skippy) > l {
				return io.ErrUnexpectedEOF
			}
			iNdEx += skippy
		}
	}

	if iNdEx > l {
		return io.ErrUnexpectedEOF
	}
	return nil
}
func skipInt(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowInt
			}
			if iNdEx >= l {
				return 0, io.ErrUnexpectedEOF
			}
			b := dAtA[iNdEx]
			iNdEx++
			wire |= (uint64(b) & 0x7F) << shift
			if b < 0x80 {
				break
			}
		}
		wireType := int(wire & 0x7)
		switch wireType {
		case 0:
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowInt
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				iNdEx++
				if dAtA[iNdEx-1] < 0x80 {
					break
				}
			}
			return iNdEx, nil
		case 1:
			iNdEx += 8
			return iNdEx, nil
		case 2:
			var length int
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return 0, ErrIntOverflowInt
				}
				if iNdEx >= l {
					return 0, io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				length |= (int(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
			iNdEx += length
			if length < 0 {
				return 0, ErrInvalidLengthInt
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowInt
					}
					if iNdEx >= l {
						return 0, io.ErrUnexpectedEOF
					}
					b := dAtA[iNdEx]
					iNdEx++
					innerWire |= (uint64(b) & 0x7F) << shift
					if b < 0x80 {
						break
					}
				}
				innerWireType := int(innerWire & 0x7)
				if innerWireType == 4 {
					break
				}
				next, err := skipInt(dAtA[start:])
				if err != nil {
					return 0, err
				}
				iNdEx = start + next
			}
			return iNdEx, nil
		case 4:
			return iNdEx, nil
		case 5:
			iNdEx += 4
			return iNdEx, nil
		default:
			return 0, fmt.Errorf("proto: illegal wireType %d", wireType)
		}
	}
	panic("unreachable")
}

var (
	ErrInvalidLengthInt = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowInt   = fmt.Errorf("proto: integer overflow")
)

func init() {
	proto.RegisterFile("github.com/xichen2020/eventdb/generated/proto/encodingpb/int.proto", fileDescriptor_int_98eec01ef54fb760)
}

var fileDescriptor_int_98eec01ef54fb760 = []byte{
	// 257 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x9c, 0xd0, 0xcd, 0x4a, 0x03, 0x31,
	0x14, 0x05, 0x60, 0x63, 0x45, 0xcb, 0xf5, 0x0f, 0xb2, 0x1a, 0x5c, 0xc4, 0xd2, 0x85, 0x74, 0x35,
	0x29, 0xa3, 0x0b, 0xd7, 0x45, 0x17, 0x15, 0x04, 0x19, 0xbb, 0x72, 0x33, 0x24, 0x93, 0xcb, 0x34,
	0x50, 0x93, 0x90, 0xb9, 0x8a, 0x7d, 0x03, 0x97, 0x3e, 0x96, 0x4b, 0x1f, 0x41, 0xc6, 0x17, 0x91,
	0xc6, 0x9f, 0xee, 0xbb, 0x3b, 0x7c, 0x9c, 0x1c, 0xc2, 0x85, 0x49, 0x63, 0x69, 0xfe, 0xa4, 0xf3,
	0xda, 0x3f, 0xca, 0x17, 0x5b, 0xcf, 0xd1, 0x15, 0xe3, 0x62, 0x2c, 0xf1, 0x19, 0x1d, 0x19, 0x2d,
	0x1b, 0x74, 0x18, 0x15, 0xa1, 0x91, 0x21, 0x7a, 0xf2, 0x12, 0x5d, 0xed, 0x8d, 0x75, 0x4d, 0xd0,
	0xd2, 0x3a, 0xca, 0x13, 0x72, 0x58, 0xeb, 0xc9, 0xd5, 0xc6, 0x7b, 0xb4, 0x0c, 0xd8, 0xfe, 0x2c,
	0x0e, 0x5f, 0x19, 0xec, 0x4d, 0x1d, 0xdd, 0x22, 0x29, 0x7e, 0x01, 0xfd, 0xbf, 0x56, 0xc6, 0x06,
	0x6c, 0x74, 0x54, 0x64, 0xf9, 0xfa, 0x59, 0x7e, 0xfd, 0x1b, 0x67, 0xcb, 0x80, 0xe5, 0x7f, 0x93,
	0x9f, 0xc1, 0xb1, 0xb6, 0xd4, 0x56, 0x01, 0x63, 0x15, 0xb1, 0xf6, 0xd1, 0x64, 0xdb, 0x03, 0x36,
	0xea, 0x95, 0x87, 0x2b, 0xbe, 0xc3, 0x58, 0x26, 0xe4, 0xa7, 0xb0, 0x6f, 0x70, 0x41, 0xaa, 0x6a,
	0x49, 0x45, 0xca, 0x7a, 0xa9, 0x03, 0x89, 0xee, 0x57, 0x32, 0x14, 0xd0, 0x9f, 0x3a, 0x9a, 0x29,
	0xbd, 0x40, 0xce, 0x61, 0xc7, 0x28, 0x52, 0xe9, 0x1b, 0x07, 0x65, 0xca, 0x93, 0x9b, 0xf7, 0x4e,
	0xb0, 0x8f, 0x4e, 0xb0, 0xcf, 0x4e, 0xb0, 0xb7, 0x2f, 0xb1, 0xf5, 0x70, 0xb9, 0xe9, 0x09, 0xf4,
	0x6e, 0x92, 0xf3, 0xef, 0x00, 0x00, 0x00, 0xff, 0xff, 0x62, 0x07, 0xf9, 0x2f, 0x95, 0x01, 0x00,
	0x00,
}
