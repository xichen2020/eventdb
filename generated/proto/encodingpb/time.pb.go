// Code generated by protoc-gen-gogo. DO NOT EDIT.
// source: github.com/xichen2020/eventdb/generated/proto/encodingpb/time.proto

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

type ResolutionType int32

const (
	ResolutionType_UNKNOWN_RESOLUTION ResolutionType = 0
	ResolutionType_NANOSECOND         ResolutionType = 1
	ResolutionType_MICROSECOND        ResolutionType = 2
	ResolutionType_MILLISECOND        ResolutionType = 3
	ResolutionType_SECOND             ResolutionType = 4
)

var ResolutionType_name = map[int32]string{
	0: "UNKNOWN_RESOLUTION",
	1: "NANOSECOND",
	2: "MICROSECOND",
	3: "MILLISECOND",
	4: "SECOND",
}
var ResolutionType_value = map[string]int32{
	"UNKNOWN_RESOLUTION": 0,
	"NANOSECOND":         1,
	"MICROSECOND":        2,
	"MILLISECOND":        3,
	"SECOND":             4,
}

func (x ResolutionType) String() string {
	return proto.EnumName(ResolutionType_name, int32(x))
}
func (ResolutionType) EnumDescriptor() ([]byte, []int) {
	return fileDescriptor_time_a3e98ce9cb103f83, []int{0}
}

type TimeMeta struct {
	Encoding   EncodingType   `protobuf:"varint,1,opt,name=encoding,proto3,enum=encodingpb.EncodingType" json:"encoding,omitempty"`
	Resolution ResolutionType `protobuf:"varint,2,opt,name=resolution,proto3,enum=encodingpb.ResolutionType" json:"resolution,omitempty"`
	// Bits per encoded value. All encoded values are bit packed.
	BitsPerEncodedValue  int64    `protobuf:"varint,3,opt,name=bits_per_encoded_value,json=bitsPerEncodedValue,proto3" json:"bits_per_encoded_value,omitempty"`
	NumValues            int32    `protobuf:"varint,4,opt,name=num_values,json=numValues,proto3" json:"num_values,omitempty"`
	MinValue             int64    `protobuf:"varint,5,opt,name=min_value,json=minValue,proto3" json:"min_value,omitempty"`
	MaxValue             int64    `protobuf:"varint,6,opt,name=max_value,json=maxValue,proto3" json:"max_value,omitempty"`
	XXX_NoUnkeyedLiteral struct{} `json:"-"`
	XXX_sizecache        int32    `json:"-"`
}

func (m *TimeMeta) Reset()         { *m = TimeMeta{} }
func (m *TimeMeta) String() string { return proto.CompactTextString(m) }
func (*TimeMeta) ProtoMessage()    {}
func (*TimeMeta) Descriptor() ([]byte, []int) {
	return fileDescriptor_time_a3e98ce9cb103f83, []int{0}
}
func (m *TimeMeta) XXX_Unmarshal(b []byte) error {
	return m.Unmarshal(b)
}
func (m *TimeMeta) XXX_Marshal(b []byte, deterministic bool) ([]byte, error) {
	if deterministic {
		return xxx_messageInfo_TimeMeta.Marshal(b, m, deterministic)
	} else {
		b = b[:cap(b)]
		n, err := m.MarshalTo(b)
		if err != nil {
			return nil, err
		}
		return b[:n], nil
	}
}
func (dst *TimeMeta) XXX_Merge(src proto.Message) {
	xxx_messageInfo_TimeMeta.Merge(dst, src)
}
func (m *TimeMeta) XXX_Size() int {
	return m.Size()
}
func (m *TimeMeta) XXX_DiscardUnknown() {
	xxx_messageInfo_TimeMeta.DiscardUnknown(m)
}

var xxx_messageInfo_TimeMeta proto.InternalMessageInfo

func (m *TimeMeta) GetEncoding() EncodingType {
	if m != nil {
		return m.Encoding
	}
	return EncodingType_UNKNOWN_ENCODING
}

func (m *TimeMeta) GetResolution() ResolutionType {
	if m != nil {
		return m.Resolution
	}
	return ResolutionType_UNKNOWN_RESOLUTION
}

func (m *TimeMeta) GetBitsPerEncodedValue() int64 {
	if m != nil {
		return m.BitsPerEncodedValue
	}
	return 0
}

func (m *TimeMeta) GetNumValues() int32 {
	if m != nil {
		return m.NumValues
	}
	return 0
}

func (m *TimeMeta) GetMinValue() int64 {
	if m != nil {
		return m.MinValue
	}
	return 0
}

func (m *TimeMeta) GetMaxValue() int64 {
	if m != nil {
		return m.MaxValue
	}
	return 0
}

func init() {
	proto.RegisterType((*TimeMeta)(nil), "encodingpb.TimeMeta")
	proto.RegisterEnum("encodingpb.ResolutionType", ResolutionType_name, ResolutionType_value)
}
func (m *TimeMeta) Marshal() (dAtA []byte, err error) {
	size := m.Size()
	dAtA = make([]byte, size)
	n, err := m.MarshalTo(dAtA)
	if err != nil {
		return nil, err
	}
	return dAtA[:n], nil
}

func (m *TimeMeta) MarshalTo(dAtA []byte) (int, error) {
	var i int
	_ = i
	var l int
	_ = l
	if m.Encoding != 0 {
		dAtA[i] = 0x8
		i++
		i = encodeVarintTime(dAtA, i, uint64(m.Encoding))
	}
	if m.Resolution != 0 {
		dAtA[i] = 0x10
		i++
		i = encodeVarintTime(dAtA, i, uint64(m.Resolution))
	}
	if m.BitsPerEncodedValue != 0 {
		dAtA[i] = 0x18
		i++
		i = encodeVarintTime(dAtA, i, uint64(m.BitsPerEncodedValue))
	}
	if m.NumValues != 0 {
		dAtA[i] = 0x20
		i++
		i = encodeVarintTime(dAtA, i, uint64(m.NumValues))
	}
	if m.MinValue != 0 {
		dAtA[i] = 0x28
		i++
		i = encodeVarintTime(dAtA, i, uint64(m.MinValue))
	}
	if m.MaxValue != 0 {
		dAtA[i] = 0x30
		i++
		i = encodeVarintTime(dAtA, i, uint64(m.MaxValue))
	}
	return i, nil
}

func encodeVarintTime(dAtA []byte, offset int, v uint64) int {
	for v >= 1<<7 {
		dAtA[offset] = uint8(v&0x7f | 0x80)
		v >>= 7
		offset++
	}
	dAtA[offset] = uint8(v)
	return offset + 1
}
func (m *TimeMeta) Size() (n int) {
	var l int
	_ = l
	if m.Encoding != 0 {
		n += 1 + sovTime(uint64(m.Encoding))
	}
	if m.Resolution != 0 {
		n += 1 + sovTime(uint64(m.Resolution))
	}
	if m.BitsPerEncodedValue != 0 {
		n += 1 + sovTime(uint64(m.BitsPerEncodedValue))
	}
	if m.NumValues != 0 {
		n += 1 + sovTime(uint64(m.NumValues))
	}
	if m.MinValue != 0 {
		n += 1 + sovTime(uint64(m.MinValue))
	}
	if m.MaxValue != 0 {
		n += 1 + sovTime(uint64(m.MaxValue))
	}
	return n
}

func sovTime(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}
func sozTime(x uint64) (n int) {
	return sovTime(uint64((x << 1) ^ uint64((int64(x) >> 63))))
}
func (m *TimeMeta) Unmarshal(dAtA []byte) error {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		preIndex := iNdEx
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return ErrIntOverflowTime
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
			return fmt.Errorf("proto: TimeMeta: wiretype end group for non-group")
		}
		if fieldNum <= 0 {
			return fmt.Errorf("proto: TimeMeta: illegal tag %d (wire type %d)", fieldNum, wire)
		}
		switch fieldNum {
		case 1:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field Encoding", wireType)
			}
			m.Encoding = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTime
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
				return fmt.Errorf("proto: wrong wireType = %d for field Resolution", wireType)
			}
			m.Resolution = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTime
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.Resolution |= (ResolutionType(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 3:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field BitsPerEncodedValue", wireType)
			}
			m.BitsPerEncodedValue = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTime
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.BitsPerEncodedValue |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 4:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field NumValues", wireType)
			}
			m.NumValues = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTime
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.NumValues |= (int32(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 5:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field MinValue", wireType)
			}
			m.MinValue = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTime
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.MinValue |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		case 6:
			if wireType != 0 {
				return fmt.Errorf("proto: wrong wireType = %d for field MaxValue", wireType)
			}
			m.MaxValue = 0
			for shift := uint(0); ; shift += 7 {
				if shift >= 64 {
					return ErrIntOverflowTime
				}
				if iNdEx >= l {
					return io.ErrUnexpectedEOF
				}
				b := dAtA[iNdEx]
				iNdEx++
				m.MaxValue |= (int64(b) & 0x7F) << shift
				if b < 0x80 {
					break
				}
			}
		default:
			iNdEx = preIndex
			skippy, err := skipTime(dAtA[iNdEx:])
			if err != nil {
				return err
			}
			if skippy < 0 {
				return ErrInvalidLengthTime
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
func skipTime(dAtA []byte) (n int, err error) {
	l := len(dAtA)
	iNdEx := 0
	for iNdEx < l {
		var wire uint64
		for shift := uint(0); ; shift += 7 {
			if shift >= 64 {
				return 0, ErrIntOverflowTime
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
					return 0, ErrIntOverflowTime
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
					return 0, ErrIntOverflowTime
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
				return 0, ErrInvalidLengthTime
			}
			return iNdEx, nil
		case 3:
			for {
				var innerWire uint64
				var start int = iNdEx
				for shift := uint(0); ; shift += 7 {
					if shift >= 64 {
						return 0, ErrIntOverflowTime
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
				next, err := skipTime(dAtA[start:])
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
	ErrInvalidLengthTime = fmt.Errorf("proto: negative length found during unmarshaling")
	ErrIntOverflowTime   = fmt.Errorf("proto: integer overflow")
)

func init() {
	proto.RegisterFile("github.com/xichen2020/eventdb/generated/proto/encodingpb/time.proto", fileDescriptor_time_a3e98ce9cb103f83)
}

var fileDescriptor_time_a3e98ce9cb103f83 = []byte{
	// 360 bytes of a gzipped FileDescriptorProto
	0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x9c, 0x91, 0x4f, 0x8b, 0xda, 0x40,
	0x18, 0xc6, 0x1d, 0xff, 0xa1, 0x6f, 0xc1, 0xca, 0x14, 0x24, 0x58, 0x1a, 0xa4, 0x27, 0xe9, 0x21,
	0x11, 0xed, 0xa1, 0xf4, 0xd6, 0x6a, 0x0e, 0xb6, 0x9a, 0x94, 0xa8, 0x2d, 0xf4, 0x12, 0x12, 0xf3,
	0xae, 0x0e, 0x98, 0x49, 0x48, 0x26, 0xa2, 0x5f, 0x61, 0x4f, 0xfb, 0xb1, 0xf6, 0xb8, 0x1f, 0x61,
	0x71, 0xbf, 0xc8, 0xe2, 0x24, 0xfe, 0xd9, 0xab, 0xb7, 0x97, 0xe7, 0xf7, 0xfc, 0x1e, 0x06, 0x06,
	0x86, 0x2b, 0x26, 0xd6, 0xa9, 0xa7, 0x2d, 0xc3, 0x40, 0xdf, 0xb1, 0xe5, 0x1a, 0x79, 0xbf, 0xd7,
	0xef, 0xe9, 0xb8, 0x45, 0x2e, 0x7c, 0x4f, 0x5f, 0x21, 0xc7, 0xd8, 0x15, 0xe8, 0xeb, 0x51, 0x1c,
	0x8a, 0x50, 0x47, 0xbe, 0x0c, 0x7d, 0xc6, 0x57, 0x91, 0xa7, 0x0b, 0x16, 0xa0, 0x26, 0x53, 0x0a,
	0x97, 0xb8, 0x3d, 0xba, 0x7d, 0x70, 0x1f, 0x61, 0x92, 0x2d, 0x7e, 0xbe, 0x2f, 0x42, 0x6d, 0xce,
	0x02, 0x9c, 0xa2, 0x70, 0xe9, 0x57, 0xa8, 0x9d, 0x6a, 0x0a, 0xe9, 0x90, 0x6e, 0xa3, 0xaf, 0x68,
	0x17, 0x4f, 0x33, 0xf2, 0x73, 0xbe, 0x8f, 0xd0, 0x3e, 0x37, 0xe9, 0x77, 0x80, 0x18, 0x93, 0x70,
	0x93, 0x0a, 0x16, 0x72, 0xa5, 0x28, 0xbd, 0xf6, 0xb5, 0x67, 0x9f, 0xa9, 0x34, 0xaf, 0xda, 0x74,
	0x00, 0x2d, 0x8f, 0x89, 0xc4, 0x89, 0x30, 0x76, 0xa4, 0x81, 0xbe, 0xb3, 0x75, 0x37, 0x29, 0x2a,
	0xa5, 0x0e, 0xe9, 0x96, 0xec, 0x0f, 0x47, 0xfa, 0x07, 0x63, 0x23, 0x63, 0x7f, 0x8f, 0x88, 0x7e,
	0x02, 0xe0, 0x69, 0x90, 0xf5, 0x12, 0xa5, 0xdc, 0x21, 0xdd, 0x8a, 0x5d, 0xe7, 0x69, 0x20, 0x69,
	0x42, 0x3f, 0x42, 0x3d, 0x60, 0x3c, 0x9f, 0xa9, 0xc8, 0x99, 0x5a, 0xc0, 0x78, 0xe6, 0x1e, 0xa1,
	0xbb, 0xcb, 0x61, 0x35, 0x87, 0xee, 0x4e, 0xc2, 0x2f, 0x77, 0xd0, 0x78, 0xfb, 0x56, 0xda, 0x02,
	0xba, 0x30, 0x7f, 0x9b, 0xd6, 0x3f, 0xd3, 0xb1, 0x8d, 0x99, 0x35, 0x59, 0xcc, 0xc7, 0x96, 0xd9,
	0x2c, 0xd0, 0x06, 0x80, 0xf9, 0xc3, 0xb4, 0x66, 0xc6, 0xd0, 0x32, 0x47, 0x4d, 0x42, 0xdf, 0xc3,
	0xbb, 0xe9, 0x78, 0x68, 0x9f, 0x82, 0x62, 0x16, 0x4c, 0x26, 0xe3, 0x3c, 0x28, 0x51, 0x80, 0x6a,
	0x7e, 0x97, 0x7f, 0xfe, 0x7a, 0x3c, 0xa8, 0xe4, 0xe9, 0xa0, 0x92, 0xe7, 0x83, 0x4a, 0x1e, 0x5e,
	0xd4, 0xc2, 0xff, 0x6f, 0xb7, 0x7e, 0xa6, 0x57, 0x95, 0xc9, 0xe0, 0x35, 0x00, 0x00, 0xff, 0xff,
	0x40, 0x83, 0x31, 0xc6, 0x60, 0x02, 0x00, 0x00,
}
