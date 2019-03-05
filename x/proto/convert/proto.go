package convert

import (
	"time"

	"github.com/xichen2020/eventdb/generated/proto/servicepb"
	xtime "github.com/xichen2020/eventdb/x/time"
)

// Int64PtrToOptionalInt64 converts an int64 pointer to an optional int64 protobuf message.
func Int64PtrToOptionalInt64(v *int64) servicepb.OptionalInt64 {
	if v == nil {
		noValue := &servicepb.OptionalInt64_NoValue{NoValue: true}
		return servicepb.OptionalInt64{Value: noValue}
	}
	value := &servicepb.OptionalInt64_Data{Data: *v}
	return servicepb.OptionalInt64{Value: value}
}

// IntPtrToOptionalInt64 converts an int pointer to an optional int64 protobuf message.
func IntPtrToOptionalInt64(v *int) servicepb.OptionalInt64 {
	if v == nil {
		noValue := &servicepb.OptionalInt64_NoValue{NoValue: true}
		return servicepb.OptionalInt64{Value: noValue}
	}
	value := &servicepb.OptionalInt64_Data{Data: int64(*v)}
	return servicepb.OptionalInt64{Value: value}
}

// DurationPtrToOptionalInt64 converts a duration pointer to an optinal int64 protobuf message.
func DurationPtrToOptionalInt64(v *xtime.Duration) servicepb.OptionalInt64 {
	if v == nil {
		noValue := &servicepb.OptionalInt64_NoValue{NoValue: true}
		return servicepb.OptionalInt64{Value: noValue}
	}
	value := &servicepb.OptionalInt64_Data{Data: time.Duration(*v).Nanoseconds()}
	return servicepb.OptionalInt64{Value: value}
}

// StringPtrToOptionalString converts a string pointer to an optional string protobuf message.
func StringPtrToOptionalString(v *string) servicepb.OptionalString {
	if v == nil {
		noValue := &servicepb.OptionalString_NoValue{NoValue: true}
		return servicepb.OptionalString{Value: noValue}
	}
	value := &servicepb.OptionalString_Data{Data: *v}
	return servicepb.OptionalString{Value: value}
}
