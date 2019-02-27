// Pooling related template instantiations.

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/parser/json/value/value_pool.gen.go -pkg=value gen \"ValuePoolOptions=PoolOptions valuePoolMetrics=poolMetrics ValuePool=Pool GenericValue=*Value\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/parser/json/value/value_array_pool.gen.go -pkg=value gen \"ValuePoolOptions=ArrayPoolOptions valuePoolMetrics=arrayPoolMetrics ValuePool=ArrayPool GenericValue=Array\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/bucketized_pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/parser/json/value/bucketized_value_array_pool.gen.go -pkg=value gen \"ValueBucket=ArrayBucket ValuePoolOptions=ArrayPoolOptions ValuePool=ArrayPool valueBucketByCapacity=arrayBucketByCapacity bucketPool=arrayBucketPool BucketizedValuePool=BucketizedArrayPool GenericValue=Array\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/parser/json/value/kv_array_pool.gen.go -pkg=value gen \"ValuePoolOptions=KVArrayPoolOptions valuePoolMetrics=kvArrayPoolMetrics ValuePool=KVArrayPool GenericValue=KVArray\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/bucketized_pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/parser/json/value/bucketized_kv_array_pool.gen.go -pkg=value gen \"ValueBucket=KVArrayBucket ValuePoolOptions=KVArrayPoolOptions ValuePool=KVArrayPool valueBucketByCapacity=kvArrayBucketByCapacity bucketPool=kvArrayBucketPool BucketizedValuePool=BucketizedKVArrayPool GenericValue=KVArray\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool_config.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/parser/json/parser_pool_config.gen.go -pkg=json gen \"ValuePoolWatermarkConfiguration=ParserPoolWatermarkConfiguration ValuePoolConfiguration=ParserPoolConfiguration ValuePoolOptions=ParserPoolOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/parser/json/parser_pool.gen.go -pkg=json gen \"ValuePoolOptions=ParserPoolOptions valuePoolMetrics=parserPoolMetrics ValuePool=ParserPool GenericValue=Parser\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool_config.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/bool_array_pool_config.gen.go -pkg=pool gen \"ValuePoolWatermarkConfiguration=BoolArrayPoolWatermarkConfiguration ValuePoolConfiguration=BoolArrayPoolConfiguration ValuePoolOptions=BoolArrayPoolOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/bool_array_pool.gen.go -pkg=pool gen \"ValuePoolOptions=BoolArrayPoolOptions valuePoolMetrics=boolArrayPoolMetrics ValuePool=BoolArrayPool GenericValue=[]bool\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/bucketized_pool_config.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/bucketized_bool_array_pool_config.gen.go -pkg=pool gen \"ValuePoolBucketConfiguration=BoolArrayBucketConfiguration ValueBucket=BoolArrayBucket ValuePoolWatermarkConfiguration=BoolArrayPoolWatermarkConfiguration BucketizedValuePoolConfiguration=BucketizedBoolArrayPoolConfiguration ValuePoolOptions=BoolArrayPoolOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/bucketized_pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/bucketized_bool_array_pool.gen.go -pkg=pool gen \"ValueBucket=BoolArrayBucket ValuePoolOptions=BoolArrayPoolOptions ValuePool=BoolArrayPool valueBucketByCapacity=boolArrayBucketByCapacity bucketPool=boolArrayBucketPool BucketizedValuePool=BucketizedBoolArrayPool GenericValue=[]bool\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/ref_counted_pooled_array.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/ref_counted_pooled_bool_array.gen.go -pkg=pool gen \"GenericBucketizedValueArrayPool=*BucketizedBoolArrayPool RefCountedPooledGenericValueArray=RefCountedPooledBoolArray GenericValue=bool\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool_config.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/int_array_pool_config.gen.go -pkg=pool gen \"ValuePoolWatermarkConfiguration=IntArrayPoolWatermarkConfiguration ValuePoolConfiguration=IntArrayPoolConfiguration ValuePoolOptions=IntArrayPoolOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/int_array_pool.gen.go -pkg=pool gen \"ValuePoolOptions=IntArrayPoolOptions valuePoolMetrics=intArrayPoolMetrics ValuePool=IntArrayPool GenericValue=[]int\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/bucketized_pool_config.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/bucketized_int_array_pool_config.gen.go -pkg=pool gen \"ValuePoolBucketConfiguration=IntArrayBucketConfiguration ValueBucket=IntArrayBucket ValuePoolWatermarkConfiguration=IntArrayPoolWatermarkConfiguration BucketizedValuePoolConfiguration=BucketizedIntArrayPoolConfiguration ValuePoolOptions=IntArrayPoolOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/bucketized_pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/bucketized_int_array_pool.gen.go -pkg=pool gen \"ValueBucket=IntArrayBucket ValuePoolOptions=IntArrayPoolOptions ValuePool=IntArrayPool valueBucketByCapacity=intArrayBucketByCapacity bucketPool=intArrayBucketPool BucketizedValuePool=BucketizedIntArrayPool GenericValue=[]int\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/ref_counted_pooled_array.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/ref_counted_pooled_int_array.gen.go -pkg=pool gen \"GenericBucketizedValueArrayPool=*BucketizedIntArrayPool RefCountedPooledGenericValueArray=RefCountedPooledIntArray GenericValue=int\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool_config.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/int64_array_pool_config.gen.go -pkg=pool gen \"ValuePoolWatermarkConfiguration=Int64ArrayPoolWatermarkConfiguration ValuePoolConfiguration=Int64ArrayPoolConfiguration ValuePoolOptions=Int64ArrayPoolOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/int64_array_pool.gen.go -pkg=pool gen \"ValuePoolOptions=Int64ArrayPoolOptions valuePoolMetrics=int64ArrayPoolMetrics ValuePool=Int64ArrayPool GenericValue=[]int64\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/bucketized_pool_config.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/bucketized_int64_array_pool_config.gen.go -pkg=pool gen \"ValuePoolBucketConfiguration=Int64ArrayBucketConfiguration ValueBucket=Int64ArrayBucket ValuePoolWatermarkConfiguration=Int64ArrayPoolWatermarkConfiguration BucketizedValuePoolConfiguration=BucketizedInt64ArrayPoolConfiguration ValuePoolOptions=Int64ArrayPoolOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/bucketized_pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/bucketized_int64_array_pool.gen.go -pkg=pool gen \"ValueBucket=Int64ArrayBucket ValuePoolOptions=Int64ArrayPoolOptions ValuePool=Int64ArrayPool valueBucketByCapacity=int64ArrayBucketByCapacity bucketPool=int64ArrayBucketPool BucketizedValuePool=BucketizedInt64ArrayPool GenericValue=[]int64\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/ref_counted_pooled_array.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/ref_counted_pooled_int64_array.gen.go -pkg=pool gen \"GenericBucketizedValueArrayPool=*BucketizedInt64ArrayPool RefCountedPooledGenericValueArray=RefCountedPooledInt64Array GenericValue=int64\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool_config.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/float64_array_pool_config.gen.go -pkg=pool gen \"ValuePoolWatermarkConfiguration=Float64ArrayPoolWatermarkConfiguration ValuePoolConfiguration=Float64ArrayPoolConfiguration ValuePoolOptions=Float64ArrayPoolOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/float64_array_pool.gen.go -pkg=pool gen \"ValuePoolOptions=Float64ArrayPoolOptions valuePoolMetrics=float64ArrayPoolMetrics ValuePool=Float64ArrayPool GenericValue=[]float64\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/bucketized_pool_config.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/bucketized_float64_array_pool_config.gen.go -pkg=pool gen \"ValuePoolBucketConfiguration=Float64ArrayBucketConfiguration ValueBucket=Float64ArrayBucket ValuePoolWatermarkConfiguration=Float64ArrayPoolWatermarkConfiguration BucketizedValuePoolConfiguration=BucketizedFloat64ArrayPoolConfiguration ValuePoolOptions=Float64ArrayPoolOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/bucketized_pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/bucketized_float64_array_pool.gen.go -pkg=pool gen \"ValueBucket=Float64ArrayBucket ValuePoolOptions=Float64ArrayPoolOptions ValuePool=Float64ArrayPool valueBucketByCapacity=float64ArrayBucketByCapacity bucketPool=float64ArrayBucketPool BucketizedValuePool=BucketizedFloat64ArrayPool GenericValue=[]float64\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/ref_counted_pooled_array.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/ref_counted_pooled_float64_array.gen.go -pkg=pool gen \"GenericBucketizedValueArrayPool=*BucketizedFloat64ArrayPool RefCountedPooledGenericValueArray=RefCountedPooledFloat64Array GenericValue=float64\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool_config.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/string_array_pool_config.gen.go -pkg=pool gen \"ValuePoolWatermarkConfiguration=StringArrayPoolWatermarkConfiguration ValuePoolConfiguration=StringArrayPoolConfiguration ValuePoolOptions=StringArrayPoolOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/string_array_pool.gen.go -pkg=pool gen \"ValuePoolOptions=StringArrayPoolOptions valuePoolMetrics=stringArrayPoolMetrics ValuePool=StringArrayPool GenericValue=[]string\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/bucketized_pool_config.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/bucketized_string_array_pool_config.gen.go -pkg=pool gen \"ValuePoolBucketConfiguration=StringArrayBucketConfiguration ValueBucket=StringArrayBucket ValuePoolWatermarkConfiguration=StringArrayPoolWatermarkConfiguration BucketizedValuePoolConfiguration=BucketizedStringArrayPoolConfiguration ValuePoolOptions=StringArrayPoolOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/bucketized_pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/bucketized_string_array_pool.gen.go -pkg=pool gen \"ValueBucket=StringArrayBucket ValuePoolOptions=StringArrayPoolOptions ValuePool=StringArrayPool valueBucketByCapacity=stringArrayBucketByCapacity bucketPool=stringArrayBucketPool BucketizedValuePool=BucketizedStringArrayPool GenericValue=[]string\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/ref_counted_pooled_array.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/ref_counted_pooled_string_array.gen.go -pkg=pool gen \"GenericBucketizedValueArrayPool=*BucketizedStringArrayPool RefCountedPooledGenericValueArray=RefCountedPooledStringArray GenericValue=string\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool_config.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/document/document_array_pool_config.gen.go -pkg=document gen \"ValuePoolWatermarkConfiguration=DocumentArrayPoolWatermarkConfiguration ValuePoolConfiguration=DocumentArrayPoolConfiguration ValuePoolOptions=DocumentArrayPoolOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/document/document_array_pool.gen.go -pkg=document gen \"ValuePoolOptions=DocumentArrayPoolOptions valuePoolMetrics=documentArrayPoolMetrics ValuePool=DocumentArrayPool GenericValue=[]Document\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/bucketized_pool_config.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/document/bucketized_document_array_pool_config.gen.go -pkg=document gen \"ValuePoolBucketConfiguration=DocumentArrayBucketConfiguration ValueBucket=DocumentArrayBucket ValuePoolWatermarkConfiguration=DocumentArrayPoolWatermarkConfiguration BucketizedValuePoolConfiguration=BucketizedDocumentArrayPoolConfiguration ValuePoolOptions=DocumentArrayPoolOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/bucketized_pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/document/bucketized_document_array_pool.gen.go -pkg=document gen \"ValueBucket=DocumentArrayBucket ValuePoolOptions=DocumentArrayPoolOptions ValuePool=DocumentArrayPool valueBucketByCapacity=documentArrayBucketByCapacity bucketPool=documentArrayBucketPool BucketizedValuePool=BucketizedDocumentArrayPool GenericValue=[]Document\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool_config.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/document/field/field_array_pool_config.gen.go -pkg=field gen \"ValuePoolWatermarkConfiguration=FieldArrayPoolWatermarkConfiguration ValuePoolConfiguration=FieldArrayPoolConfiguration ValuePoolOptions=FieldArrayPoolOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/document/field/field_array_pool.gen.go -pkg=field gen \"ValuePoolOptions=FieldArrayPoolOptions valuePoolMetrics=fieldArrayPoolMetrics ValuePool=FieldArrayPool GenericValue=[]Field\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/bucketized_pool_config.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/document/field/bucketized_field_array_pool_config.gen.go -pkg=field gen \"ValuePoolBucketConfiguration=FieldArrayBucketConfiguration ValueBucket=FieldArrayBucket ValuePoolWatermarkConfiguration=FieldArrayPoolWatermarkConfiguration BucketizedValuePoolConfiguration=BucketizedFieldArrayPoolConfiguration ValuePoolOptions=FieldArrayPoolOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/bucketized_pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/document/field/bucketized_field_array_pool.gen.go -pkg=field gen \"ValueBucket=FieldArrayBucket ValuePoolOptions=FieldArrayPoolOptions ValuePool=FieldArrayPool valueBucketByCapacity=fieldArrayBucketByCapacity bucketPool=fieldArrayBucketPool BucketizedValuePool=BucketizedFieldArrayPool GenericValue=[]Field\""

// Iterator related template instantiations.

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/index/template/at_position_value_field_iterator.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/index/field/at_position_bool_field_iterator.gen.go -pkg=field -imp \"github.com/xichen2020/eventdb/values/iterator\" gen \"GenericValue=bool ForwardValueIterator=BoolIterator:iterator.ForwardBoolIterator SeekableValueIterator=SeekableBoolIterator:iterator.SeekableBoolIterator atPositionValueFieldIterator=atPositionBoolFieldIterator newAtPositionValueFieldIterator=newAtPositionBoolFieldIterator errPositionIterValueIterCountMismatch=errPositionIterBoolIterCountMismatch valueAsUnionFn=boolAsUnionFn:field.BoolAsUnionFn\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/index/template/at_position_value_field_iterator.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/index/field/at_position_int_field_iterator.gen.go -pkg=field -imp \"github.com/xichen2020/eventdb/values/iterator\" gen \"GenericValue=int ForwardValueIterator=IntIterator:iterator.ForwardIntIterator SeekableValueIterator=SeekableIntIterator:iterator.SeekableIntIterator atPositionValueFieldIterator=atPositionIntFieldIterator newAtPositionValueFieldIterator=newAtPositionIntFieldIterator errPositionIterValueIterCountMismatch=errPositionIterIntIterCountMismatch valueAsUnionFn=intAsUnionFn:field.IntAsUnionFn\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/index/template/at_position_value_field_iterator.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/index/field/at_position_double_field_iterator.gen.go -pkg=field -imp \"github.com/xichen2020/eventdb/values/iterator\" gen \"GenericValue=float64 ForwardValueIterator=DoubleIterator:iterator.ForwardDoubleIterator SeekableValueIterator=SeekableDoubleIterator:iterator.SeekableDoubleIterator atPositionValueFieldIterator=atPositionDoubleFieldIterator newAtPositionValueFieldIterator=newAtPositionDoubleFieldIterator errPositionIterValueIterCountMismatch=errPositionIterDoubleIterCountMismatch valueAsUnionFn=doubleAsUnionFn:field.DoubleAsUnionFn\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/index/template/at_position_value_field_iterator.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/index/field/at_position_string_field_iterator.gen.go -pkg=field -imp \"github.com/xichen2020/eventdb/values/iterator\" gen \"GenericValue=string ForwardValueIterator=StringIterator:iterator.ForwardStringIterator SeekableValueIterator=SeekableStringIterator:iterator.SeekableStringIterator atPositionValueFieldIterator=atPositionStringFieldIterator newAtPositionValueFieldIterator=newAtPositionStringFieldIterator errPositionIterValueIterCountMismatch=errPositionIterStringIterCountMismatch valueAsUnionFn=stringAsUnionFn:field.StringAsUnionFn\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/index/template/at_position_value_field_iterator.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/index/field/at_position_time_field_iterator.gen.go -pkg=field -imp \"github.com/xichen2020/eventdb/values/iterator\" gen \"GenericValue=int64 ForwardValueIterator=TimeIterator:iterator.ForwardTimeIterator SeekableValueIterator=SeekableTimeIterator:iterator.SeekableTimeIterator atPositionValueFieldIterator=atPositionTimeFieldIterator newAtPositionValueFieldIterator=newAtPositionTimeFieldIterator errPositionIterValueIterCountMismatch=errPositionIterTimeIterCountMismatch valueAsUnionFn=timeAsUnionFn:field.TimeAsUnionFn\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/index/template/field_iterator.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/index/field/bool_field_iterator.gen.go -pkg=field -imp \"github.com/xichen2020/eventdb/values/iterator\" gen \"GenericValue=bool ForwardValueIterator=BoolIterator:iterator.ForwardBoolIterator valueFieldIterator=boolFieldIterator newValueFieldIterator=newBoolFieldIterator valueAsUnionFn=boolAsUnionFn:field.BoolAsUnionFn\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/index/template/field_iterator.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/index/field/int_field_iterator.gen.go -pkg=field -imp \"github.com/xichen2020/eventdb/values/iterator\" gen \"GenericValue=int ForwardValueIterator=IntIterator:iterator.ForwardIntIterator valueFieldIterator=intFieldIterator newValueFieldIterator=newIntFieldIterator valueAsUnionFn=intAsUnionFn:field.IntAsUnionFn\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/index/template/field_iterator.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/index/field/double_field_iterator.gen.go -pkg=field -imp \"github.com/xichen2020/eventdb/values/iterator\" gen \"GenericValue=float64 ForwardValueIterator=DoubleIterator:iterator.ForwardDoubleIterator valueFieldIterator=doubleFieldIterator newValueFieldIterator=newDoubleFieldIterator valueAsUnionFn=doubleAsUnionFn:field.DoubleAsUnionFn\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/index/template/field_iterator.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/index/field/string_field_iterator.gen.go -pkg=field -imp \"github.com/xichen2020/eventdb/values/iterator\" gen \"GenericValue=string ForwardValueIterator=StringIterator:iterator.ForwardStringIterator valueFieldIterator=stringFieldIterator newValueFieldIterator=newStringFieldIterator valueAsUnionFn=stringAsUnionFn:field.StringAsUnionFn\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/index/template/field_iterator.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/index/field/time_field_iterator.gen.go -pkg=field -imp \"github.com/xichen2020/eventdb/values/iterator\" gen \"GenericValue=int64 ForwardValueIterator=TimeIterator:iterator.ForwardTimeIterator valueFieldIterator=timeFieldIterator newValueFieldIterator=newTimeFieldIterator valueAsUnionFn=timeAsUnionFn:field.TimeAsUnionFn\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/values/template/filtered_value_iterator.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/values/iterator/impl/filtered_bool_iterator.gen.go -pkg=impl -imp \"github.com/xichen2020/eventdb/values/iterator\" -imp \"github.com/xichen2020/eventdb/filter\" gen \"GenericValue=bool ForwardValueIterator=BoolIterator:iterator.ForwardBoolIterator FilteredValueIterator=FilteredBoolIterator ValueFilter=BoolFilter:filter.BoolFilter\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/values/template/filtered_value_iterator.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/values/iterator/impl/filtered_int_iterator.gen.go -pkg=impl -imp \"github.com/xichen2020/eventdb/values/iterator\" -imp \"github.com/xichen2020/eventdb/filter\" gen \"GenericValue=int ForwardValueIterator=IntIterator:iterator.ForwardIntIterator FilteredValueIterator=FilteredIntIterator ValueFilter=IntFilter:filter.IntFilter\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/values/template/filtered_value_iterator.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/values/iterator/impl/filtered_double_iterator.gen.go -pkg=impl -imp \"github.com/xichen2020/eventdb/values/iterator\" -imp \"github.com/xichen2020/eventdb/filter\" gen \"GenericValue=float64 ForwardValueIterator=DoubleIterator:iterator.ForwardDoubleIterator FilteredValueIterator=FilteredDoubleIterator ValueFilter=DoubleFilter:filter.DoubleFilter\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/values/template/filtered_value_iterator.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/values/iterator/impl/filtered_string_iterator.gen.go -pkg=impl -imp \"github.com/xichen2020/eventdb/values/iterator\" -imp \"github.com/xichen2020/eventdb/filter\" gen \"GenericValue=string ForwardValueIterator=StringIterator:iterator.ForwardStringIterator FilteredValueIterator=FilteredStringIterator ValueFilter=StringFilter:filter.StringFilter\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/values/template/filtered_value_iterator.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/values/iterator/impl/filtered_time_iterator.gen.go -pkg=impl -imp \"github.com/xichen2020/eventdb/values/iterator\" -imp \"github.com/xichen2020/eventdb/filter\" gen \"GenericValue=int64 ForwardValueIterator=TimeIterator:iterator.ForwardTimeIterator FilteredValueIterator=FilteredTimeIterator ValueFilter=TimeFilter:filter.TimeFilter\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/values/template/default_filtered_value_iterator.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/values/impl/default_filtered_array_based_value_iterator.gen.go -pkg=impl gen \"BoolValueCollection=*ArrayBasedBoolValues defaultFilteredBoolValueIterator=defaultFilteredArrayBasedBoolValueIterator IntValueCollection=*ArrayBasedIntValues defaultFilteredIntValueIterator=defaultFilteredArrayBasedIntValueIterator DoubleValueCollection=*ArrayBasedDoubleValues defaultFilteredDoubleValueIterator=defaultFilteredArrayBasedDoubleValueIterator StringValueCollection=*ArrayBasedStringValues defaultFilteredStringValueIterator=defaultFilteredArrayBasedStringValueIterator TimeValueCollection=*ArrayBasedTimeValues defaultFilteredTimeValueIterator=defaultFilteredArrayBasedTimeValueIterator\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/values/template/default_filtered_value_iterator.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/values/decoding/default_filtered_fs_value_iterator.gen.go -pkg=decoding gen \"BoolValueCollection=*fsBasedBoolValues defaultFilteredBoolValueIterator=defaultFilteredFsBasedBoolValueIterator IntValueCollection=*fsBasedIntValues defaultFilteredIntValueIterator=defaultFilteredFsBasedIntValueIterator DoubleValueCollection=*fsBasedDoubleValues defaultFilteredDoubleValueIterator=defaultFilteredFsBasedDoubleValueIterator StringValueCollection=*fsBasedStringValues defaultFilteredStringValueIterator=defaultFilteredFsBasedStringValueIterator TimeValueCollection=*fsBasedTimeValues defaultFilteredTimeValueIterator=defaultFilteredFsBasedTimeValueIterator\""

// Encoding / Decoding related template instantiations.

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/proto/template/encode.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/proto/encode_bool_meta.gen.go -pkg=proto gen \"GenericEncodeProtoMessage=*encodingpb.BoolMeta EncodeValue=EncodeBoolMeta\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/proto/template/decode.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/proto/decode_bool_meta.gen.go -pkg=proto gen \"GenericDecodeProtoMessage=*encodingpb.BoolMeta DecodeValue=DecodeBoolMeta DecodeValueRaw=DecodeBoolMetaRaw\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/proto/template/encode.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/proto/encode_string_meta.gen.go -pkg=proto gen \"GenericEncodeProtoMessage=*encodingpb.StringMeta EncodeValue=EncodeStringMeta\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/proto/template/decode.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/proto/decode_string_meta.gen.go -pkg=proto gen \"GenericDecodeProtoMessage=*encodingpb.StringMeta DecodeValue=DecodeStringMeta DecodeValueRaw=DecodeStringMetaRaw\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/proto/template/encode.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/proto/encode_string_array.gen.go -pkg=proto gen \"GenericEncodeProtoMessage=*encodingpb.StringArray EncodeValue=EncodeStringArray\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/proto/template/decode.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/proto/decode_string_array.gen.go -pkg=proto gen \"GenericDecodeProtoMessage=*encodingpb.StringArray DecodeValue=DecodeStringArray DecodeValueRaw=DecodeStringArrayRaw\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/proto/template/encode.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/proto/encode_int_meta.gen.go -pkg=proto gen \"GenericEncodeProtoMessage=*encodingpb.IntMeta EncodeValue=EncodeIntMeta\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/proto/template/decode.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/proto/decode_int_meta.gen.go -pkg=proto gen \"GenericDecodeProtoMessage=*encodingpb.IntMeta DecodeValue=DecodeIntMeta DecodeValueRaw=DecodeIntMetaRaw\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/proto/template/encode.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/proto/encode_int_dictionary.gen.go -pkg=proto gen \"GenericEncodeProtoMessage=*encodingpb.IntDictionary EncodeValue=EncodeIntDictionary\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/proto/template/decode.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/proto/decode_int_dictionary.gen.go -pkg=proto gen \"GenericDecodeProtoMessage=*encodingpb.IntDictionary DecodeValue=DecodeIntDictionary DecodeValueRaw=DecodeIntDictionaryRaw\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/proto/template/encode.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/proto/encode_double_meta.gen.go -pkg=proto gen \"GenericEncodeProtoMessage=*encodingpb.DoubleMeta EncodeValue=EncodeDoubleMeta\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/proto/template/decode.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/proto/decode_double_meta.gen.go -pkg=proto gen \"GenericDecodeProtoMessage=*encodingpb.DoubleMeta DecodeValue=DecodeDoubleMeta DecodeValueRaw=DecodeDoubleMetaRaw\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/proto/template/encode.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/proto/encode_time_meta.gen.go -pkg=proto gen \"GenericEncodeProtoMessage=*encodingpb.TimeMeta EncodeValue=EncodeTimeMeta\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/proto/template/decode.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/proto/decode_time_meta.gen.go -pkg=proto gen \"GenericDecodeProtoMessage=*encodingpb.TimeMeta DecodeValue=DecodeTimeMeta DecodeValueRaw=DecodeTimeMetaRaw\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/values/template/run_length_encode.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/values/encoding/run_length_encode_bool.gen.go -pkg=encoding -imp \"github.com/xichen2020/eventdb/values/iterator\" gen \"GenericValue=bool ForwardValueIterator=ForwardBoolIterator:iterator.ForwardBoolIterator runLengthEncodeValue=runLengthEncodeBool\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/values/template/run_length_decode.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/values/decoding/run_length_decode_bool.gen.go -pkg=decoding gen \"GenericValue=bool runLengthDecodeValue=runLengthDecodeBool runLengthValueIterator=runLengthBoolIterator newRunLengthValueIterator=newRunLengthBoolIterator\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/values/template/delta_encode.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/values/encoding/delta_int_encode.gen.go -pkg=encoding -imp \"github.com/xichen2020/eventdb/values/iterator\" gen \"GenericValue=int ForwardValueIterator=ForwardIntIterator:iterator.ForwardIntIterator deltaValueEncode=deltaIntEncode\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/values/template/delta_iterator.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/values/decoding/delta_int_iterator.gen.go -pkg=decoding gen \"GenericValue=int deltaValueIterator=deltaIntIterator newDeltaValueIterator=newDeltaIntIterator applyOpToValueIntFn=applyOpToIntIntFn\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/values/template/delta_encode.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/values/encoding/delta_time_encode.gen.go -pkg=encoding -imp \"github.com/xichen2020/eventdb/values/iterator\" gen \"GenericValue=int64 ForwardValueIterator=ForwardTimeIterator:iterator.ForwardTimeIterator deltaValueEncode=deltaTimeEncode\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/values/template/delta_iterator.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/values/decoding/delta_time_iterator.gen.go -pkg=decoding gen \"GenericValue=int64 deltaValueIterator=deltaTimeIterator newDeltaValueIterator=newDeltaTimeIterator applyOpToValueIntFn=applyOpToTimeIntFn\""

// Hash map related template instantiations from external source (m3db/m3x).

// Use perl to rename symbols to work around genny's limitation of not renaming symbols properly (and sed doesn't work on BSD / Mac OS).
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/vendor/github.com/m3db/m3x/generics/hashmap/map.go | awk '/^package/{i++}i' | genny -pkg=query -imp \"github.com/xichen2020/eventdb/document/field\" -imp \"github.com/xichen2020/eventdb/calculation\" gen \"KeyType=Values:field.Values ValueType=ResultArray:calculation.ResultArray Map=ValuesResultArrayHash mapKey=valuesResultArrayHashKey mapOptions=valuesResultArrayHashOptions mapAlloc=valuesResultArrayHashAlloc\" | perl -p -e 's/ValuesResultArrayHashHash/ValuesResultArrayHashHash/gi' | perl -p -e 's/ValuesResultArrayHashEntry/ValuesResultArrayHashEntry/gi' > $GOPATH/src/$PACKAGE/query/values_result_array_map.gen.go"

// Heap related template instantiations.
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/heap/generic.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/query/raw_result_heap.gen.go -pkg=query gen \"GenericValue=RawResult ValueHeap=RawResultHeap NewHeap=NewRawResultHeap TopNValues=TopNRawResults NewTopValues=NewTopNRawResults ValueAddOptions=RawResultAddOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/heap/generic.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/query/multi_key_result_group_heap.gen.go -pkg=query gen \"GenericValue=multiKeyResultGroup ValueHeap=multiKeyResultGroupHeap NewHeap=newMultiKeyResultGroupHeap TopNValues=topNMultiKeyResultGroup NewTopValues=newTopNMultiKeyResultGroup ValueAddOptions=multiKeyResultGroupAddOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/heap/generic.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/query/bool_result_group_heap.gen.go -pkg=query gen \"GenericValue=boolResultGroup ValueHeap=boolResultGroupHeap NewHeap=newBoolResultGroupHeap TopNValues=topNBools NewTopValues=newTopNBools ValueAddOptions=boolAddOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/heap/generic.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/query/int_result_group_heap.gen.go -pkg=query gen \"GenericValue=intResultGroup ValueHeap=intResultGroupHeap NewHeap=newIntResultGroupHeap TopNValues=topNInts NewTopValues=newTopNInts ValueAddOptions=intAddOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/heap/generic.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/query/double_result_group_heap.gen.go -pkg=query gen \"GenericValue=doubleResultGroup ValueHeap=doubleResultGroupHeap NewHeap=newDoubleResultGroupHeap TopNValues=topNDoubles NewTopValues=newTopNDoubles ValueAddOptions=doubleAddOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/heap/generic.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/query/string_result_group_heap.gen.go -pkg=query gen \"GenericValue=stringResultGroup ValueHeap=stringResultGroupHeap NewHeap=newStringResultGroupHeap TopNValues=topNStrings NewTopValues=newTopNStrings ValueAddOptions=stringAddOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/heap/generic.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/query/time_result_group_heap.gen.go -pkg=query gen \"GenericValue=timeResultGroup ValueHeap=timeResultGroupHeap NewHeap=newTimeResultGroupHeap TopNValues=topNTimes NewTopValues=newTopNTimes ValueAddOptions=timeAddOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/heap/generic.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/storage/doc_id_values_heap.gen.go -pkg=storage gen \"GenericValue=docIDValues ValueHeap=docIDValuesHeap NewHeap=newDocIDValuesHeap TopNValues=topNDocIDValues NewTopValues=newTopNDocIDValues ValueAddOptions=docIDValuesAddOptions\""

package generics
