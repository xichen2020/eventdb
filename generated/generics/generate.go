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
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/append.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/append_bool.gen.go -pkg=pool gen \"GenericBucketizedValueArrayPool=*BucketizedBoolArrayPool AppendValue=AppendBool GenericValue=bool\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool_config.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/int_array_pool_config.gen.go -pkg=pool gen \"ValuePoolWatermarkConfiguration=IntArrayPoolWatermarkConfiguration ValuePoolConfiguration=IntArrayPoolConfiguration ValuePoolOptions=IntArrayPoolOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/int_array_pool.gen.go -pkg=pool gen \"ValuePoolOptions=IntArrayPoolOptions valuePoolMetrics=intArrayPoolMetrics ValuePool=IntArrayPool GenericValue=[]int\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/bucketized_pool_config.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/bucketized_int_array_pool_config.gen.go -pkg=pool gen \"ValuePoolBucketConfiguration=IntArrayBucketConfiguration ValueBucket=IntArrayBucket ValuePoolWatermarkConfiguration=IntArrayPoolWatermarkConfiguration BucketizedValuePoolConfiguration=BucketizedIntArrayPoolConfiguration ValuePoolOptions=IntArrayPoolOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/bucketized_pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/bucketized_int_array_pool.gen.go -pkg=pool gen \"ValueBucket=IntArrayBucket ValuePoolOptions=IntArrayPoolOptions ValuePool=IntArrayPool valueBucketByCapacity=intArrayBucketByCapacity bucketPool=intArrayBucketPool BucketizedValuePool=BucketizedIntArrayPool GenericValue=[]int\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/append.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/append_int.gen.go -pkg=pool gen \"GenericBucketizedValueArrayPool=*BucketizedIntArrayPool AppendValue=AppendInt GenericValue=int\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool_config.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/int64_array_pool_config.gen.go -pkg=pool gen \"ValuePoolWatermarkConfiguration=Int64ArrayPoolWatermarkConfiguration ValuePoolConfiguration=Int64ArrayPoolConfiguration ValuePoolOptions=Int64ArrayPoolOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/int64_array_pool.gen.go -pkg=pool gen \"ValuePoolOptions=Int64ArrayPoolOptions valuePoolMetrics=int64ArrayPoolMetrics ValuePool=Int64ArrayPool GenericValue=[]int64\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/bucketized_pool_config.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/bucketized_int64_array_pool_config.gen.go -pkg=pool gen \"ValuePoolBucketConfiguration=Int64ArrayBucketConfiguration ValueBucket=Int64ArrayBucket ValuePoolWatermarkConfiguration=Int64ArrayPoolWatermarkConfiguration BucketizedValuePoolConfiguration=BucketizedInt64ArrayPoolConfiguration ValuePoolOptions=Int64ArrayPoolOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/bucketized_pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/bucketized_int64_array_pool.gen.go -pkg=pool gen \"ValueBucket=Int64ArrayBucket ValuePoolOptions=Int64ArrayPoolOptions ValuePool=Int64ArrayPool valueBucketByCapacity=int64ArrayBucketByCapacity bucketPool=int64ArrayBucketPool BucketizedValuePool=BucketizedInt64ArrayPool GenericValue=[]int64\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/append.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/append_int64.gen.go -pkg=pool gen \"GenericBucketizedValueArrayPool=*BucketizedInt64ArrayPool AppendValue=AppendInt64 GenericValue=int64\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool_config.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/float64_array_pool_config.gen.go -pkg=pool gen \"ValuePoolWatermarkConfiguration=Float64ArrayPoolWatermarkConfiguration ValuePoolConfiguration=Float64ArrayPoolConfiguration ValuePoolOptions=Float64ArrayPoolOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/float64_array_pool.gen.go -pkg=pool gen \"ValuePoolOptions=Float64ArrayPoolOptions valuePoolMetrics=float64ArrayPoolMetrics ValuePool=Float64ArrayPool GenericValue=[]float64\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/bucketized_pool_config.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/bucketized_float64_array_pool_config.gen.go -pkg=pool gen \"ValuePoolBucketConfiguration=Float64ArrayBucketConfiguration ValueBucket=Float64ArrayBucket ValuePoolWatermarkConfiguration=Float64ArrayPoolWatermarkConfiguration BucketizedValuePoolConfiguration=BucketizedFloat64ArrayPoolConfiguration ValuePoolOptions=Float64ArrayPoolOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/bucketized_pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/bucketized_float64_array_pool.gen.go -pkg=pool gen \"ValueBucket=Float64ArrayBucket ValuePoolOptions=Float64ArrayPoolOptions ValuePool=Float64ArrayPool valueBucketByCapacity=float64ArrayBucketByCapacity bucketPool=float64ArrayBucketPool BucketizedValuePool=BucketizedFloat64ArrayPool GenericValue=[]float64\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/append.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/append_float64.gen.go -pkg=pool gen \"GenericBucketizedValueArrayPool=*BucketizedFloat64ArrayPool AppendValue=AppendFloat64 GenericValue=float64\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool_config.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/string_array_pool_config.gen.go -pkg=pool gen \"ValuePoolWatermarkConfiguration=StringArrayPoolWatermarkConfiguration ValuePoolConfiguration=StringArrayPoolConfiguration ValuePoolOptions=StringArrayPoolOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/string_array_pool.gen.go -pkg=pool gen \"ValuePoolOptions=StringArrayPoolOptions valuePoolMetrics=stringArrayPoolMetrics ValuePool=StringArrayPool GenericValue=[]string\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/bucketized_pool_config.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/bucketized_string_array_pool_config.gen.go -pkg=pool gen \"ValuePoolBucketConfiguration=StringArrayBucketConfiguration ValueBucket=StringArrayBucket ValuePoolWatermarkConfiguration=StringArrayPoolWatermarkConfiguration BucketizedValuePoolConfiguration=BucketizedStringArrayPoolConfiguration ValuePoolOptions=StringArrayPoolOptions\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/bucketized_pool.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/bucketized_string_array_pool.gen.go -pkg=pool gen \"ValueBucket=StringArrayBucket ValuePoolOptions=StringArrayPoolOptions ValuePool=StringArrayPool valueBucketByCapacity=stringArrayBucketByCapacity bucketPool=stringArrayBucketPool BucketizedValuePool=BucketizedStringArrayPool GenericValue=[]string\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/pool/template/append.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/pool/append_string.gen.go -pkg=pool gen \"GenericBucketizedValueArrayPool=*BucketizedStringArrayPool AppendValue=AppendString GenericValue=string\""

//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/proto/template/encode.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/proto/encode_string_meta.gen.go -pkg=proto gen \"GenericProtoMessage=*encodingpb.StringMeta EncodeValue=EncodeStringMeta\""
//go:generate sh -c "cat $GOPATH/src/$PACKAGE/x/proto/template/encode.go | awk '/^package/{i++}i' | genny -out=$GOPATH/src/$PACKAGE/x/proto/encode_string_array.gen.go -pkg=proto gen \"GenericProtoMessage=*encodingpb.StringArray EncodeValue=EncodeStringArray\""

package generics
